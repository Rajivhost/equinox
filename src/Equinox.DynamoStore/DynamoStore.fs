namespace Equinox.DynamoStore

open System
open Amazon
open Serilog
open Equinox.Core
open Amazon.DynamoDBv2
open FSharp.AWS.DynamoDB

type EventData =
    {
        EventId: Guid
        Type: string
        IsJson: bool
        Data: byte array
        Metadata: byte array
    }

type EventStream =
    {
        [<HashKey>]
        StreamId : string
        [<RangeKey>]
        Version: int64
        Events: EventData[]
        Timestamp : int
    }

[<RequireQualifiedAccess>]
type Direction = Forward | Backward with
    override this.ToString() = match this with Forward -> "Forward" | Backward -> "Backward"

module Log =
    [<NoEquality; NoComparison>]
    type Measurement = { stream: string; interval: StopwatchInterval; bytes: int; count: int }

    [<NoEquality; NoComparison>]
    type Event =
        | WriteSuccess of Measurement
        | WriteConflict of Measurement
        | Slice of Direction * Measurement
        | Batch of Direction * slices: int * Measurement

    let prop name value (log : ILogger) = log.ForContext(name, value)

    let propEvents name (kvps : System.Collections.Generic.KeyValuePair<string,string> seq) (log : ILogger) =
        let items = seq { for kv in kvps do yield sprintf "{\"%s\": %s}" kv.Key kv.Value }
        log.ForContext(name, sprintf "[%s]" (String.concat ",\n\r" items))

    let propEventData name (events : EventData[]) (log : ILogger) =
        log |> propEvents name (seq {
            for x in events do
                if x.IsJson then
                    yield System.Collections.Generic.KeyValuePair<_,_>(x.Type, System.Text.Encoding.UTF8.GetString x.Data) })

    //let propResolvedEvents name (events : ResolvedEvent[]) (log : ILogger) =
    //    log |> propEvents name (seq {
    //        for x in events do
    //            let e = x.Event
    //            if e.IsJson then
    //                yield System.Collections.Generic.KeyValuePair<_,_>(e.EventType, System.Text.Encoding.UTF8.GetString e.Data) })

    open Serilog.Events

    /// Attach a property to the log context to hold the metrics
    // Sidestep Log.ForContext converting to a string; see https://github.com/serilog/serilog/issues/1124
    let event (value : Event) (log : ILogger) =
        let enrich (e : LogEvent) = e.AddPropertyIfAbsent(LogEventProperty("esEvt", ScalarValue(value)))
        log.ForContext({ new Serilog.Core.ILogEventEnricher with member __.Enrich(evt,_) = enrich evt })

    let withLoggedRetries<'t> retryPolicy (contextLabel : string) (f : ILogger -> Async<'t>) log : Async<'t> =
        match retryPolicy with
        | None -> f log
        | Some retryPolicy ->
            let withLoggingContextWrapping count =
                let log = if count = 1 then log else log |> prop contextLabel count
                f log
            retryPolicy withLoggingContextWrapping

    let (|BlobLen|) = function null -> 0 | (x : byte[]) -> x.Length

    /// NB Caveat emptor; this is subject to unlimited change without the major version changing - while the `dotnet-templates` repo will be kept in step, and
    /// the ChangeLog will mention changes, it's critical to not assume that the presence or nature of these helpers be considered stable
    module InternalMetrics =

        module Stats =
            let inline (|Stats|) ({ interval = i } : Measurement) = let e = i.Elapsed in int64 e.TotalMilliseconds

            let (|Read|Write|Resync|Rollup|) = function
                | Slice (_, (Stats s)) -> Read s
                | WriteSuccess (Stats s) -> Write s
                | WriteConflict (Stats s) -> Resync s
                // slices are rolled up into batches so be sure not to double-count
                | Batch (_, _, (Stats s)) -> Rollup s

            let (|SerilogScalar|_|) : LogEventPropertyValue -> obj option = function
                | (:? ScalarValue as x) -> Some x.Value
                | _ -> None

            let (|EsMetric|_|) (logEvent : LogEvent) : Event option =
                match logEvent.Properties.TryGetValue("esEvt") with
                | true, SerilogScalar (:? Event as e) -> Some e
                | _ -> None

            type Counter =
                { mutable count : int64; mutable ms : int64 }
                static member Create() = { count = 0L; ms = 0L }
                member __.Ingest(ms) =
                    System.Threading.Interlocked.Increment(&__.count) |> ignore
                    System.Threading.Interlocked.Add(&__.ms, ms) |> ignore

            type LogSink() =
                static let epoch = System.Diagnostics.Stopwatch.StartNew()
                static member val Read = Counter.Create() with get, set
                static member val Write = Counter.Create() with get, set
                static member val Resync = Counter.Create() with get, set
                static member Restart() =
                    LogSink.Read <- Counter.Create()
                    LogSink.Write <- Counter.Create()
                    LogSink.Resync <- Counter.Create()
                    let span = epoch.Elapsed
                    epoch.Restart()
                    span
                interface Serilog.Core.ILogEventSink with
                    member __.Emit logEvent = logEvent |> function
                        | EsMetric (Read stats) -> LogSink.Read.Ingest stats
                        | EsMetric (Write stats) -> LogSink.Write.Ingest stats
                        | EsMetric (Resync stats) -> LogSink.Resync.Ingest stats
                        | EsMetric (Rollup _) -> ()
                        | _ -> ()

        /// Relies on feeding of metrics from Log through to Stats.LogSink
        /// Use Stats.LogSink.Restart() to reset the start point (and stats) where relevant
        let dump (log : Serilog.ILogger) =
            let stats =
              [ "Read", Stats.LogSink.Read
                "Write", Stats.LogSink.Write
                "Resync", Stats.LogSink.Resync ]
            let logActivity name count lat =
                log.Information("{name}: {count:n0} requests; Average latency: {lat:n0}ms",
                    name, count, (if count = 0L then Double.NaN else float lat/float count))
            let mutable rows, totalCount, totalMs = 0, 0L, 0L
            for name, stat in stats do
                if stat.count <> 0L then
                    totalCount <- totalCount + stat.count
                    totalMs <- totalMs + stat.ms
                    logActivity name stat.count stat.ms
                    rows <- rows + 1
            // Yes, there's a minor race here between the use of the values and the reset
            let duration = Stats.LogSink.Restart()
            if rows > 1 then logActivity "TOTAL" totalCount totalMs
            let measures : (string * (TimeSpan -> float)) list = [ "s", fun x -> x.TotalSeconds(*; "m", fun x -> x.TotalMinutes; "h", fun x -> x.TotalHours*) ]
            let logPeriodicRate name count = log.Information("rp{name} {count:n0}", name, count)
            for uom, f in measures do let d = f duration in if d <> 0. then logPeriodicRate uom (float totalCount/d |> int64)

[<RequireQualifiedAccess; NoEquality; NoComparison>]
type EsSyncResult = Written of TableKey | Conflict of actualVersion: int64

module private Write =
    /// Yields `EsSyncResult.Written` or `EsSyncResult.Conflict` to signify WrongExpectedVersion
    let private writeEventsAsync (log : ILogger) (conn : TableContext<EventStream>) (streamName : string) (version : int64) (events : EventData[])
        : Async<EsSyncResult> = async {
        try
            match! TableKey.Range(streamName, version) |> conn.ContainsKeyAsync with
            | true ->
                log.Information("Es TrySync WrongExpectedVersionException writing {EventTypes}, actual {ActualVersion}", [| for x in events -> x.Type |], version)
                return EsSyncResult.Conflict -1L
            | false ->
                let stream = {
                        EventStream.StreamId = streamName
                        Version = version
                        Events = events
                        Timestamp = (DateTime.Now.Subtract(new DateTime(1970,1,1)).TotalSeconds |> int)
                    }

                let! key = conn.PutItemAsync(stream)
                return EsSyncResult.Written key
        with ex ->
            log.Error(ex, ex.Message)
            return EsSyncResult.Conflict -1L }

    let eventDataBytes events =
        let eventDataLen (x : EventData) = match x.Data, x.Metadata with Log.BlobLen bytes, Log.BlobLen metaBytes -> bytes + metaBytes
        events |> Array.sumBy eventDataLen

    let private writeEventsLogged (conn : TableContext<EventStream>) (streamName : string) (version : int64) (events : EventData[]) (log : ILogger)
        : Async<EsSyncResult> = async {
        let log = if (not << log.IsEnabled) Events.LogEventLevel.Debug then log else log |> Log.propEventData "Json" events
        let bytes, count = eventDataBytes events, events.Length
        let log = log |> Log.prop "bytes" bytes
        let writeLog = log |> Log.prop "stream" streamName |> Log.prop "expectedVersion" version |> Log.prop "count" count
        let! t, result = writeEventsAsync writeLog conn streamName version events |> Stopwatch.Time
        let reqMetric : Log.Measurement = { stream = streamName; interval = t; bytes = bytes; count = count}
        let resultLog, evt =
            match result, reqMetric with
            | EsSyncResult.Conflict actualVersion, m ->
                log |> Log.prop "actualVersion" actualVersion, Log.WriteConflict m
            | EsSyncResult.Written x, m ->
                log |> Log.prop "tableKey" x, Log.WriteSuccess m
        (resultLog |> Log.event evt).Information("Es{action:l} count={count} conflict={conflict}",
            "Write", events.Length, match evt with Log.WriteConflict _ -> true | _ -> false)
        return result }

    let writeEvents (log : ILogger) retryPolicy (conn : TableContext<EventStream>) (streamName : string) (version : int64) (events : EventData[])
        : Async<EsSyncResult> =
        let call = writeEventsLogged conn streamName version events
        Log.withLoggedRetries retryPolicy "writeAttempt" call log
