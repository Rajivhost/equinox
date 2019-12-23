namespace Equinox.DynamoDb.Store

open Amazon.DynamoDBv2.DataModel
open Equinox.Core
open FsCodec
open Newtonsoft.Json
open Serilog
open System
open System.IO

/// A single Domain Event from the array held in a Batch
type [<NoEquality; NoComparison; JsonObject(ItemRequired=Required.Always)>]
    Event =
    {   /// Creation datetime (as opposed to system-defined _lastUpdated which is touched by triggers, replication etc.)
        t: DateTimeOffset // ISO 8601

        /// The Case (Event Type); used to drive deserialization
        c: string // required

        /// Event body, as UTF-8 encoded json ready to be injected into the Json being rendered for CosmosDB
        [<JsonConverter(typeof<FsCodec.NewtonsoftJson.VerbatimUtf8JsonConverter>)>]
        [<JsonProperty(Required=Required.AllowNull)>]
        d: byte[] // Required, but can be null so Nullary cases can work

        /// Optional metadata, as UTF-8 encoded json, ready to emit directly (null, not written if missing)
        [<JsonConverter(typeof<FsCodec.NewtonsoftJson.VerbatimUtf8JsonConverter>)>]
        [<JsonProperty(Required=Required.Default, NullValueHandling=NullValueHandling.Ignore)>]
        m: byte[]

        /// Optional correlationId (can be null, not written if missing)
        [<JsonProperty(Required=Required.Default, NullValueHandling=NullValueHandling.Ignore)>]
        correlationId : string

        /// Optional causationId (can be null, not written if missing)
        [<JsonProperty(Required=Required.Default, NullValueHandling=NullValueHandling.Ignore)>]
        causationId : string }

    interface IEventData<byte[]> with
        member __.EventType = __.c
        member __.Data = __.d
        member __.Meta = __.m
        member __.Timestamp = __.t
        member __.CorrelationId = __.correlationId
        member __.CausationId = __.causationId

/// A 'normal' (frozen, not Tip) Batch of Events (without any Unfolds)
type [<NoEquality; NoComparison; JsonObject(ItemRequired=Required.Always)>]
    Batch =
    {   /// CosmosDB-mandated Partition Key, must be maintained within the document
        /// Not actually required if running in single partition mode, but for simplicity, we always write it
        [<JsonProperty(Required=Required.Default)>] // Not requested in queries
        p: string // "{streamName}"

        /// CosmosDB-mandated unique row key; needs to be unique within any partition it is maintained; must be string
        /// At the present time, one can't perform an ORDER BY on this field, hence we also have i shadowing it
        /// NB Tip uses a well known value here while it's actively 'open'
        id: string // "{index}"

        /// When we read, we need to capture the value so we can retain it for caching purposes
        /// NB this is not relevant to fill in when we pass it to the writing stored procedure
        /// as it will do: 1. read 2. merge 3. write merged version contingent on the _etag not having changed
        [<JsonProperty(DefaultValueHandling=DefaultValueHandling.Ignore, Required=Required.Default)>]
        _etag: string

        /// base 'i' value for the Events held herein
        i: int64 // {index}

        // `i` value for successor batch (to facilitate identifying which Batch a given startPos is within)
        n: int64 // {index}

        /// The Domain Events (as opposed to Unfolded Events, see Tip) at this offset in the stream
        e: Event[] }
    /// Unless running in single partition mode (which would restrict us to 10GB per collection)
    /// we need to nominate a partition key that will be in every document
    static member internal PartitionKeyField = "p"
    /// As one cannot sort by the implicit `id` field, we have an indexed `i` field for sort and range query use
    static member internal IndexedFields = [Batch.PartitionKeyField; "i"; "n"]

/// Compaction/Snapshot/Projection Event based on the state at a given point in time `i`
type Unfold =
    {   /// Base: Stream Position (Version) of State from which this Unfold Event was generated
        i: int64

        /// The Case (Event Type) of this compaction/snapshot, used to drive deserialization
        c: string // required

        /// Event body - Json -> UTF-8 -> Deflate -> Base64
        [<JsonConverter(typeof<Base64DeflateUtf8JsonConverter>)>]
        d: byte[] // required

        /// Optional metadata, same encoding as `d` (can be null; not written if missing)
        [<JsonConverter(typeof<Base64DeflateUtf8JsonConverter>)>]
        [<JsonProperty(Required=Required.Default, NullValueHandling=NullValueHandling.Ignore)>]
        m: byte[] } // optional

/// Manages zipping of the UTF-8 json bytes to make the index record minimal from the perspective of the writer stored proc
/// Only applied to snapshots in the Tip
and Base64DeflateUtf8JsonConverter() =
    inherit JsonConverter()
    let pickle (input : byte[]) : string =
        if input = null then null else

        use output = new MemoryStream()
        use compressor = new System.IO.Compression.DeflateStream(output, System.IO.Compression.CompressionLevel.Optimal)
        compressor.Write(input,0,input.Length)
        compressor.Close()
        System.Convert.ToBase64String(output.ToArray())
    let unpickle str : byte[] =
        if str = null then null else

        let compressedBytes = System.Convert.FromBase64String str
        use input = new MemoryStream(compressedBytes)
        use decompressor = new System.IO.Compression.DeflateStream(input, System.IO.Compression.CompressionMode.Decompress)
        use output = new MemoryStream()
        decompressor.CopyTo(output)
        output.ToArray()

    override __.CanConvert(objectType) =
        typeof<byte[]>.Equals(objectType)
    override __.ReadJson(reader, _, _, serializer) =
        //(   if reader.TokenType = JsonToken.Null then null else
        serializer.Deserialize(reader, typedefof<string>) :?> string |> unpickle |> box
    override __.WriteJson(writer, value, serializer) =
        let pickled = value |> unbox |> pickle
        serializer.Serialize(writer, pickled)

/// The special-case 'Pending' Batch Format used to read the currently active (and mutable) document
/// Stored representation has the following diffs vs a 'normal' (frozen/completed) Batch: a) `id` = `-1` b) contains unfolds (`u`)
/// NB the type does double duty as a) model for when we read it b) encoding a batch being sent to the stored proc
type [<NoEquality; NoComparison; JsonObject(ItemRequired=Required.Always)>]
    Tip =
    {   [<JsonProperty(Required=Required.Default)>] // Not requested in queries
        /// Partition key, as per Batch
        p: string // "{streamName}"
        /// Document Id within partition, as per Batch
        id: string // "{-1}" - Well known IdConstant used while this remains the pending batch

        /// When we read, we need to capture the value so we can retain it for caching purposes
        /// NB this is not relevant to fill in when we pass it to the writing stored procedure
        /// as it will do: 1. read 2. merge 3. write merged version contingent on the _etag not having changed
        [<JsonProperty(DefaultValueHandling=DefaultValueHandling.Ignore, Required=Required.Default)>]
        _etag: string

        /// base 'i' value for the Events held herein
        i: int64

        /// `i` value for successor batch (to facilitate identifying which Batch a given startPos is within)
        n: int64 // {index}

        /// Domain Events, will eventually move out to a Batch
        e: Event[]

        /// Compaction/Snapshot/Projection events - owned and managed by the sync stored proc
        u: Unfold[] }
    static member internal WellKnownDocumentId = "-1"

/// Position and Etag to which an operation is relative
type [<NoComparison>]
    Position = { index: int64; etag: string option }

module internal Position =
    /// NB very inefficient compared to FromDocument or using one already returned to you
    let fromI (i: int64) = { index = i; etag = None }
    /// If we have strong reason to suspect a stream is empty, we won't have an etag (and Writer Stored Procedure special cases this)
    let fromKnownEmpty = fromI 0L
    /// Just Do It mode
    let fromAppendAtEnd = fromI -1L // sic - needs to yield -1
    let fromEtag (value : string) = { fromI -2L with etag = Some value }
    /// NB very inefficient compared to FromDocument or using one already returned to you
    let fromMaxIndex (xs: ITimelineEvent<byte[]>[]) =
        if Array.isEmpty xs then fromKnownEmpty
        else fromI (1L + Seq.max (seq { for x in xs -> x.Index }))
    /// Create Position from Tip record context (facilitating 1 RU reads)
    let fromTip (x: Tip) = { index = x.n; etag = match x._etag with null -> None | x -> Some x }
    /// If we encounter the tip (id=-1) item/document, we're interested in its etag so we can re-sync for 1 RU
    let tryFromBatch (x: Batch) =
        if x.id <> Tip.WellKnownDocumentId then None
        else Some { index = x.n; etag = match x._etag with null -> None | x -> Some x }

[<RequireQualifiedAccess>]
type Direction = Forward | Backward override this.ToString() = match this with Forward -> "Forward" | Backward -> "Backward"

type internal Enum() =
    static member internal Events(b: Tip) : ITimelineEvent<byte[]> seq =
        b.e |> Seq.mapi (fun offset x -> FsCodec.Core.TimelineEvent.Create(b.i + int64 offset, x.c, x.d, x.m, x.correlationId, x.causationId, x.t) :> _)
    static member Events(i: int64, e: Event[], startPos : Position option, direction) : ITimelineEvent<byte[]> seq = seq {
        // If we're loading from a nominated position, we need to discard items in the batch before/after the start on the start page
        let isValidGivenStartPos i =
            match startPos with
            | Some sp when direction = Direction.Backward -> i < sp.index
            | Some sp -> i >= sp.index
            | _ -> true
        for offset in 0..e.Length-1 do
            let index = i + int64 offset
            if isValidGivenStartPos index then
                let x = e.[offset]
                yield FsCodec.Core.TimelineEvent.Create(index, x.c, x.d, x.m, x.correlationId, x.causationId, x.t) :> _ }
    static member internal Events(b: Batch, startPos, direction) =
        Enum.Events(b.i, b.e, startPos, direction)
        |> if direction = Direction.Backward then System.Linq.Enumerable.Reverse else id
    static member Unfolds(xs: Unfold[]) : ITimelineEvent<byte[]> seq = seq {
        for x in xs -> FsCodec.Core.TimelineEvent.Create(x.i, x.c, x.d, x.m, null, null, DateTimeOffset.MinValue, isUnfold=true) }
    static member EventsAndUnfolds(x: Tip): ITimelineEvent<byte[]> seq =
        Enum.Events x
        |> Seq.append (Enum.Unfolds x.u)
        // where Index is equal, unfolds get delivered after the events so the fold semantics can be 'idempotent'
        |> Seq.sortBy (fun x -> x.Index, x.IsUnfold)

type IRetryPolicy = abstract member Execute: (int -> Async<'T>) -> Async<'T>