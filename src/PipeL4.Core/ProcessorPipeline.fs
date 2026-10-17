namespace PipeL4.Core

open System
open System.Buffers
open System.Runtime.ExceptionServices
open System.Threading
open System.Threading.Tasks

type internal IProcessorPipelineSink =
    /// The sequence remains valid until the returned value task completes.
    abstract WriteAsync: bytes: ReadOnlySequence<byte> * cancellationToken: CancellationToken -> ValueTask
    /// Called after the last stage has completed and all output has been written.
    abstract CompleteAsync: cancellationToken: CancellationToken -> ValueTask

type private SegmentOwner(owner: IMemoryOwner<byte>) =
    let mutable references = 1

    member _.Memory = owner.Memory

    member _.Retain() =
        references <- references + 1

    member _.Release() =
        references <- references - 1

        if references = 0 then
            owner.Dispose()

[<Struct; NoEquality; NoComparison>]
type private OwnedRange =
    { Owner: SegmentOwner
      Offset: int
      Length: int }

type private SequenceNode(memory: ReadOnlyMemory<byte>) =
    inherit ReadOnlySequenceSegment<byte>()

    do base.Memory <- memory

    member this.Append(next: SequenceNode) =
        next.RunningIndex <- this.RunningIndex + int64 this.Memory.Length
        this.Next <- next

[<RequireQualifiedAccess>]
module private PipelineRanges =
    let release (ranges: ResizeArray<OwnedRange>) =
        for range in ranges do
            range.Owner.Release()

        ranges.Clear()

    let appendTransferred (target: ResizeArray<OwnedRange>) (source: ResizeArray<OwnedRange>) =
        target.AddRange(source)
        source.Clear()

    let appendBorrowed
        (source: ResizeArray<OwnedRange>)
        offset
        length
        (target: ResizeArray<OwnedRange>)
        =
        let mutable skip = offset
        let mutable remaining = length

        for range in source do
            if remaining > 0L then
                if skip >= int64 range.Length then
                    skip <- skip - int64 range.Length
                else
                    let localOffset = int skip
                    let count = int (min remaining (int64 (range.Length - localOffset)))
                    range.Owner.Retain()

                    try
                        target.Add(
                            { Owner = range.Owner
                              Offset = range.Offset + localOffset
                              Length = count }
                        )
                    with error ->
                        range.Owner.Release()
                        raise error

                    remaining <- remaining - int64 count
                    skip <- 0L

    let retainSuffix (ranges: ResizeArray<OwnedRange>) (inputLength: int64) retainedLength =
        let mutable toRelease = inputLength - retainedLength
        let mutable wholeRanges = 0

        while wholeRanges < ranges.Count && toRelease >= int64 ranges[wholeRanges].Length do
            let range = ranges[wholeRanges]
            toRelease <- toRelease - int64 range.Length
            range.Owner.Release()
            wholeRanges <- wholeRanges + 1

        if wholeRanges > 0 then
            ranges.RemoveRange(0, wholeRanges)

        if toRelease > 0L then
            let range = ranges[0]
            let count = int toRelease

            ranges[0] <-
                { range with
                    Offset = range.Offset + count
                    Length = range.Length - count }

    let asSequence (ranges: ResizeArray<OwnedRange>) =
        let memory (range: OwnedRange) : ReadOnlyMemory<byte> =
            Memory<byte>.op_Implicit(range.Owner.Memory.Slice(range.Offset, range.Length))

        match ranges.Count with
        | 0 -> ReadOnlySequence<byte>.Empty
        | 1 -> ReadOnlySequence<byte>(memory ranges[0])
        | _ ->
            let first = SequenceNode(memory ranges[0])
            let mutable last = first

            for index in 1 .. ranges.Count - 1 do
                let next = SequenceNode(memory ranges[index])
                last.Append(next)
                last <- next

            ReadOnlySequence<byte>(first, 0, last, last.Memory.Length)

type private Stage(processor: IByteStreamProcessor) =
    let pending = ResizeArray<OwnedRange>()

    member _.Processor = processor
    member _.Pending = pending

type private InvocationOutput
    (
        input: ProcessorInput,
        inputRanges: ResizeArray<OwnedRange>,
        pool: MemoryPool<byte>
    ) =
    let mutable ranges = ResizeArray<OwnedRange>()
    let mutable writable: IMemoryOwner<byte> option = None
    let mutable active = true
    let mutable violation: exn option = None

    let fail message =
        let error = InvalidOperationException(message)
        violation <- Some error
        raise error

    let checkActive () =
        if not active then
            fail "The processor output cannot be used after its invocation."

    let checkNoWritable () =
        if writable.IsSome then
            fail "Writable output must be advanced before another output operation."

    let getMemory sizeHint =
        checkActive ()
        checkNoWritable ()

        if sizeHint < 0 then
            fail "The writable output size hint cannot be negative."

        let owner = pool.Rent(max sizeHint 1)
        writable <- Some owner
        owner.Memory

    member _.Seal() =
        checkActive ()
        active <- false

        match violation with
        | Some error -> raise error
        | None -> ()

        if writable.IsSome then
            fail "Writable output was not advanced before the processor returned."

    member _.TakeRanges() =
        let result = ranges
        ranges <- ResizeArray()
        result

    interface IProcessorOutput with
        member _.AppendInput(offset, length) =
            checkActive ()
            checkNoWritable ()

            try
                ProcessorContract.getInputRange input offset length |> ignore
                PipelineRanges.appendBorrowed inputRanges offset length ranges
            with :? InvalidOperationException as error ->
                violation <- Some error
                raise error

        member _.GetMemory(sizeHint) = getMemory sizeHint

        member _.GetSpan(sizeHint) =
            (getMemory sizeHint).Span

        member _.Advance(count) =
            checkActive ()

            match writable with
            | None -> fail "Writable output must be requested before advancing it."
            | Some owner ->
                if count < 0 || count > owner.Memory.Length then
                    fail "The advanced byte count exceeds the current writable output."

                writable <- None

                if count = 0 then
                    owner.Dispose()
                else
                    let segment = SegmentOwner(owner)

                    try
                        ranges.Add(
                            { Owner = segment
                              Offset = 0
                              Length = count }
                        )
                    with error ->
                        segment.Release()
                        raise error

    interface IDisposable with
        member _.Dispose() =
            active <- false

            match writable with
            | Some owner ->
                writable <- None
                owner.Dispose()
            | None -> ()

            PipelineRanges.release ranges

type private PipelineState =
    | Running
    | Completed
    | Faulted
    | Disposed

type internal ProcessorPipeline
    (
        processors: IByteStreamProcessor array,
        sink: IProcessorPipelineSink,
        pool: MemoryPool<byte>
    ) =
    let stages = processors |> Array.map Stage
    let mutable state = Running
    let mutable busy = 0

    let raisePreserving (error: exn) =
        ExceptionDispatchInfo.Capture(error).Throw()
        Unchecked.defaultof<_>

    let contextualizeFailure
        (processor: IByteStreamProcessor)
        (cancellationToken: CancellationToken)
        (error: exn)
        =
        match processor with
        | :? IProcessorFailureContext as context ->
            context.ContextualizeFailure(error, cancellationToken)
        | _ -> error

    let releasePending () =
        for stage in stages do
            PipelineRanges.release stage.Pending

    let enter () =
        if Interlocked.CompareExchange(&busy, 1, 0) <> 0 then
            invalidOp "A pipeline operation is already in progress."

        if state <> Running then
            Volatile.Write(&busy, 0)
            invalidOp "The pipeline is no longer running."

    let abort () =
        state <- Faulted
        releasePending ()

    let disposeAsync () =
        task {
            if Interlocked.CompareExchange(&busy, 1, 0) <> 0 then
                invalidOp "A pipeline operation is still in progress."

            let failures = ResizeArray<exn>()

            try
                if state <> Disposed then
                    try
                        releasePending ()
                    with error ->
                        failures.Add error

                    state <- Disposed

                    let! processorFailures =
                        ProcessorLifetime.disposeAllAsync processors
                        |> _.AsTask()

                    failures.AddRange processorFailures
            finally
                Volatile.Write(&busy, 0)

            match failures.Count with
            | 0 -> ()
            | 1 -> raisePreserving failures[0]
            | _ ->
                raise (
                    AggregateException(
                        "Multiple processor pipeline resources failed during disposal.",
                        failures
                    )
                )
        }

    let writeRanges (ranges: ResizeArray<OwnedRange>) (cancellationToken: CancellationToken) =
        task {
            try
                if ranges.Count > 0 then
                    cancellationToken.ThrowIfCancellationRequested()
                    do! sink.WriteAsync(PipelineRanges.asSequence ranges, cancellationToken).AsTask()
                    cancellationToken.ThrowIfCancellationRequested()
            finally
                PipelineRanges.release ranges
        }

    let processStages isCompleted (cancellationToken: CancellationToken) =
        task {
            let mutable hasNewInput = true

            for index in 0 .. stages.Length - 1 do
                if hasNewInput || isCompleted then
                    cancellationToken.ThrowIfCancellationRequested()
                    let stage = stages[index]

                    let input =
                        { Bytes = PipelineRanges.asSequence stage.Pending
                          IsCompleted = isCompleted }

                    let mutable stagedOutput: ResizeArray<OwnedRange> option = None

                    try
                        use output =
                            new InvocationOutput(input, stage.Pending, pool)

                        let! result =
                            stage.Processor.ProcessAsync(input, output, cancellationToken).AsTask()

                        cancellationToken.ThrowIfCancellationRequested()
                        ProcessorContract.getRetainedSuffix input result |> ignore
                        output.Seal()
                        let emitted = output.TakeRanges()
                        stagedOutput <- Some emitted

                        PipelineRanges.retainSuffix
                            stage.Pending
                            input.Bytes.Length
                            result.RetainedLength
                    with error ->
                        match stagedOutput with
                        | Some emitted -> PipelineRanges.release emitted
                        | None -> ()

                        return
                            raisePreserving (
                                contextualizeFailure
                                    stage.Processor
                                    cancellationToken
                                    error
                            )

                    let emitted = stagedOutput.Value

                    if index + 1 < stages.Length then
                        hasNewInput <- emitted.Count > 0
                        PipelineRanges.appendTransferred stages[index + 1].Pending emitted
                    else
                        do! writeRanges emitted cancellationToken
        }

    member _.PushAsync(owner: IMemoryOwner<byte>, length: int, cancellationToken: CancellationToken) =
        if obj.ReferenceEquals(owner, null) then
            nullArg (nameof owner)

        task {
            if length < 0 || length > owner.Memory.Length then
                invalidArg (nameof length) "The input length must fit within the owned memory."

            let mutable entered = false

            try
                cancellationToken.ThrowIfCancellationRequested()
                enter ()
                entered <- true

                try
                    try
                        if length = 0 then
                            owner.Dispose()
                        elif stages.Length = 0 then
                            try
                                let bytes = Memory<byte>.op_Implicit(owner.Memory.Slice(0, length))
                                do! sink.WriteAsync(ReadOnlySequence<byte>(bytes), cancellationToken).AsTask()
                                cancellationToken.ThrowIfCancellationRequested()
                            finally
                                owner.Dispose()
                        else
                            let segment = SegmentOwner(owner)

                            try
                                stages[0].Pending.Add(
                                    { Owner = segment
                                      Offset = 0
                                      Length = length }
                                )
                            with error ->
                                segment.Release()
                                raise error

                            do! processStages false cancellationToken
                    with error ->
                        abort ()
                        return raise error
                finally
                    Volatile.Write(&busy, 0)
            finally
                if not entered then
                    owner.Dispose()
        }

    member _.CompleteAsync(cancellationToken: CancellationToken) =
        task {
            cancellationToken.ThrowIfCancellationRequested()
            enter ()

            try
                try
                    if stages.Length > 0 then
                        do! processStages true cancellationToken

                    cancellationToken.ThrowIfCancellationRequested()
                    do! sink.CompleteAsync(cancellationToken).AsTask()
                    cancellationToken.ThrowIfCancellationRequested()
                    state <- Completed
                with error ->
                    abort ()
                    return raise error
            finally
                Volatile.Write(&busy, 0)
        }

    interface IDisposable with
        member _.Dispose() =
            disposeAsync().GetAwaiter().GetResult()

    interface IAsyncDisposable with
        member _.DisposeAsync() = ValueTask(disposeAsync ())
