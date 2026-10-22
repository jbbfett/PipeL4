namespace PipeL4.Core

open System
open System.Buffers
open System.Diagnostics
open System.Runtime.ExceptionServices
open System.Threading
open System.Threading.Tasks

type internal IProcessorPipelineSink =
    /// The sequence remains valid until the returned value task completes.
    abstract WriteAsync: bytes: ReadOnlySequence<byte> * cancellationToken: CancellationToken -> ValueTask
    /// Called after the last stage has completed and all output has been written.
    abstract CompleteAsync: cancellationToken: CancellationToken -> ValueTask

[<Struct; NoEquality; NoComparison>]
type internal ProcessorStageMetricsSnapshot =
    { StageIndex: int
      InputBytes: int64
      InputBackedOutputBytes: int64
      GeneratedOutputBytes: int64
      InvocationCount: int64
      SuccessfulInvocationCount: int64
      FailedInvocationCount: int64
      CancelledInvocationCount: int64
      ProcessingDuration: TimeSpan
      CurrentRetainedBytes: int64 }

[<Struct; NoEquality; NoComparison>]
type internal ProcessorPipelineMetricsSnapshot =
    { Stages: ProcessorStageMetricsSnapshot list
      InputBytes: int64
      InputBackedOutputBytes: int64
      GeneratedOutputBytes: int64
      InvocationCount: int64
      SuccessfulInvocationCount: int64
      FailedInvocationCount: int64
      CancelledInvocationCount: int64
      ProcessingDuration: TimeSpan
      CurrentRetainedBytes: int64 }

type private StageMetrics() =
    let mutable inputBytes = 0L
    let mutable inputBackedOutputBytes = 0L
    let mutable generatedOutputBytes = 0L
    let mutable invocationCount = 0L
    let mutable successfulInvocationCount = 0L
    let mutable failedInvocationCount = 0L
    let mutable cancelledInvocationCount = 0L
    let mutable processingDurationTicks = 0L
    let mutable currentRetainedBytes = 0L

    let checkAdd name current increment =
        if increment < 0L then
            invalidArg name "A processor metric increment cannot be negative."

        if current > Int64.MaxValue - increment then
            invalidOp $"Processor metric '{name}' overflowed."

    member _.CheckInputBytes(value) = checkAdd "InputBytes" inputBytes value

    member _.CheckInputBackedOutputBytes(value) =
        checkAdd "InputBackedOutputBytes" inputBackedOutputBytes value

    member _.CheckGeneratedOutputBytes(value) =
        checkAdd "GeneratedOutputBytes" generatedOutputBytes value

    member _.CheckInvocationCount() = checkAdd "InvocationCount" invocationCount 1L

    member _.CheckSuccessfulInvocationCount() =
        checkAdd "SuccessfulInvocationCount" successfulInvocationCount 1L

    member _.CheckFailedInvocationCount() =
        checkAdd "FailedInvocationCount" failedInvocationCount 1L

    member _.CheckCancelledInvocationCount() =
        checkAdd "CancelledInvocationCount" cancelledInvocationCount 1L

    member _.CheckProcessingDuration value =
        checkAdd "ProcessingDuration" processingDurationTicks value

    member _.AddInputBytes value = inputBytes <- inputBytes + value

    member _.AddInputBackedOutputBytes value =
        inputBackedOutputBytes <- inputBackedOutputBytes + value

    member _.AddGeneratedOutputBytes value =
        generatedOutputBytes <- generatedOutputBytes + value

    member _.AddInvocation() = invocationCount <- invocationCount + 1L

    member _.AddSuccessfulInvocation() =
        successfulInvocationCount <- successfulInvocationCount + 1L

    member _.AddFailedInvocation() = failedInvocationCount <- failedInvocationCount + 1L

    member _.AddCancelledInvocation() =
        cancelledInvocationCount <- cancelledInvocationCount + 1L

    member _.AddProcessingDuration value =
        processingDurationTicks <- processingDurationTicks + value

    member _.SetCurrentRetainedBytes value = currentRetainedBytes <- value

    member _.Snapshot stageIndex =
        { StageIndex = stageIndex
          InputBytes = inputBytes
          InputBackedOutputBytes = inputBackedOutputBytes
          GeneratedOutputBytes = generatedOutputBytes
          InvocationCount = invocationCount
          SuccessfulInvocationCount = successfulInvocationCount
          FailedInvocationCount = failedInvocationCount
          CancelledInvocationCount = cancelledInvocationCount
          ProcessingDuration = TimeSpan.FromTicks processingDurationTicks
          CurrentRetainedBytes = currentRetainedBytes }

type internal ProcessorPipelineMetrics(stageCount: int) =
    let stages =
        if stageCount < 0 then
            invalidArg (nameof stageCount) "A processor stage count cannot be negative."

        Array.init stageCount (fun _ -> StageMetrics())

    let gate = obj ()

    let stage index =
        if index < 0 || index >= stages.Length then
            invalidArg (nameof index) "A processor stage index is outside the configured pipeline."

        stages[index]

    let checkedDurationTicks (value: int64) =
        if value < 0L then
            invalidArg (nameof value) "A processor duration cannot be negative."

        value

    member _.StageCount = stages.Length

    member internal _.AcceptInput(index, value: int64) =
        lock gate (fun () ->
            let metrics = stage index
            metrics.CheckInputBytes value
            metrics.AddInputBytes value)

    member internal _.BeginInvocation index =
        lock gate (fun () ->
            let metrics = stage index
            metrics.CheckInvocationCount()
            metrics.AddInvocation())

    member internal _.ValidateSuccessful(
        index,
        inputBackedOutputBytes: int64,
        generatedOutputBytes: int64,
        durationTicks: int64
    ) =
        lock gate (fun () ->
            let metrics = stage index
            let durationTicks = checkedDurationTicks durationTicks

            metrics.CheckInputBackedOutputBytes inputBackedOutputBytes
            metrics.CheckGeneratedOutputBytes generatedOutputBytes
            metrics.CheckProcessingDuration durationTicks

            metrics.CheckSuccessfulInvocationCount())

    member internal _.CommitSuccessful(
        index,
        inputBackedOutputBytes: int64,
        generatedOutputBytes: int64,
        durationTicks: int64,
        retainedBytes: int64
    ) =
        lock gate (fun () ->
            let metrics = stage index
            let durationTicks = checkedDurationTicks durationTicks

            metrics.CheckInputBackedOutputBytes inputBackedOutputBytes
            metrics.CheckGeneratedOutputBytes generatedOutputBytes
            metrics.CheckProcessingDuration durationTicks
            metrics.CheckSuccessfulInvocationCount()
            metrics.AddInputBackedOutputBytes inputBackedOutputBytes
            metrics.AddGeneratedOutputBytes generatedOutputBytes
            metrics.AddProcessingDuration durationTicks
            metrics.AddSuccessfulInvocation()
            metrics.SetCurrentRetainedBytes retainedBytes)

    member internal _.CommitFailed(index, durationTicks: int64, cancelled: bool) =
        lock gate (fun () ->
            let metrics = stage index
            let durationTicks = checkedDurationTicks durationTicks

            metrics.CheckProcessingDuration durationTicks

            if cancelled then
                metrics.CheckCancelledInvocationCount()
            else
                metrics.CheckFailedInvocationCount()

            metrics.AddProcessingDuration durationTicks

            if cancelled then
                metrics.AddCancelledInvocation()
            else
                metrics.AddFailedInvocation())

    member internal _.ClearRetainedBytes() =
        lock gate (fun () ->
            for metrics in stages do
                metrics.SetCurrentRetainedBytes 0L)

    member _.GetSnapshot() =
        lock gate (fun () ->
            let snapshots =
                stages
                |> Array.mapi (fun index metrics -> metrics.Snapshot index)
                |> Array.toList

            let sum name selector =
                let mutable total = 0L

                for snapshot in snapshots do
                    let value = selector snapshot

                    if value < 0L || total > Int64.MaxValue - value then
                        invalidOp $"Processor metric '{name}' overflowed while aggregating stages."

                    total <- total + value

                total

            let durationTicks = sum "ProcessingDuration" (fun snapshot -> snapshot.ProcessingDuration.Ticks)

            { Stages = snapshots
              InputBytes = sum "InputBytes" (fun snapshot -> snapshot.InputBytes)
              InputBackedOutputBytes =
                  sum "InputBackedOutputBytes" (fun snapshot -> snapshot.InputBackedOutputBytes)
              GeneratedOutputBytes = sum "GeneratedOutputBytes" (fun snapshot -> snapshot.GeneratedOutputBytes)
              InvocationCount = sum "InvocationCount" (fun snapshot -> snapshot.InvocationCount)
              SuccessfulInvocationCount =
                  sum "SuccessfulInvocationCount" (fun snapshot -> snapshot.SuccessfulInvocationCount)
              FailedInvocationCount = sum "FailedInvocationCount" (fun snapshot -> snapshot.FailedInvocationCount)
              CancelledInvocationCount =
                  sum "CancelledInvocationCount" (fun snapshot -> snapshot.CancelledInvocationCount)
              ProcessingDuration = TimeSpan.FromTicks durationTicks
              CurrentRetainedBytes = sum "CurrentRetainedBytes" (fun snapshot -> snapshot.CurrentRetainedBytes) })

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
        let mutable failures: ResizeArray<exn> option = None

        for range in ranges do
            try
                range.Owner.Release()
            with error ->
                match failures with
                | Some values -> values.Add error
                | None ->
                    let values = ResizeArray<exn>()
                    values.Add error
                    failures <- Some values

        ranges.Clear()

        match failures with
        | Some values -> values.ToArray()
        | None -> Array.empty

    let raiseFailures (failures: exn array) =
        match failures.Length with
        | 0 -> ()
        | 1 ->
            ExceptionDispatchInfo.Capture(failures[0]).Throw()
        | _ ->
            raise (
                AggregateException(
                    "Multiple processor pipeline resources failed during cleanup.",
                    failures
                )
            )

    let appendTransferred (target: ResizeArray<OwnedRange>) (source: ResizeArray<OwnedRange>) =
        target.AddRange source
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
                        target.Add
                            { Owner = range.Owner
                              Offset = range.Offset + localOffset
                              Length = count }
                    with error ->
                        try
                            range.Owner.Release()
                        with cleanupError ->
                            raise (AggregateException("Processor output cleanup failed.", [| error; cleanupError |]))

                        ExceptionDispatchInfo.Capture(error).Throw()

                    remaining <- remaining - int64 count
                    skip <- 0L

    let retainSuffix (ranges: ResizeArray<OwnedRange>) (inputLength: int64) retainedLength =
        let mutable toRelease = inputLength - retainedLength
        let mutable wholeRanges = 0
        let mutable failures: ResizeArray<exn> option = None

        while wholeRanges < ranges.Count && toRelease >= int64 ranges[wholeRanges].Length do
            let range = ranges[wholeRanges]
            toRelease <- toRelease - int64 range.Length

            try
                range.Owner.Release()
            with error ->
                match failures with
                | Some values -> values.Add error
                | None ->
                    let values = ResizeArray<exn>()
                    values.Add error
                    failures <- Some values

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

        match failures with
        | Some values -> raiseFailures (values.ToArray())
        | None -> ()

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
        pool: MemoryPool<byte>,
        memoryAccounting: ConnectionMemoryAccounting
    ) =
    let mutable ranges = ResizeArray<OwnedRange>()
    let mutable writable: IMemoryOwner<byte> option = None
    let mutable active = true
    let mutable violation: exn option = None
    let mutable inputBackedOutputBytes = 0L
    let mutable generatedOutputBytes = 0L

    let fail message =
        let error = InvalidOperationException message
        violation <- Some error
        raise error

    let checkActive () =
        if not active then
            fail "The processor output cannot be used after its invocation."

    let checkNoWritable () =
        if writable.IsSome then
            fail "Writable output must be advanced before another output operation."

    let addOutputBytes name current increment =
        if increment < 0L || current > Int64.MaxValue - increment then
            fail $"The processor {name} output count overflowed."

        current + increment

    let getMemory sizeHint =
        checkActive ()
        checkNoWritable ()

        if sizeHint < 0 then
            fail "The writable output size hint cannot be negative."

        let owner = memoryAccounting.Track(pool.Rent(max sizeHint 1))
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

    member _.InputBackedOutputBytes = inputBackedOutputBytes
    member _.GeneratedOutputBytes = generatedOutputBytes

    interface IProcessorOutput with
        member _.AppendInput(offset, length) =
            checkActive ()
            checkNoWritable ()

            try
                ProcessorContract.getInputRange input offset length |> ignore
                PipelineRanges.appendBorrowed inputRanges offset length ranges
                inputBackedOutputBytes <-
                    addOutputBytes "input-backed" inputBackedOutputBytes length
            with :? InvalidOperationException as error ->
                violation <- Some error
                raise error

        member _.GetMemory sizeHint = getMemory sizeHint

        member _.GetSpan sizeHint =
            (getMemory sizeHint).Span

        member _.Advance count =
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
                    let segment = SegmentOwner owner

                    try
                        ranges.Add
                            { Owner = segment
                              Offset = 0
                              Length = count }
                    with error ->
                        try
                            segment.Release()
                        with cleanupError ->
                            raise (AggregateException("Processor output cleanup failed.", [| error; cleanupError |]))

                        ExceptionDispatchInfo.Capture(error).Throw()

                    generatedOutputBytes <-
                        addOutputBytes "generated" generatedOutputBytes (int64 count)

    interface IDisposable with
        member _.Dispose() =
            active <- false

            let mutable writableFailure: exn option = None

            match writable with
            | Some owner ->
                writable <- None
                try
                    owner.Dispose()
                with error ->
                    writableFailure <- Some error
            | None -> ()

            let rangeFailures = PipelineRanges.release ranges

            match writableFailure, rangeFailures.Length with
            | None, 0 -> ()
            | Some error, 0 -> PipelineRanges.raiseFailures [| error |]
            | None, _ -> PipelineRanges.raiseFailures rangeFailures
            | Some error, _ ->
                PipelineRanges.raiseFailures (Array.append [| error |] rangeFailures)

type private PipelineState =
    | Running
    | Completed
    | Faulted
    | Disposed

type internal ProcessorPipeline
    (
        processors: IByteStreamProcessor array,
        sink: IProcessorPipelineSink,
        pool: MemoryPool<byte>,
        memoryAccounting: ConnectionMemoryAccounting,
        metrics: ProcessorPipelineMetrics
    ) =
    do
        if metrics.StageCount <> processors.Length then
            invalidArg (nameof metrics) "The processor metrics stage count does not match the pipeline."

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
        let mutable failures: ResizeArray<exn> option = None

        for stage in stages do
            for error in PipelineRanges.release stage.Pending do
                match failures with
                | Some values -> values.Add error
                | None ->
                    let values = ResizeArray<exn>()
                    values.Add error
                    failures <- Some values

        match failures with
        | Some values -> values.ToArray()
        | None -> Array.empty

    let transitionToTerminal terminalState =
        state <- terminalState
        metrics.ClearRetainedBytes()

    let enter () =
        if Interlocked.CompareExchange(&busy, 1, 0) <> 0 then
            invalidOp "A pipeline operation is already in progress."

        if state <> Running then
            Volatile.Write(&busy, 0)
            invalidOp "The pipeline is no longer running."

    let abort () =
        transitionToTerminal Faulted
        releasePending ()

    let disposeAsync () =
        task {
            if Interlocked.CompareExchange(&busy, 1, 0) <> 0 then
                invalidOp "A pipeline operation is still in progress."

            let mutable failures: ResizeArray<exn> option = None

            try
                if state <> Disposed then
                    transitionToTerminal Disposed

                    try
                        for error in releasePending () do
                            match failures with
                            | Some values -> values.Add error
                            | None ->
                                let values = ResizeArray<exn>()
                                values.Add error
                                failures <- Some values
                    with error ->
                        match failures with
                        | Some values -> values.Add error
                        | None ->
                            let values = ResizeArray<exn>()
                            values.Add error
                            failures <- Some values

                    let! processorFailures =
                        ProcessorLifetime.disposeAllAsync processors
                        |> _.AsTask()

                    for error in processorFailures do
                        match failures with
                        | Some values -> values.Add error
                        | None ->
                            let values = ResizeArray<exn>()
                            values.Add error
                            failures <- Some values
            finally
                Volatile.Write(&busy, 0)

            let failures =
                match failures with
                | Some values -> values.ToArray()
                | None -> Array.empty

            match failures.Length with
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
            let mutable operationError: exn option = None

            try
                if ranges.Count > 0 then
                    cancellationToken.ThrowIfCancellationRequested()
                    do! sink.WriteAsync(PipelineRanges.asSequence ranges, cancellationToken).AsTask()
                    cancellationToken.ThrowIfCancellationRequested()
            with error ->
                operationError <- Some error

            let cleanupFailures = PipelineRanges.release ranges

            match operationError, cleanupFailures.Length with
            | None, 0 -> ()
            | Some error, 0 -> return raisePreserving error
            | None, _ -> PipelineRanges.raiseFailures cleanupFailures
            | Some error, _ ->
                let failures = Array.append [| error |] cleanupFailures
                return raise (AggregateException("Processor pipeline operation and cleanup failed.", failures))
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
                    let mutable invocationStarted = false
                    let mutable invocationDurationTicks = 0L
                    let mutable invocationDurationRecorded = false
                    let mutable invocationStartedAt = 0L

                    let recordInvocationDuration startedAt =
                        if not invocationDurationRecorded then
                            invocationDurationTicks <- Stopwatch.GetElapsedTime(startedAt).Ticks
                            invocationDurationRecorded <- true

                    try
                        let output =
                            new InvocationOutput(input, stage.Pending, pool, memoryAccounting)

                        let mutable invocationError: exn option = None

                        try
                            metrics.BeginInvocation(index)
                            invocationStarted <- true
                            invocationStartedAt <- Stopwatch.GetTimestamp()
                            let! result = stage.Processor.ProcessAsync(input, output, cancellationToken).AsTask()
                            recordInvocationDuration invocationStartedAt

                            cancellationToken.ThrowIfCancellationRequested()
                            ProcessorContract.getRetainedSuffix input result |> ignore
                            output.Seal()
                            let emitted = output.TakeRanges()
                            stagedOutput <- Some emitted

                            metrics.ValidateSuccessful(
                                index,
                                output.InputBackedOutputBytes,
                                output.GeneratedOutputBytes,
                                invocationDurationTicks
                            )

                            // Retention can release arbitrary synchronous storage.
                            // Perform that work outside the metrics monitor, then
                            // publish the already-validated result.
                            PipelineRanges.retainSuffix
                                stage.Pending
                                input.Bytes.Length
                                result.RetainedLength

                            metrics.CommitSuccessful(
                                index,
                                output.InputBackedOutputBytes,
                                output.GeneratedOutputBytes,
                                invocationDurationTicks,
                                result.RetainedLength
                            )
                        with error ->
                            invocationError <- Some error

                        let outputCleanupError =
                            try
                                (output :> IDisposable).Dispose()
                                None
                            with error ->
                                Some error

                        match invocationError, outputCleanupError with
                        | None, None -> ()
                        | Some error, None -> raisePreserving error
                        | None, Some error -> raisePreserving error
                        | Some error, Some cleanupError ->
                            raise (
                                AggregateException(
                                    "Processor invocation and output cleanup failed.",
                                    [| error; cleanupError |]
                                )
                            )
                    with error ->
                        let failures = ResizeArray<exn>()

                        if invocationStarted then
                            recordInvocationDuration invocationStartedAt

                            let cancelled =
                                match error with
                                | :? OperationCanceledException when cancellationToken.IsCancellationRequested ->
                                    true
                                | _ -> false

                            try
                                metrics.CommitFailed(index, invocationDurationTicks, cancelled)
                            with metricError ->
                                failures.Add metricError

                        match stagedOutput with
                        | Some emitted -> failures.AddRange(PipelineRanges.release emitted)
                        | None -> ()

                        let primary =
                            contextualizeFailure
                                stage.Processor
                                cancellationToken
                                error

                        if failures.Count = 0 then
                            return raisePreserving primary
                        else
                            let allFailures = Array.append [| primary |] (failures.ToArray())
                            return raise (AggregateException("Processor invocation and cleanup failed.", allFailures))

                    let emitted = stagedOutput.Value

                    if index + 1 < stages.Length then
                        hasNewInput <- emitted.Count > 0
                        let emittedLength = emitted |> Seq.sumBy (fun range -> int64 range.Length)
                        PipelineRanges.appendTransferred stages[index + 1].Pending emitted
                        metrics.AcceptInput(index + 1, emittedLength)
                    else
                        do! writeRanges emitted cancellationToken
        }

    new(processors, sink, pool) =
        new ProcessorPipeline(
            processors,
            sink,
            pool,
            ConnectionMemoryAccounting(),
            ProcessorPipelineMetrics processors.Length
        )

    new(processors, sink, pool, memoryAccounting) =
        new ProcessorPipeline(
            processors,
            sink,
            pool,
            memoryAccounting,
            ProcessorPipelineMetrics processors.Length
        )

    member _.GetStageMetricsSnapshot() =
        (metrics.GetSnapshot()).Stages

    member _.GetConnectionMemorySnapshot() = memoryAccounting.GetSnapshot()

    member _.PushAsync(owner: IMemoryOwner<byte>, length: int, cancellationToken: CancellationToken) =
        if obj.ReferenceEquals(owner, null) then
            nullArg (nameof owner)

        task {
            if length < 0 || length > owner.Memory.Length then
                invalidArg (nameof length) "The input length must fit within the owned memory."

            let trackedOwner = memoryAccounting.Track owner
            let mutable entered = false

            try
                cancellationToken.ThrowIfCancellationRequested()
                enter ()
                entered <- true

                try
                    try
                        if length = 0 then
                            trackedOwner.Dispose()
                        elif stages.Length = 0 then
                            try
                                let bytes =
                                    Memory<byte>.op_Implicit(trackedOwner.Memory.Slice(0, length))

                                do! sink.WriteAsync(ReadOnlySequence<byte> bytes, cancellationToken).AsTask()
                                cancellationToken.ThrowIfCancellationRequested()
                            finally
                                trackedOwner.Dispose()
                        else
                            let segment = SegmentOwner trackedOwner

                            try
                                stages[0].Pending.Add
                                    { Owner = segment
                                      Offset = 0
                                      Length = length }
                            with error ->
                                try
                                    segment.Release()
                                with cleanupError ->
                                    raise (AggregateException("Processor input cleanup failed.", [| error; cleanupError |]))

                                ExceptionDispatchInfo.Capture(error).Throw()

                            metrics.AcceptInput(0, int64 length)
                            do! processStages false cancellationToken
                    with error ->
                        let cleanupFailures = abort ()

                        if cleanupFailures.Length = 0 then
                            return raisePreserving error
                        else
                            let failures = Array.append [| error |] cleanupFailures
                            return raise (AggregateException("Processor operation and cleanup failed.", failures))
                finally
                    Volatile.Write(&busy, 0)
            finally
                if not entered then
                    trackedOwner.Dispose()
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
                    let cleanupFailures = abort ()

                    if cleanupFailures.Length = 0 then
                        return raisePreserving error
                    else
                        let failures = Array.append [| error |] cleanupFailures
                        return raise (AggregateException("Processor completion and cleanup failed.", failures))
            finally
                Volatile.Write(&busy, 0)
        }

    interface IDisposable with
        member _.Dispose() =
            disposeAsync().GetAwaiter().GetResult()

    interface IAsyncDisposable with
        member _.DisposeAsync() = ValueTask(disposeAsync ())
