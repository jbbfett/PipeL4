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
      MaximumInvocationDuration: TimeSpan
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
      MaximumInvocationDuration: TimeSpan
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
    let mutable maximumInvocationDurationTicks = 0L
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

    member _.CheckMaximumInvocationDuration value =
        if value < 0L then
            invalidArg "MaximumInvocationDuration" "A processor duration cannot be negative."

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

    member _.UpdateMaximumInvocationDuration value =
        maximumInvocationDurationTicks <- max maximumInvocationDurationTicks value

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
          MaximumInvocationDuration = TimeSpan.FromTicks maximumInvocationDurationTicks
          CurrentRetainedBytes = currentRetainedBytes }

type internal ProcessorPipelineMetrics(stageCount: int, ?diagnosticsEnabled: bool) =
    let stages =
        if stageCount < 0 then
            invalidArg (nameof stageCount) "A processor stage count cannot be negative."

        Array.init stageCount (fun _ -> StageMetrics())

    let gate = obj ()
    let diagnosticsEnabled = defaultArg diagnosticsEnabled true

    let stage index =
        if index < 0 || index >= stages.Length then
            invalidArg (nameof index) "A processor stage index is outside the configured pipeline."

        stages[index]

    let checkedDurationTicks (value: int64) =
        if value < 0L then
            invalidArg (nameof value) "A processor duration cannot be negative."

        value

    member _.StageCount = stages.Length
    member _.DiagnosticsEnabled = diagnosticsEnabled

    member internal _.AcceptInput(index, value: int64) =
        if diagnosticsEnabled then
            lock gate (fun () ->
                let metrics = stage index
                metrics.CheckInputBytes value
                metrics.AddInputBytes value)

    member internal _.BeginInvocation index =
        if diagnosticsEnabled then
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
        if diagnosticsEnabled then
            lock gate (fun () ->
                let metrics = stage index
                let durationTicks = checkedDurationTicks durationTicks

                metrics.CheckInputBackedOutputBytes inputBackedOutputBytes
                metrics.CheckGeneratedOutputBytes generatedOutputBytes
                metrics.CheckProcessingDuration durationTicks
                metrics.CheckMaximumInvocationDuration durationTicks

                metrics.CheckSuccessfulInvocationCount())

    member internal _.CommitSuccessful(
        index,
        inputBackedOutputBytes: int64,
        generatedOutputBytes: int64,
        durationTicks: int64,
        retainedBytes: int64
    ) =
        if diagnosticsEnabled then
            lock gate (fun () ->
                let metrics = stage index
                let durationTicks = checkedDurationTicks durationTicks

                metrics.CheckInputBackedOutputBytes inputBackedOutputBytes
                metrics.CheckGeneratedOutputBytes generatedOutputBytes
                metrics.CheckProcessingDuration durationTicks
                metrics.CheckMaximumInvocationDuration durationTicks
                metrics.CheckSuccessfulInvocationCount()
                metrics.AddInputBackedOutputBytes inputBackedOutputBytes
                metrics.AddGeneratedOutputBytes generatedOutputBytes
                metrics.AddProcessingDuration durationTicks
                metrics.UpdateMaximumInvocationDuration durationTicks
                metrics.AddSuccessfulInvocation()
                metrics.SetCurrentRetainedBytes retainedBytes)

    member internal _.CommitFailed(index, durationTicks: int64, cancelled: bool) =
        if diagnosticsEnabled then
            lock gate (fun () ->
                let metrics = stage index
                let durationTicks = checkedDurationTicks durationTicks

                metrics.CheckProcessingDuration durationTicks
                metrics.CheckMaximumInvocationDuration durationTicks

                if cancelled then
                    metrics.CheckCancelledInvocationCount()
                else
                    metrics.CheckFailedInvocationCount()

                metrics.AddProcessingDuration durationTicks
                metrics.UpdateMaximumInvocationDuration durationTicks

                if cancelled then
                    metrics.AddCancelledInvocation()
                else
                    metrics.AddFailedInvocation())

    member internal _.ClearRetainedBytes() =
        if diagnosticsEnabled then
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
            let maximumDurationTicks =
                snapshots
                |> List.fold (fun current snapshot -> max current snapshot.MaximumInvocationDuration.Ticks) 0L

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
              MaximumInvocationDuration = TimeSpan.FromTicks maximumDurationTicks
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
      Logical: CapacityReservation
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
    let private addFailure (failures: ResizeArray<exn> option) (error: exn) : ResizeArray<exn> option =
        match failures with
        | Some values ->
            values.Add error
            Some values
        | None ->
            let values = ResizeArray<exn>()
            values.Add error
            Some values

    let private releaseLogical (range: OwnedRange) =
        try
            // Terminal cleanup uses the idempotent reservation boundary so a
            // logical phase can safely be repeated by the outer failure path.
            (range.Logical :> IDisposable).Dispose()
            None
        with error ->
            Some error

    let private releasePhysical (range: OwnedRange) =
        try
            range.Owner.Release()
            None
        with error ->
            Some error

    let releaseLogicalRangesWithNotification (onFailure: exn -> unit) (ranges: ResizeArray<OwnedRange>) =
        let mutable failures: ResizeArray<exn> option = None

        for range in ranges do
            match releaseLogical range with
            | Some error ->
                onFailure error
                failures <- addFailure failures error
            | None -> ()

        match failures with
        | Some values -> values.ToArray()
        | None -> Array.empty

    let releaseLogicalRanges (ranges: ResizeArray<OwnedRange>) =
        releaseLogicalRangesWithNotification ignore ranges

    let releasePhysicalRangesWithNotification (onFailure: exn -> unit) (ranges: ResizeArray<OwnedRange>) =
        let mutable failures: ResizeArray<exn> option = None

        for range in ranges do
            match releasePhysical range with
            | Some error ->
                onFailure error
                failures <- addFailure failures error
            | None -> ()

        ranges.Clear()

        match failures with
        | Some values -> values.ToArray()
        | None -> Array.empty

    let releasePhysicalRanges (ranges: ResizeArray<OwnedRange>) =
        releasePhysicalRangesWithNotification ignore ranges

    let releaseWithNotification (onFailure: exn -> unit) (ranges: ResizeArray<OwnedRange>) =
        let logicalFailures = releaseLogicalRangesWithNotification onFailure ranges
        let physicalFailures = releasePhysicalRangesWithNotification onFailure ranges

        match logicalFailures.Length, physicalFailures.Length with
        | 0, 0 -> Array.empty
        | 0, _ -> physicalFailures
        | _, 0 -> logicalFailures
        | _ -> Array.append logicalFailures physicalFailures

    let release (ranges: ResizeArray<OwnedRange>) =
        releaseWithNotification ignore ranges

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
        (logicalCapacity: CapacityBudget)
        (target: ResizeArray<OwnedRange>)
        =
        let logicalReservation =
            if length = 0L then
                None
            else
                match logicalCapacity.TryReserve length with
                | ValueSome reservation -> Some reservation
                | ValueNone when length > logicalCapacity.LimitBytes ->
                    raise (
                        ArgumentOutOfRangeException(
                            nameof length,
                            length,
                            "The logical output occurrence exceeds the configured capacity limit."
                        )
                    )
                | ValueNone ->
                    raise (
                        InvalidOperationException(
                            "The logical output occurrence could not be admitted synchronously."
                        )
                    )

        let mutable skip = offset
        let mutable remaining = length
        let mutable reservation = logicalReservation

        let takeReservation count =
            match reservation with
            | None -> invalidOp "A logical output range was created without a reservation."
            | Some current when current.RemainingAmount = int64 count ->
                reservation <- None
                current
            | Some current ->
                let split = current.Split(int64 count)

                if current.RemainingAmount = 0L then
                    reservation <- None

                split

        try
            for range in source do
                if remaining > 0L then
                    if skip >= int64 range.Length then
                        skip <- skip - int64 range.Length
                    else
                        let localOffset = int skip
                        let count = int (min remaining (int64 (range.Length - localOffset)))
                        let rangeReservation = takeReservation count
                        let mutable retained = false

                        try
                            range.Owner.Retain()
                            retained <- true

                            target.Add
                                { Owner = range.Owner
                                  Logical = rangeReservation
                                  Offset = range.Offset + localOffset
                                  Length = count }
                        with error ->
                            let failures = ResizeArray<exn>()

                            try
                                rangeReservation.Release()
                            with cleanupError ->
                                failures.Add cleanupError

                            if retained then
                                try
                                    range.Owner.Release()
                                with cleanupError ->
                                    failures.Add cleanupError

                            if failures.Count = 0 then
                                ExceptionDispatchInfo.Capture(error).Throw()
                            else
                                raise (
                                    AggregateException(
                                        "Processor output cleanup failed.",
                                        Array.append [| error |] (failures.ToArray())
                                    )
                                )

                        remaining <- remaining - int64 count
                        skip <- 0L

            if remaining <> 0L then
                invalidOp "A processor output range exceeded the current input."

            match reservation with
            | Some unused -> (unused :> IDisposable).Dispose()
            | None -> ()
        with error ->
            let failures = ResizeArray<exn>()

            match reservation with
            | Some unused ->
                try
                    (unused :> IDisposable).Dispose()
                with cleanupError ->
                    failures.Add cleanupError
            | None -> ()

            if failures.Count = 0 then
                ExceptionDispatchInfo.Capture(error).Throw()
            else
                raise (
                    AggregateException(
                        "Processor output admission and cleanup failed.",
                        Array.append [| error |] (failures.ToArray())
                    )
                )

    let retainSuffix (ranges: ResizeArray<OwnedRange>) (inputLength: int64) retainedLength =
        let mutable toRelease = inputLength - retainedLength
        let mutable wholeRanges = 0
        let mutable failures: ResizeArray<exn> option = None

        while wholeRanges < ranges.Count && toRelease >= int64 ranges[wholeRanges].Length do
            let range = ranges[wholeRanges]
            toRelease <- toRelease - int64 range.Length

            wholeRanges <- wholeRanges + 1

        // Apply every logical release, including the partial boundary, before
        // attempting any physical owner disposal.
        if wholeRanges > 0 then
            for index in 0 .. wholeRanges - 1 do
                match releaseLogical ranges[index] with
                | Some error -> failures <- addFailure failures error
                | None -> ()

        if toRelease > 0L then
            let range = ranges[wholeRanges]
            let count = int toRelease

            try
                range.Logical.ReleasePartial(toRelease)
            with error ->
                failures <- addFailure failures error

            ranges[wholeRanges] <-
                { range with
                    Offset = range.Offset + count
                    Length = range.Length - count }

        if wholeRanges > 0 then
            for index in 0 .. wholeRanges - 1 do
                match releasePhysical ranges[index] with
                | Some error -> failures <- addFailure failures error
                | None -> ()

            ranges.RemoveRange(0, wholeRanges)

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
        memoryAccounting: ConnectionMemoryAccounting,
        logicalCapacity: CapacityBudget,
        diagnostics: DirectionRuntimeDiagnostics
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

        let owner = memoryAccounting.Rent(pool, max sizeHint 1)
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

    member _.ReleaseLogical() =
        PipelineRanges.releaseLogicalRanges ranges

    member _.InputBackedOutputBytes = inputBackedOutputBytes
    member _.GeneratedOutputBytes = generatedOutputBytes

    interface IProcessorOutput with
        member _.AppendInput(offset, length) =
            checkActive ()
            checkNoWritable ()

            try
                ProcessorContract.getInputRange input offset length |> ignore
                PipelineRanges.appendBorrowed inputRanges offset length logicalCapacity ranges
                inputBackedOutputBytes <-
                    addOutputBytes "input-backed" inputBackedOutputBytes length
            with error ->
                match error with
                | :? ArgumentOutOfRangeException when error.Message.Contains("logical output occurrence") ->
                    diagnostics.RecordProgressFailure ProgressFailureCategory.SynchronousLogicalOutputExhaustion
                | :? InvalidOperationException when error.Message.Contains("logical output occurrence") ->
                    diagnostics.RecordProgressFailure ProgressFailureCategory.SynchronousLogicalOutputExhaustion
                | _ -> ()

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

                if count = 0 then
                    writable <- None
                    owner.Dispose()
                else
                    let logicalReservation =
                        try
                            match logicalCapacity.TryReserve(int64 count) with
                            | ValueSome reservation -> reservation
                            | ValueNone when int64 count > logicalCapacity.LimitBytes ->
                                raise (
                                    ArgumentOutOfRangeException(
                                        nameof count,
                                        count,
                                        "The logical generated output occurrence exceeds the configured capacity limit."
                                    )
                                )
                            | ValueNone ->
                                raise (
                                    InvalidOperationException(
                                        "The logical generated output occurrence could not be admitted synchronously."
                                    )
                                )
                        with error ->
                            diagnostics.RecordProgressFailure
                                ProgressFailureCategory.SynchronousLogicalOutputExhaustion

                            violation <- Some error
                            raise error

                    let segment = SegmentOwner owner
                    writable <- None

                    try
                        ranges.Add
                            { Owner = segment
                              Logical = logicalReservation
                              Offset = 0
                              Length = count }
                    with error ->
                        let failures = ResizeArray<exn>()

                        try
                            logicalReservation.Release()
                        with cleanupError ->
                            failures.Add cleanupError

                        try
                            segment.Release()
                        with cleanupError ->
                            failures.Add cleanupError

                        violation <- Some error

                        if failures.Count = 0 then
                            ExceptionDispatchInfo.Capture(error).Throw()
                        else
                            raise (
                                AggregateException(
                                    "Processor output cleanup failed.",
                                    Array.append [| error |] (failures.ToArray())
                                )
                            )

                    generatedOutputBytes <-
                        addOutputBytes "generated" generatedOutputBytes (int64 count)

    interface IDisposable with
        member _.Dispose() =
            active <- false

            // Release staged ranges first. PipelineRanges performs a logical
            // pass before physical owner disposal, so a blocking writable
            // owner cannot strand the invocation's abandoned logical credit.
            let rangeFailures = PipelineRanges.release ranges
            let mutable writableFailure: exn option = None

            match writable with
            | Some owner ->
                writable <- None
                try
                    owner.Dispose()
                with error ->
                    writableFailure <- Some error
            | None -> ()

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
        logicalCapacity: CapacityBudget,
        metrics: ProcessorPipelineMetrics,
        ?diagnostics: DirectionRuntimeDiagnostics
    ) =
    do
        if metrics.StageCount <> processors.Length then
            invalidArg (nameof metrics) "The processor metrics stage count does not match the pipeline."

        if obj.ReferenceEquals(logicalCapacity, null) then
            nullArg (nameof logicalCapacity)

    let stages = processors |> Array.map Stage
    let diagnostics = defaultArg diagnostics (DirectionRuntimeDiagnostics(false))
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

    let collectFailures (target: ResizeArray<exn> option) (failures: exn array) =
        if failures.Length = 0 then
            target
        else
            match target with
            | Some values ->
                values.AddRange failures
                Some values
            | None ->
                let values = ResizeArray<exn>()
                values.AddRange failures
                Some values

    let releasePendingLogical () =
        let mutable failures: ResizeArray<exn> option = None

        for stage in stages do
            failures <- collectFailures failures (PipelineRanges.releaseLogicalRanges stage.Pending)

        match failures with
        | Some values -> values.ToArray()
        | None -> Array.empty

    let releasePendingPhysical () =
        let mutable failures: ResizeArray<exn> option = None

        for stage in stages do
            failures <- collectFailures failures (PipelineRanges.releasePhysicalRanges stage.Pending)

        match failures with
        | Some values -> values.ToArray()
        | None -> Array.empty

    let releasePendingWithNotification (onFailure: exn -> unit) =
        let mutable failures: ResizeArray<exn> option = None

        for stage in stages do
            failures <- collectFailures failures (PipelineRanges.releaseLogicalRangesWithNotification onFailure stage.Pending)

        for stage in stages do
            failures <- collectFailures failures (PipelineRanges.releasePhysicalRangesWithNotification onFailure stage.Pending)

        match failures with
        | Some values -> values.ToArray()
        | None -> Array.empty

    let releasePending () =
        releasePendingWithNotification ignore

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

    let prepareTerminalLogicalCleanup () =
        transitionToTerminal Faulted
        releasePendingLogical ()

    let pendingLogicalBytes () =
        let mutable total = 0L

        for stage in stages do
            for range in stage.Pending do
                if total > Int64.MaxValue - int64 range.Length then
                    invalidOp "Logical pipeline occupancy overflowed."

                total <- total + int64 range.Length

        total

    let reserveInputAsync (length: int) (cancellationToken: CancellationToken) =
        cancellationToken.ThrowIfCancellationRequested()

        if int64 length > logicalCapacity.LimitBytes then
            diagnostics.RecordProgressFailure ProgressFailureCategory.LogicalReceiveExhaustion
            raise (
                ArgumentOutOfRangeException(
                    nameof length,
                    length,
                    "The logical input occurrence exceeds the configured capacity limit."
                )
            )

        match logicalCapacity.TryReserve(int64 length) with
        | ValueSome reservation -> ValueTask<CapacityReservation>(reservation)
        | ValueNone ->
            let pending = pendingLogicalBytes()

            if pending > logicalCapacity.LimitBytes - int64 length then
                diagnostics.RecordProgressFailure ProgressFailureCategory.LogicalReceiveExhaustion
                raise (
                    InvalidOperationException(
                        "Logical capacity cannot admit input while retained pipeline data prevents progress."
                    )
                )

            logicalCapacity.ReserveAsync(int64 length, cancellationToken)

    let disposeAsync (onRuntimeMemoryReleased: unit -> unit) (onFailure: exn -> unit) =
        task {
            if Interlocked.CompareExchange(&busy, 1, 0) <> 0 then
                invalidOp "A pipeline operation is still in progress."

            let mutable failures: ResizeArray<exn> option = None

            try
                if state <> Disposed then
                    transitionToTerminal Disposed

                    try
                        for error in releasePendingWithNotification onFailure do
                            match failures with
                            | Some values -> values.Add error
                            | None ->
                                let values = ResizeArray<exn>()
                                values.Add error
                                failures <- Some values
                    with error ->
                        onFailure error

                        match failures with
                        | Some values -> values.Add error
                        | None ->
                            let values = ResizeArray<exn>()
                            values.Add error
                            failures <- Some values

                    // Directional physical credit can be handed to the
                    // surviving TCP direction once every runtime-owned range
                    // has completed its logical and physical cleanup attempt.
                    // Processor disposal is external cleanup and may suspend;
                    // it must not unnecessarily retain an otherwise free
                    // directional share.
                    try
                        onRuntimeMemoryReleased ()
                    with error ->
                        onFailure error

                        match failures with
                        | Some values -> values.Add error
                        | None ->
                            let values = ResizeArray<exn>()
                            values.Add error
                            failures <- Some values

                    let! processorFailures =
                        ProcessorLifetime.disposeAllAsyncWithNotification processors onFailure
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

            let mutable cleanupFailures: ResizeArray<exn> option = None

            let addCleanupFailures failures =
                cleanupFailures <- collectFailures cleanupFailures failures

            let logicalFailures = PipelineRanges.releaseLogicalRanges ranges
            addCleanupFailures logicalFailures

            // A sink failure abandons every pending stage range as well as the
            // output currently at the sink. Release all abandoned logical
            // credit before disposing either collection's physical owners.
            if operationError.IsSome || logicalFailures.Length > 0 then
                addCleanupFailures (prepareTerminalLogicalCleanup ())

            let physicalFailures = PipelineRanges.releasePhysicalRanges ranges
            addCleanupFailures physicalFailures

            // A physical disposal failure discovered after an otherwise
            // successful write makes the pipeline terminal. The remaining
            // pending logical ranges are released before that failure leaves
            // this operation.
            if operationError.IsNone
               && logicalFailures.Length = 0
               && physicalFailures.Length > 0 then
                addCleanupFailures (prepareTerminalLogicalCleanup ())

            let cleanupFailures =
                match cleanupFailures with
                | Some values -> values.ToArray()
                | None -> Array.empty

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
                            new InvocationOutput(
                                input,
                                stage.Pending,
                                pool,
                                memoryAccounting,
                                logicalCapacity,
                                diagnostics
                            )

                        let mutable invocationError: exn option = None

                        try
                            metrics.BeginInvocation(index)
                            invocationStarted <- metrics.DiagnosticsEnabled

                            if invocationStarted then
                                invocationStartedAt <- Stopwatch.GetTimestamp()

                            let! result = stage.Processor.ProcessAsync(input, output, cancellationToken).AsTask()

                            if invocationStarted then
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

                        let mutable invocationCleanupFailures: ResizeArray<exn> option = None

                        let addInvocationCleanupFailures failures =
                            invocationCleanupFailures <- collectFailures invocationCleanupFailures failures

                        match invocationError with
                        | Some _ ->
                            // The invocation has become terminal. Release
                            // pending input and staged logical output before
                            // output disposal can block on a physical owner.
                            addInvocationCleanupFailures (prepareTerminalLogicalCleanup ())
                            addInvocationCleanupFailures (output.ReleaseLogical ())
                        | None -> ()

                        let outputCleanupError =
                            try
                                (output :> IDisposable).Dispose()
                                None
                            with error ->
                                Some error

                        let invocationCleanupFailures =
                            match invocationCleanupFailures with
                            | Some values -> values.ToArray()
                            | None -> Array.empty

                        let outputCleanupFailures =
                            match outputCleanupError with
                            | Some error -> Array.singleton error
                            | None -> Array.empty

                        let allCleanupFailures =
                            Array.append invocationCleanupFailures outputCleanupFailures

                        match invocationError, allCleanupFailures.Length with
                        | None, 0 -> ()
                        | Some error, 0 -> raisePreserving error
                        | None, 1 -> raisePreserving allCleanupFailures[0]
                        | None, _ -> PipelineRanges.raiseFailures allCleanupFailures
                        | Some error, _ ->
                            raise (
                                AggregateException(
                                    "Processor invocation and output cleanup failed.",
                                    Array.append [| error |] allCleanupFailures
                                )
                            )
                    with error ->
                        let failures = ResizeArray<exn>()

                        // Cover failures that occur before an InvocationOutput
                        // can perform its own logical rollback, and make the
                        // outer abort path idempotent for already-prepared
                        // terminal cleanup.
                        failures.AddRange(prepareTerminalLogicalCleanup ())

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
            new ConnectionMemoryAccounting(),
            new CapacityBudget(Int64.MaxValue),
            new ProcessorPipelineMetrics(processors.Length)
        )

    new(processors, sink, pool, memoryAccounting) =
        new ProcessorPipeline(
            processors,
            sink,
            pool,
            memoryAccounting,
            new CapacityBudget(Int64.MaxValue),
            new ProcessorPipelineMetrics(processors.Length)
        )

    new(processors, sink, pool, memoryAccounting, metrics: ProcessorPipelineMetrics) =
        new ProcessorPipeline(
            processors,
            sink,
            pool,
            memoryAccounting,
            new CapacityBudget(Int64.MaxValue),
            metrics
        )

    new(processors, sink, pool, memoryAccounting, logicalCapacity) =
        new ProcessorPipeline(
            processors,
            sink,
            pool,
            memoryAccounting,
            logicalCapacity,
            new ProcessorPipelineMetrics(processors.Length)
        )

    member _.GetStageMetricsSnapshot() =
        (metrics.GetSnapshot()).Stages

    member _.GetConnectionMemorySnapshot() = memoryAccounting.GetSnapshot()

    member _.GetLogicalCapacitySnapshot() = logicalCapacity.GetSnapshot()

    member internal _.DisposeAsync(onRuntimeMemoryReleased: unit -> unit) =
        if obj.ReferenceEquals(onRuntimeMemoryReleased, null) then
            nullArg (nameof onRuntimeMemoryReleased)

        ValueTask(disposeAsync onRuntimeMemoryReleased ignore)

    member internal _.DisposeAsync(onRuntimeMemoryReleased: unit -> unit, onFailure: exn -> unit) =
        if obj.ReferenceEquals(onRuntimeMemoryReleased, null) then
            nullArg (nameof onRuntimeMemoryReleased)

        if obj.ReferenceEquals(onFailure, null) then
            nullArg (nameof onFailure)

        ValueTask(disposeAsync onRuntimeMemoryReleased onFailure)

    member _.PushAsync(owner: IMemoryOwner<byte>, length: int, cancellationToken: CancellationToken) =
        if obj.ReferenceEquals(owner, null) then
            nullArg (nameof owner)

        task {
            if length < 0 || length > owner.Memory.Length then
                invalidArg (nameof length) "The input length must fit within the owned memory."

            let! trackedOwner = memoryAccounting.TrackAsync(owner, cancellationToken).AsTask()
            let mutable inputTransferred = false
            let mutable inputReservation: CapacityReservation option = None
            let mutable entered = false
            let mutable operationError: exn option = None

            try
                try
                    cancellationToken.ThrowIfCancellationRequested()
                    enter ()
                    entered <- true

                    try
                        if length = 0 then
                            inputTransferred <- true
                            trackedOwner.Dispose()
                        else
                            let! reservation = reserveInputAsync length cancellationToken
                            inputReservation <- Some reservation

                            if stages.Length = 0 then
                                inputTransferred <- true

                                try
                                    let bytes =
                                        Memory<byte>.op_Implicit(trackedOwner.Memory.Slice(0, length))

                                    do! sink.WriteAsync(ReadOnlySequence<byte> bytes, cancellationToken).AsTask()
                                    cancellationToken.ThrowIfCancellationRequested()
                                finally
                                    let cleanupFailures = ResizeArray<exn>()

                                    match inputReservation with
                                    | Some current ->
                                        inputReservation <- None
                                        try
                                            (current :> IDisposable).Dispose()
                                        with error ->
                                            cleanupFailures.Add error
                                    | None -> ()

                                    try
                                        trackedOwner.Dispose()
                                    with error ->
                                        cleanupFailures.Add error

                                    if cleanupFailures.Count > 0 then
                                        PipelineRanges.raiseFailures (cleanupFailures.ToArray())
                            else
                                let segment = SegmentOwner trackedOwner
                                inputTransferred <- true

                                try
                                    stages[0].Pending.Add
                                        { Owner = segment
                                          Logical = reservation
                                          Offset = 0
                                          Length = length }

                                    inputReservation <- None
                                with error ->
                                    let failures = ResizeArray<exn>()

                                    match inputReservation with
                                    | Some current ->
                                        inputReservation <- None

                                        try
                                            current.Release()
                                        with cleanupError ->
                                            failures.Add cleanupError
                                    | None -> ()

                                    try
                                        segment.Release()
                                    with cleanupError ->
                                        failures.Add cleanupError

                                    if failures.Count = 0 then
                                        ExceptionDispatchInfo.Capture(error).Throw()
                                    else
                                        raise (
                                            AggregateException(
                                                "Processor input cleanup failed.",
                                                Array.append [| error |] (failures.ToArray())
                                            )
                                        )

                                metrics.AcceptInput(0, int64 length)
                                do! processStages false cancellationToken
                    with error ->
                        let cleanupFailures = abort ()

                        operationError <-
                            if cleanupFailures.Length = 0 then
                                Some error
                            else
                                Some(
                                    AggregateException(
                                        "Processor operation and cleanup failed.",
                                        Array.append [| error |] cleanupFailures
                                    )
                                )
                with error ->
                    // Cancellation or a failed non-reentrant admission occurs
                    // before the pipeline owns the input. Preserve it while
                    // still allowing the final input cleanup pass below.
                    operationError <- Some error
            finally
                if entered then
                    Volatile.Write(&busy, 0)

            let inputCleanupFailures = ResizeArray<exn>()

            match inputReservation with
            | Some reservation ->
                inputReservation <- None

                try
                    (reservation :> IDisposable).Dispose()
                with error ->
                    inputCleanupFailures.Add error
            | None -> ()

            if not inputTransferred then
                try
                    trackedOwner.Dispose()
                with error ->
                    inputCleanupFailures.Add error

            match operationError, inputCleanupFailures.Count with
            | None, 0 -> ()
            | Some error, 0 -> raisePreserving error
            | None, _ -> PipelineRanges.raiseFailures (inputCleanupFailures.ToArray())
            | Some error, _ ->
                raise (
                    AggregateException(
                        "Processor operation and input cleanup both failed.",
                        Array.append [| error |] (inputCleanupFailures.ToArray())
                    )
                )
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
            (disposeAsync ignore ignore).GetAwaiter().GetResult()

    interface IAsyncDisposable with
        member _.DisposeAsync() = ValueTask(disposeAsync ignore ignore)
