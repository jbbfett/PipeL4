namespace PipeL4.Benchmarks

open System
open System.Buffers
open System.Threading
open System.Threading.Tasks
open BenchmarkDotNet.Attributes
open PipeL4.Core

type private SlowDownstreamSink() =
    let mutable started = Unchecked.defaultof<TaskCompletionSource<unit>>
    let mutable release = Unchecked.defaultof<TaskCompletionSource<unit>>
    let mutable output = Array.empty<byte>

    member _.BeginWrite() =
        started <- new TaskCompletionSource<unit>(TaskCreationOptions.RunContinuationsAsynchronously)
        release <- new TaskCompletionSource<unit>(TaskCreationOptions.RunContinuationsAsynchronously)
        output <- Array.empty

    member _.WaitForWriteAsync() = started.Task

    member _.Release() = release.TrySetResult() |> ignore

    member _.Output = output

    member _.IsStarted = not (obj.ReferenceEquals(started, null)) && started.Task.IsCompleted

    interface IProcessorPipelineSink with
        member _.WriteAsync(bytes, _) =
            ValueTask(
                task {
                    let copy = Array.zeroCreate<byte> (int bytes.Length)
                    let mutable offset = 0

                    let mutable position = bytes.Start
                    let mutable segment = ReadOnlyMemory<byte>.Empty

                    while bytes.TryGet(&position, &segment) do
                        segment.Span.CopyTo(copy.AsSpan(offset))
                        offset <- offset + segment.Length

                    output <- copy
                    started.TrySetResult() |> ignore
                    do! release.Task
                }
            )

        member _.CompleteAsync _ = ValueTask()

type private SlowDownstreamProcessor() =
    interface IByteStreamProcessor with
        member _.ProcessAsync(input, output, _) =
            output.AppendInput(0L, input.Bytes.Length)
            ValueTask<ProcessorResult>({ RetainedLength = 0L })

[<Struct; NoEquality; NoComparison>]
type private PressureObservation =
    { OperationWasSuspended: bool
      SinkWasStarted: bool
      Logical: CapacityUsageSnapshot
      LogicalBudget: CapacityUsageSnapshot
      Physical: ConnectionMemorySnapshot
      PhysicalBudget: CapacityUsageSnapshot }

[<MemoryDiagnoser>]
type BackpressurePressureBenchmarks() =
    let mutable pipeline = Unchecked.defaultof<ProcessorPipeline>
    let mutable memoryAccounting = Unchecked.defaultof<ConnectionMemoryAccounting>
    let mutable physicalBudget = Unchecked.defaultof<CapacityBudget>
    let mutable logicalCapacity = Unchecked.defaultof<CapacityBudget>
    let mutable sink = Unchecked.defaultof<SlowDownstreamSink>
    let mutable payload = Array.empty<byte>
    let mutable observation: PressureObservation option = None
    let physicalCapacityLimit = 4L * 1024L * 1024L

    [<Params(4096, 1048576)>]
    member val PayloadSize = 0 with get, set

    [<GlobalSetup>]
    member this.Setup() =
        payload <- Array.init this.PayloadSize (fun index -> byte (index % 251))
        sink <- new SlowDownstreamSink()

        physicalBudget <- new CapacityBudget(physicalCapacityLimit)
        memoryAccounting <- new ConnectionMemoryAccounting(physicalBudget)
        logicalCapacity <- new CapacityBudget(Int64.MaxValue)

        pipeline <-
            new ProcessorPipeline(
                [| new SlowDownstreamProcessor() :> IByteStreamProcessor |],
                sink,
                MemoryPool<byte>.Shared,
                memoryAccounting,
                logicalCapacity
            )

    [<GlobalCleanup>]
    member _.Cleanup() =
        if not (obj.ReferenceEquals(pipeline, null)) then
            (pipeline :> IDisposable).Dispose()

        if not (obj.ReferenceEquals(memoryAccounting, null)) then
            memoryAccounting.Dispose()

        if not (obj.ReferenceEquals(physicalBudget, null)) then
            let snapshot = physicalBudget.GetSnapshot()

            if snapshot.CurrentBytes <> 0L then
                invalidOp "The pressure benchmark left physical capacity reserved after cleanup."

            physicalBudget.Dispose()

        if not (obj.ReferenceEquals(logicalCapacity, null)) then
            let snapshot = logicalCapacity.GetSnapshot()

            if snapshot.CurrentBytes <> 0L then
                invalidOp "The pressure benchmark left logical capacity reserved after cleanup."

            logicalCapacity.Dispose()

    [<Benchmark(Baseline = true)>]
    member this.SlowDownstreamFlowControl() =
        task {
            sink.BeginWrite()
            let owner = MemoryPool<byte>.Shared.Rent(this.PayloadSize)
            payload.AsSpan().CopyTo(owner.Memory.Span)
            let operation = pipeline.PushAsync(owner, this.PayloadSize, CancellationToken.None)

            do! sink.WaitForWriteAsync()

            let logical = pipeline.GetLogicalCapacitySnapshot()
            let logicalBudgetSnapshot = logicalCapacity.GetSnapshot()
            let physical = pipeline.GetConnectionMemorySnapshot()
            let physicalBudgetSnapshot = physicalBudget.GetSnapshot()

            observation <-
                Some
                    { OperationWasSuspended = not operation.IsCompleted
                      SinkWasStarted = sink.IsStarted
                      Logical = logical
                      LogicalBudget = logicalBudgetSnapshot
                      Physical = physical
                      PhysicalBudget = physicalBudgetSnapshot }

            sink.Release()
            do! operation
        }

    [<IterationCleanup>]
    member _.ValidateIteration() =
        match observation with
        | None -> invalidOp "The pressure benchmark did not capture a gated observation."
        | Some captured ->
            if not captured.OperationWasSuspended || not captured.SinkWasStarted then
                invalidOp "The pressure benchmark did not suspend at the downstream sink."

            if captured.Logical.CurrentBytes <= 0L
               || captured.Logical.PeakBytes <= 0L
               || captured.Logical.PeakBytes > captured.Logical.LimitBytes then
                invalidOp "The pressure benchmark lost live logical occupancy while gated."

            if captured.Logical.LimitBytes <> captured.LogicalBudget.LimitBytes
               || captured.Logical.CurrentBytes <> captured.LogicalBudget.CurrentBytes
               || captured.Logical.PeakBytes <> captured.LogicalBudget.PeakBytes then
                invalidOp "The pressure benchmark logical budget and pipeline accounting disagree."

            if captured.Physical.CurrentPhysicalBufferCapacityBytes <= 0L
               || captured.Physical.PeakPhysicalBufferCapacityBytes <= 0L then
                invalidOp "The pressure benchmark observed no live physical ownership while gated."

            if captured.Physical.CurrentPhysicalBufferCapacityBytes <> captured.PhysicalBudget.CurrentBytes
               || captured.Physical.PeakPhysicalBufferCapacityBytes <> captured.PhysicalBudget.PeakBytes then
                invalidOp "The pressure benchmark physical budget and pipeline accounting disagree."

            if captured.PhysicalBudget.LimitBytes <> physicalCapacityLimit
               || captured.PhysicalBudget.CurrentBytes <= 0L
               || captured.PhysicalBudget.PeakBytes <= 0L
               || captured.PhysicalBudget.CurrentBytes > physicalCapacityLimit
               || captured.PhysicalBudget.PeakBytes > physicalCapacityLimit then
                invalidOp "The pressure benchmark violated its finite physical capacity guard."

            if not (payload.AsSpan().SequenceEqual(sink.Output.AsSpan())) then
                invalidOp "The pressure benchmark changed byte content or ordering."

            let completedLogical = pipeline.GetLogicalCapacitySnapshot()
            let completedLogicalBudget = logicalCapacity.GetSnapshot()
            let completedPhysical = pipeline.GetConnectionMemorySnapshot()
            let completedPhysicalBudget = physicalBudget.GetSnapshot()

            if completedLogical.CurrentBytes <> 0L
               || completedLogical.CurrentBytes <> completedLogicalBudget.CurrentBytes
               || completedPhysical.CurrentPhysicalBufferCapacityBytes <> 0L
               || completedPhysical.CurrentPhysicalBufferCapacityBytes <> completedPhysicalBudget.CurrentBytes
               || completedPhysicalBudget.CurrentBytes <> 0L then
                invalidOp "The pressure benchmark left capacity reserved after completion."

            observation <- None
