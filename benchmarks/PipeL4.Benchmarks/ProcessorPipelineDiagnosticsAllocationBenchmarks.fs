namespace PipeL4.Benchmarks

open System
open System.Buffers
open System.Threading
open System.Threading.Tasks
open BenchmarkDotNet.Attributes
open PipeL4.Core

type private DiagnosticsAllocationSink() =
    let mutable writeCount = 0L
    let mutable writtenBytes = 0L

    member _.WriteCount = writeCount
    member _.WrittenBytes = writtenBytes

    interface IProcessorPipelineSink with
        member _.WriteAsync(bytes, _) =
            writeCount <- writeCount + 1L
            writtenBytes <- writtenBytes + bytes.Length
            ValueTask()

        member _.CompleteAsync _ = ValueTask()

type private DiagnosticsAllocationProcessor() =
    interface IByteStreamProcessor with
        member _.ProcessAsync(input, output, _) =
            output.AppendInput(0L, input.Bytes.Length)
            ValueTask<ProcessorResult>({ RetainedLength = 0L })

[<MemoryDiagnoser>]
type ProcessorPipelineDiagnosticsAllocationBenchmarks() =
    let payloadSize = 4096
    let mutable pipeline = Unchecked.defaultof<ProcessorPipeline>
    let mutable sink = Unchecked.defaultof<DiagnosticsAllocationSink>
    let mutable accounting = Unchecked.defaultof<ConnectionMemoryAccounting>
    let mutable logicalCapacity = Unchecked.defaultof<CapacityBudget>
    let mutable metrics = Unchecked.defaultof<ProcessorPipelineMetrics>
    let mutable payload = Array.empty<byte>

    let pushOnce () =
        let owner = MemoryPool<byte>.Shared.Rent(payloadSize)
        payload.AsSpan().CopyTo(owner.Memory.Span)
        let operation = pipeline.PushAsync(owner, payloadSize, CancellationToken.None)

        operation.GetAwaiter().GetResult()

    let verifySynchronousPush () =
        let owner = MemoryPool<byte>.Shared.Rent(payloadSize)
        payload.AsSpan().CopyTo(owner.Memory.Span)
        let operation = pipeline.PushAsync(owner, payloadSize, CancellationToken.None)

        if not operation.IsCompleted then
            invalidOp "The diagnostics allocation path did not complete synchronously."

        operation.GetAwaiter().GetResult()

    [<GlobalSetup>]
    member this.Setup() =
        payload <- Array.init payloadSize (fun index -> byte (index % 251))
        sink <- new DiagnosticsAllocationSink()
        accounting <- new ConnectionMemoryAccounting()
        logicalCapacity <- new CapacityBudget(Int64.MaxValue)
        metrics <- new ProcessorPipelineMetrics(1, this.RuntimeDiagnosticsEnabled)

        pipeline <-
            new ProcessorPipeline(
                [| DiagnosticsAllocationProcessor() :> IByteStreamProcessor |],
                sink,
                MemoryPool<byte>.Shared,
                accounting,
                logicalCapacity,
                metrics
            )

        verifySynchronousPush ()

        let stage = pipeline.GetStageMetricsSnapshot() |> List.head

        let expectedInvocationCount = if this.RuntimeDiagnosticsEnabled then 1L else 0L

        if
            stage.InvocationCount <> expectedInvocationCount
            || stage.SuccessfulInvocationCount <> expectedInvocationCount
        then
            invalidOp "The diagnostics allocation preflight did not complete the expected invocation accounting."

    [<Params(false, true)>]
    member val RuntimeDiagnosticsEnabled = false with get, set

    [<GlobalCleanup>]
    member _.Cleanup() =
        if not (obj.ReferenceEquals(sink, null)) then
            if sink.WriteCount <= 0L || sink.WrittenBytes <= 0L then
                invalidOp "The diagnostics allocation sink observed no output."

        if not (obj.ReferenceEquals(pipeline, null)) then
            (pipeline :> IDisposable).Dispose()

        if not (obj.ReferenceEquals(accounting, null)) then
            accounting.Dispose()

    [<Benchmark(Baseline = true)>]
    member _.Push4KiB() =
        pushOnce ()
