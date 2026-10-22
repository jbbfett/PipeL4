namespace PipeL4.Benchmarks

open System
open System.Buffers
open System.Threading.Tasks
open BenchmarkDotNet.Attributes
open PipeL4.Core

type private SnapshotSink() =
    interface IProcessorPipelineSink with
        member _.WriteAsync(_, _) = ValueTask()
        member _.CompleteAsync _ = ValueTask()

type private SnapshotProcessor() =
    interface IByteStreamProcessor with
        member _.ProcessAsync(_, _, _) =
            ValueTask<ProcessorResult>({ RetainedLength = 0L })

[<MemoryDiagnoser>]
type SnapshotBenchmarks() =
    let mutable pipeline = Unchecked.defaultof<ProcessorPipeline>
    let mutable directionMetrics = Unchecked.defaultof<ProcessorPipelineMetrics>
    let mutable connectionMetrics = Unchecked.defaultof<ConnectionRuntimeMetrics>
    let mutable physicalLease = Unchecked.defaultof<IDisposable>

    let seedMetrics (metrics: ProcessorPipelineMetrics) =
        for index in 0 .. metrics.StageCount - 1 do
            metrics.AcceptInput(index, 1024L)
            metrics.BeginInvocation index
            metrics.ValidateSuccessful(index, 1024L, 0L, 100L)
            metrics.CommitSuccessful(index, 1024L, 0L, 100L, 0L)

    [<Params(1, 4, 8)>]
    member val StageCount = 0 with get, set

    [<GlobalSetup>]
    member this.Setup() =
        let accounting = ConnectionMemoryAccounting()
        let stageMetrics = ProcessorPipelineMetrics(this.StageCount)
        let processors = Array.init this.StageCount (fun _ -> SnapshotProcessor() :> IByteStreamProcessor)

        pipeline <-
            new ProcessorPipeline(
                processors,
                SnapshotSink(),
                MemoryPool<byte>.Shared,
                accounting,
                stageMetrics
            )

        seedMetrics stageMetrics

        directionMetrics <- ProcessorPipelineMetrics(this.StageCount)
        seedMetrics directionMetrics

        let backendMetrics = ProcessorPipelineMetrics(this.StageCount)
        seedMetrics backendMetrics

        connectionMetrics <- ConnectionRuntimeMetrics(accounting)
        connectionMetrics.Publish(ClientToBackend, directionMetrics)
        connectionMetrics.Publish(BackendToClient, backendMetrics)
        physicalLease <- accounting.TrackArrayCapacity(4096L)

    [<GlobalCleanup>]
    member _.Cleanup() =
        if not (obj.ReferenceEquals(pipeline, null)) then
            (pipeline :> IDisposable).Dispose()

        if not (obj.ReferenceEquals(physicalLease, null)) then
            physicalLease.Dispose()

    [<Benchmark(Baseline = true)>]
    member _.StageSnapshot() =
        pipeline.GetStageMetricsSnapshot()
        |> List.sumBy (fun snapshot -> snapshot.InputBytes)

    [<Benchmark>]
    member _.DirectionSnapshot() =
        let snapshot = directionMetrics.GetSnapshot()
        snapshot.InputBytes + snapshot.InvocationCount

    [<Benchmark>]
    member _.ConnectionSnapshot() =
        let snapshot = connectionMetrics.GetSnapshot()
        snapshot.ClientToBackend.InputBytes
        + snapshot.BackendToClient.InputBytes
        + snapshot.CurrentPhysicalBufferCapacityBytes
