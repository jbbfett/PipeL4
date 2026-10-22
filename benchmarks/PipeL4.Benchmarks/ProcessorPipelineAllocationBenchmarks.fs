namespace PipeL4.Benchmarks

open System
open System.Buffers
open System.Threading
open System.Threading.Tasks
open BenchmarkDotNet.Attributes
open PipeL4.Core

type private AllocationSink() =
    interface IProcessorPipelineSink with
        member _.WriteAsync(_, _) = ValueTask()
        member _.CompleteAsync _ = ValueTask()

type private AllocationProcessor() =
    interface IByteStreamProcessor with
        member _.ProcessAsync(input, output, _) =
            output.AppendInput(0L, input.Bytes.Length)
            ValueTask<ProcessorResult>({ RetainedLength = 0L })

[<MemoryDiagnoser>]
type ProcessorPipelineAllocationBenchmarks() =
    let mutable pipeline = Unchecked.defaultof<ProcessorPipeline>

    [<Params(0, 1, 4, 8)>]
    member val StageCount = 0 with get, set

    [<GlobalSetup>]
    member this.Setup() =
        let processors =
            Array.init this.StageCount (fun _ -> AllocationProcessor() :> IByteStreamProcessor)

        pipeline <- new ProcessorPipeline(processors, AllocationSink(), MemoryPool<byte>.Shared)

    [<GlobalCleanup>]
    member _.Cleanup() =
        if not (obj.ReferenceEquals(pipeline, null)) then
            (pipeline :> IDisposable).Dispose()

    [<Benchmark>]
    member _.Push4KiB() =
        let owner = MemoryPool<byte>.Shared.Rent(4096)
        pipeline.PushAsync(owner, 4096, CancellationToken.None)
