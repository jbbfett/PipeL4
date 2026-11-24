namespace PipeL4.Benchmarks

open System
open System.Buffers
open System.Threading
open System.Threading.Tasks
open BenchmarkDotNet.Attributes
open PipeL4.Core
open PipeL4.PluginHost
open PipeL4.Plugins

type private FunctionalAdapterSink() =
    interface IProcessorPipelineSink with
        member _.WriteAsync(_, _) = ValueTask()
        member _.CompleteAsync _ = ValueTask()

type private AdapterClassProcessor(generated: bool) =
    interface PipeL4.Plugins.IByteStreamProcessor with
        member _.ProcessAsync(input, output, _) =
            if generated then
                for segment in input.Bytes do
                    segment.Span.CopyTo(output.GetSpan(segment.Length))
                    output.Advance(segment.Length)
            else
                output.AppendInput(0L, input.Bytes.Length)

            ValueTask<PipeL4.Plugins.ProcessorResult>(releaseAll)

/// Matched public class/functional adapters through the production executor, without sockets.
[<MemoryDiagnoser>]
type FunctionalProcessorAdapterBenchmarks() =
    let mutable classPipeline = Unchecked.defaultof<ProcessorPipeline>
    let mutable functionalPipeline = Unchecked.defaultof<ProcessorPipeline>
    let state = new ConnectionState()

    [<Params(false, true)>]
    member val Generated = false with get, set

    [<GlobalSetup>]
    member this.Setup() =
        let publicState = ConnectionStateAdapter(state) :> IConnectionState
        let handle ctx =
            if this.Generated then
                for segment in ctx.Input do
                    emitBytes ctx segment.Span
            else
                emitInput ctx 0L ctx.Input.Length
            releaseAll

        classPipeline <-
            new ProcessorPipeline(
                [| ScriptProcessorAdapter(AdapterClassProcessor(this.Generated)) |],
                FunctionalAdapterSink(),
                MemoryPool<byte>.Shared
            )
        functionalPipeline <-
            new ProcessorPipeline(
                [| FunctionalProcessorAdapter(handle, publicState) |],
                FunctionalAdapterSink(),
                MemoryPool<byte>.Shared
            )

        for name, pipeline in [ "class", classPipeline; "functional", functionalPipeline ] do
            let owner = MemoryPool<byte>.Shared.Rent(4096)
            pipeline.PushAsync(owner, 4096, CancellationToken.None).GetAwaiter().GetResult()
            let metrics = pipeline.GetStageMetricsSnapshot() |> List.exactlyOne
            let logical = pipeline.GetLogicalCapacitySnapshot()
            let physical = pipeline.GetConnectionMemorySnapshot()
            let copied = if this.Generated then 4096L else 0L
            if metrics.GeneratedOutputBytes <> copied
               || metrics.InputBackedOutputBytes <> 4096L - copied
               || logical.CurrentBytes <> 0L
               || physical.CurrentPhysicalBufferCapacityBytes <> 0L then
                invalidOp "Adapter preflight output accounting or cleanup failed."
            printfn
                "%s preflight: copied=%d B, copy ratio=%.1f, logical peak=%d B, physical peak=%d B"
                name copied (float copied / 4096.0) logical.PeakBytes physical.PeakPhysicalBufferCapacityBytes

    [<GlobalCleanup>]
    member _.Cleanup() =
        if not (obj.ReferenceEquals(classPipeline, null)) then
            (classPipeline :> IDisposable).Dispose()
        if not (obj.ReferenceEquals(functionalPipeline, null)) then
            (functionalPipeline :> IDisposable).Dispose()
        (state :> IAsyncDisposable).DisposeAsync().GetAwaiter().GetResult()

    [<Benchmark(Baseline = true)>]
    member _.Class4KiB() =
        let owner = MemoryPool<byte>.Shared.Rent(4096)
        classPipeline.PushAsync(owner, 4096, CancellationToken.None)

    [<Benchmark>]
    member _.Functional4KiB() =
        let owner = MemoryPool<byte>.Shared.Rent(4096)
        functionalPipeline.PushAsync(owner, 4096, CancellationToken.None)
