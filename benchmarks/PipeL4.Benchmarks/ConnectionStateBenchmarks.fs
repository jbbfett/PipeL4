namespace PipeL4.Benchmarks

open System
open System.Threading.Tasks
open BenchmarkDotNet.Attributes
open PipeL4.Core

[<MemoryDiagnoser>]
type ConnectionStateBenchmarks() =
    let state = ConnectionState()
    let factory = fun () -> obj()

    [<GlobalSetup>]
    member _.Setup() =
        state.GetOrAdd("published", factory) |> ignore

    [<GlobalCleanup>]
    member _.Cleanup() =
        (state :> IAsyncDisposable).DisposeAsync().AsTask().GetAwaiter().GetResult()

    [<Benchmark(Baseline = true)>]
    member _.GetOrAddPublished() = state.GetOrAdd("published", factory)

    [<Benchmark>]
    member _.TryGetPublished() = state.TryGet<obj>("published")

type private CompletedAsyncDisposable() =
    interface IAsyncDisposable with
        member _.DisposeAsync() = ValueTask()

[<MemoryDiagnoser>]
type ConnectionStateLifecycleBenchmarks() =
    let keys = [| "first"; "second"; "third"; "fourth" |]
    let values = Array.init keys.Length (fun _ -> obj())
    let factories = values |> Array.map (fun value -> fun () -> value)
    let ownedValueFactory = fun () -> CompletedAsyncDisposable()

    let dispose (state: ConnectionState) =
        (state :> IAsyncDisposable).DisposeAsync().AsTask().GetAwaiter().GetResult()

    [<Benchmark(Baseline = true)>]
    member _.CreateRegistry() : obj = ConnectionState()

    [<Benchmark>]
    member _.CreateAndPublishFirstValue() : obj =
        let state = ConnectionState()
        state.GetOrAdd(keys[0], factories[0]) |> ignore
        state

    [<Benchmark>]
    member _.CreateAndPublishFourValues() : obj =
        let state = ConnectionState()

        for index in 0 .. keys.Length - 1 do
            state.GetOrAdd(keys[index], factories[index]) |> ignore

        state

    [<Benchmark>]
    member _.CreateAndDisposeEmptyRegistry() =
        let state = ConnectionState()
        dispose state

    [<Benchmark>]
    member _.CreatePublishFourOwnedValuesAndDispose() =
        let state = ConnectionState()

        for key in keys do
            state.GetOrAdd(key, ownedValueFactory) |> ignore

        dispose state
