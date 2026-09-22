namespace PipeL4.Benchmarks

open BenchmarkDotNet.Attributes
open BenchmarkDotNet.Running

type SmokeBenchmark() =
    [<Benchmark>]
    member _.ReturnConstant() = 42

module Program =
    [<EntryPoint>]
    let main args =
        BenchmarkSwitcher.FromAssembly(typeof<SmokeBenchmark>.Assembly).Run(args) |> ignore
        0
