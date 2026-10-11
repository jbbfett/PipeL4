namespace PipeL4.Benchmarks

open BenchmarkDotNet.Columns
open BenchmarkDotNet.Configs
open BenchmarkDotNet.Running

module Program =
    [<EntryPoint>]
    let main args =
        let configuration =
            ManualConfig.Create(DefaultConfig.Instance).AddColumn(
                [| LogicalThroughputColumn() :> IColumn |]
            )

        BenchmarkSwitcher
            .FromAssembly(typeof<RawTcpForwardingBenchmarks>.Assembly)
            .Run(args, configuration)
        |> ignore

        0
