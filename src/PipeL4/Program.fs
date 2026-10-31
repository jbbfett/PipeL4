module PipeL4.Program

open System
open System.Threading
open Microsoft.Extensions.Logging
open PipeL4.Core
open PipeL4.Plugins

let private usage =
    "Usage: PipeL4 --listen-address <ip> --listen-port <port> --backend-host <ip> --backend-port <port> [--client-to-server-plugin <path>]... [--server-to-client-plugin <path>]... [--client-to-server-logical-capacity-bytes <bytes>] [--server-to-client-logical-capacity-bytes <bytes>] [--connection-physical-capacity-bytes <bytes>] [--disable-runtime-diagnostics]"

let private help =
    String.concat
        Environment.NewLine
        [ "Usage: PipeL4 [options]"
          ""
          "Required endpoints:"
          "  --listen-address <ip>      Local address to listen on (IPv4 or IPv6 literal)."
          "  --listen-port <port>       TCP port to listen on (1 through 65535)."
          "  --backend-host <ip>        Backend address (IPv4 or IPv6 literal)."
          "  --backend-port <port>      Backend TCP port (1 through 65535)."
          "  Address options accept IP addresses only; hostnames are not resolved."
          ""
          "Directional plugins:"
          "  --client-to-server-plugin <path>  Add a stage for client-to-backend traffic."
          "  --server-to-client-plugin <path>  Add a stage for backend-to-client traffic."
          "  Repeat either option to add stages; each direction runs them in argument order."
          "  Plugin scripts are compiled before the listener starts."
          ""
          "Byte capacities:"
          "  --client-to-server-logical-capacity-bytes <bytes>  Minimum 1; default unbounded."
          "  --server-to-client-logical-capacity-bytes <bytes>  Minimum 1; default unbounded."
          "  --connection-physical-capacity-bytes <bytes>       Minimum 2; default unbounded."
          "    The physical capacity is shared by both directions within each connection."
          ""
          "Diagnostics:"
          "  Runtime diagnostics are enabled by default."
          "  --disable-runtime-diagnostics  Omit optional counters, timings, progress categories,"
          "                                 and terminal runtime-diagnostics summaries."
          ""
          "Invocation example (run from the repository root with a backend on port 6000):"
          "  dotnet run --project src/PipeL4 -- --listen-address 127.0.0.1 --listen-port 5000 --backend-host 127.0.0.1 --backend-port 6000 --client-to-server-plugin ./benchmarks/PipeL4.Benchmarks/Scripts/PassThrough.fsx"
          ""
          "Help:"
          "  -h, --help  Show this help. Use either option by itself." ]

[<EntryPoint>]
let main arguments =
    match arguments with
    | [| "--help" |]
    | [| "-h" |] ->
        printfn "%s" help
        0
    | _ when arguments |> Array.exists (fun argument -> argument = "--help" || argument = "-h") ->
        eprintfn "Error: Help options '--help' and '-h' must be used alone."
        eprintfn "%s" usage
        2
    | _ ->
        match ForwardingConfiguration.tryParseArguments arguments with
        | Error error ->
            eprintfn "Error: %s" error
            eprintfn "%s" usage
            2
        | Ok configuration ->
            use shutdown = new CancellationTokenSource()

            Console.CancelKeyPress.Add(fun eventArguments ->
                eventArguments.Cancel <- true
                shutdown.Cancel())

            use loggerFactory =
                LoggerFactory.Create(fun builder ->
                    builder
                        .SetMinimumLevel(LogLevel.Information)
                        .AddJsonConsole()
                    |> ignore)

            let logger = loggerFactory.CreateLogger("PipeL4.Core.TcpForwarder")

            try
                ScriptTcpForwarder.runAsync configuration logger shutdown.Token
                |> fun runtime -> runtime.GetAwaiter().GetResult()

                0
            with
            | :? ScriptPluginConfigurationException as error ->
                eprintfn "Plugin configuration error: %s" error.Message
                2
            | :? OperationCanceledException when shutdown.IsCancellationRequested ->
                0
            | error ->
                eprintfn "Runtime error: %s" error.Message
                1
