module PipeL4.Program

open System
open System.Reflection
open System.Threading
open Microsoft.Extensions.Logging
open PipeL4.Core
open PipeL4.Plugins

let private usage =
    String.concat
        Environment.NewLine
        [ "Usage: pipel4 --listen-address <ip> --listen-port <port> --backend-host <ip> --backend-port <port> [--client-to-server-plugin <path>]... [--server-to-client-plugin <path>]... [--client-to-server-logical-capacity-bytes <bytes>] [--server-to-client-logical-capacity-bytes <bytes>] [--connection-physical-capacity-bytes <bytes>] [--disable-runtime-diagnostics]"
          "       pipel4 examples list | copy <name> [--output-dir <directory>] [--force]"
          "       pipel4 --help | --version" ]

let private applicationVersion () =
    let assembly = Assembly.GetExecutingAssembly()

    let informationalVersion =
        assembly.GetCustomAttribute<AssemblyInformationalVersionAttribute>()
        |> Option.ofObj
        |> Option.map _.InformationalVersion
        |> Option.defaultWith (fun () ->
            assembly.GetName().Version
            |> Option.ofObj
            |> Option.map _.ToString()
            |> Option.defaultValue "unknown")

    let buildMetadataIndex = informationalVersion.IndexOf('+')

    if buildMetadataIndex < 0 then
        informationalVersion
    else
        informationalVersion.Substring(0, buildMetadataIndex)

let private help =
    String.concat
        Environment.NewLine
        [ "Usage: pipel4 [options]"
          ""
          "Required endpoints:"
          "  --listen-address <ip>      Local address to listen on (IPv4 or IPv6 literal)."
          "  --listen-port <port>       TCP port to listen on (1 through 65535)."
          "  --backend-host <ip>        Backend address (IPv4 or IPv6 literal)."
          "  --backend-port <port>      Backend TCP port (1 through 65535)."
          "  Address options accept IP addresses only; hostnames are not resolved."
          "  The four endpoint options are required."
          ""
          "Directional plugins:"
          "  --client-to-server-plugin <path>  Add a stage for client-to-backend traffic."
          "  --server-to-client-plugin <path>  Add a stage for backend-to-client traffic."
          "  Repeat either option to add stages; each direction runs them in argument order."
          "  Each path names an .fsx script, compiled and loaded before the listener starts."
          "  Relative paths start from the current working directory."
          "  Scripts execute as trusted code in this process; there is no sandbox."
          ""
          "Byte capacities:"
          "  Supply capacities as decimal byte counts without suffixes."
          "  --client-to-server-logical-capacity-bytes <bytes>  Minimum 1; default unbounded."
          "  --server-to-client-logical-capacity-bytes <bytes>  Minimum 1; default unbounded."
          "  --connection-physical-capacity-bytes <bytes>       Minimum 2; default unbounded."
          "    The physical capacity is shared by both directions within each connection."
          ""
          "Diagnostics:"
          "  Runtime diagnostics are enabled by default."
          "  --disable-runtime-diagnostics  Takes no value; omit optional counters, timings, progress categories,"
          "                                 and terminal runtime-diagnostics summaries."
          ""
          "Version:"
          "  --version  Show the application version and exit."
          ""
          "Invocation example (run from the repository root):"
          "  pipel4 --listen-address 127.0.0.1 --listen-port 5000 --backend-host 127.0.0.1 --backend-port 6000 --client-to-server-plugin ./examples/AsciiUppercase.fsx"
          ""
          "Help:"
          "  -h, --help  Show this help. Use either option by itself."
          "  pipel4 examples --help  Show bundled-example commands."
          "  Help and version options must each be used alone." ]

[<EntryPoint>]
let main arguments =
    match arguments with
    | _ when arguments.Length > 0 && arguments[0] = "examples" -> ExampleDistribution.run arguments[1..]
    | [| "--help" |]
    | [| "-h" |] ->
        printfn "%s" help
        0
    | [| "--version" |] ->
        printfn "PipeL4 %s" (applicationVersion ())
        0
    | _ when arguments |> Array.exists (fun argument -> argument = "--help" || argument = "-h") ->
        eprintfn "Error: Help options '--help' and '-h' must be used alone."
        eprintfn "%s" usage
        2
    | _ when arguments |> Array.contains "--version" ->
        eprintfn "Error: Version option '--version' must be used alone."
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
