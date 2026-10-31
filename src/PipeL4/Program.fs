module PipeL4.Program

open System
open System.Threading
open Microsoft.Extensions.Logging
open PipeL4.Core
open PipeL4.Plugins

let private usage =
    "Usage: PipeL4 --listen-address <ip> --listen-port <port> --backend-host <ip> --backend-port <port> [--client-to-server-plugin <path>]... [--server-to-client-plugin <path>]... [--client-to-server-logical-capacity-bytes <bytes>] [--server-to-client-logical-capacity-bytes <bytes>] [--connection-physical-capacity-bytes <bytes>] [--disable-runtime-diagnostics]"

[<EntryPoint>]
let main arguments =
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
