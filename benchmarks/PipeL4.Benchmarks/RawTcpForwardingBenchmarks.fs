namespace PipeL4.Benchmarks

open System
open System.Globalization
open System.IO
open System.Net
open System.Net.Sockets
open System.Threading
open System.Threading.Tasks
open BenchmarkDotNet.Attributes
open BenchmarkDotNet.Columns
open BenchmarkDotNet.Reports
open BenchmarkDotNet.Running
open Microsoft.Extensions.Logging.Abstractions
open PipeL4.Core

type LogicalThroughputColumn() =
    let calculateValue (summary: Summary) (benchmarkCase: BenchmarkCase) =
        let payloadSize =
            benchmarkCase.Parameters.Items
            |> Seq.tryFind (fun parameter -> parameter.Name = "PayloadSize")

        let statistics =
            summary[benchmarkCase]
            |> Option.ofObj
            |> Option.bind (fun report -> report.ResultStatistics |> Option.ofObj)

        match payloadSize, statistics with
        | Some parameter, Some result when result.Mean > 0.0 ->
            let logicalBytes = 2.0 * Convert.ToDouble(parameter.Value, CultureInfo.InvariantCulture)
            let mebibytesPerSecond = logicalBytes * 1_000_000_000.0 / result.Mean / (1024.0 * 1024.0)
            mebibytesPerSecond.ToString("N2", CultureInfo.InvariantCulture)
        | _ -> "?"

    interface IColumn with
        member _.Id = "LogicalThroughput"
        member _.ColumnName = "Logical MiB/s"
        member _.AlwaysShow = true
        member _.Category = ColumnCategory.Metric
        member _.PriorityInCategory = 0
        member _.IsNumeric = true
        member _.UnitType = UnitType.Dimensionless

        member _.Legend =
            "Logical request and response payload bytes transferred per second."

        member _.IsDefault(_, _) = false
        member _.IsAvailable(_) = true

        member _.GetValue(summary, benchmarkCase) =
            calculateValue summary benchmarkCase

        member _.GetValue(summary, benchmarkCase, _) =
            calculateValue summary benchmarkCase

[<MemoryDiagnoser>]
type RawTcpForwardingBenchmarks() =
    let loopback = IPAddress.Loopback
    let mutable backendListener: TcpListener option = None
    let mutable client: TcpClient option = None
    let mutable backend: TcpClient option = None
    let mutable lifetimeCancellation: CancellationTokenSource option = None
    let mutable proxyCompletion: Task = Task.CompletedTask
    let mutable echoCompletion: Task = Task.CompletedTask
    let mutable clientStream = Unchecked.defaultof<NetworkStream>
    let mutable payload = Array.empty<byte>
    let mutable response = Array.empty<byte>

    let endpointPort (listener: TcpListener) =
        (listener.LocalEndpoint :?> IPEndPoint).Port

    let reservePort () =
        use listener = new TcpListener(loopback, 0)
        listener.Start()
        endpointPort listener

    let createConfiguration listenPort backendPort =
        let arguments =
            [| "--listen-address"
               string loopback
               "--listen-port"
               string listenPort
               "--backend-host"
               string loopback
               "--backend-port"
               string backendPort |]

        match ForwardingConfiguration.tryParseArguments arguments with
        | Ok configuration -> configuration
        | Error error -> failwithf "Benchmark configuration was invalid: %s" error

    let connect port (cancellationToken: CancellationToken) =
        task {
            let connection = new TcpClient(AddressFamily.InterNetwork)

            try
                do! connection.ConnectAsync(loopback, port, cancellationToken)
                return connection
            with error ->
                connection.Dispose()
                return raise error
        }

    let readExactly
        (stream: NetworkStream)
        (buffer: byte array)
        (cancellationToken: CancellationToken)
        =
        task {
            let mutable offset = 0

            while offset < buffer.Length do
                let! received =
                    stream.ReadAsync(
                        buffer.AsMemory(offset, buffer.Length - offset),
                        cancellationToken
                    )

                if received = 0 then
                    raise (EndOfStreamException $"Stream ended after {offset} of {buffer.Length} bytes.")

                offset <- offset + received
        }

    let readMessage
        (stream: NetworkStream)
        (buffer: byte array)
        (cancellationToken: CancellationToken)
        =
        task {
            let mutable offset = 0
            let mutable reachedEndOfStream = false

            while offset < buffer.Length && not reachedEndOfStream do
                let! received =
                    stream.ReadAsync(
                        buffer.AsMemory(offset, buffer.Length - offset),
                        cancellationToken
                    )

                if received = 0 then
                    reachedEndOfStream <- true
                else
                    offset <- offset + received

            if reachedEndOfStream && offset > 0 then
                raise (EndOfStreamException $"Stream ended after {offset} of {buffer.Length} bytes.")

            return not reachedEndOfStream
        }

    let runEchoServer
        (stream: NetworkStream)
        payloadSize
        (cancellationToken: CancellationToken)
        =
        task {
            let buffer = Array.zeroCreate<byte> payloadSize

            try
                let mutable connected = true

                while connected do
                    let! receivedMessage = readMessage stream buffer cancellationToken

                    if receivedMessage then
                        do! stream.WriteAsync(buffer.AsMemory(), cancellationToken)
                    else
                        connected <- false
            with
            | :? OperationCanceledException when cancellationToken.IsCancellationRequested -> ()
            | :? IOException when cancellationToken.IsCancellationRequested -> ()
            | :? ObjectDisposedException when cancellationToken.IsCancellationRequested -> ()
        }

    member private _.RoundTrip() =
        task {
            do! clientStream.WriteAsync(payload.AsMemory(), CancellationToken.None)
            do! readExactly clientStream response CancellationToken.None
        }

    member private this.Setup(viaPipeL4: bool) =
        task {
            payload <- Array.init this.PayloadSize (fun index -> byte (index % 251))
            response <- Array.zeroCreate this.PayloadSize

            let cancellation = new CancellationTokenSource()
            lifetimeCancellation <- Some cancellation

            let listener = new TcpListener(loopback, 0)
            listener.Start()
            backendListener <- Some listener

            use setupTimeout = new CancellationTokenSource(TimeSpan.FromSeconds 30.0)
            let backendPort = endpointPort listener
            let acceptBackend = listener.AcceptTcpClientAsync(setupTimeout.Token).AsTask()

            let! clientConnection =
                if viaPipeL4 then
                    let proxyPort = reservePort ()
                    let configuration = createConfiguration proxyPort backendPort

                    proxyCompletion <-
                        TcpForwarder.runAsync configuration NullLogger.Instance cancellation.Token
                        :> Task

                    connect proxyPort setupTimeout.Token
                else
                    connect backendPort setupTimeout.Token

            client <- Some clientConnection

            let! backendConnection = acceptBackend
            backend <- Some backendConnection

            clientStream <- clientConnection.GetStream()

            echoCompletion <-
                runEchoServer
                    (backendConnection.GetStream())
                    this.PayloadSize
                    cancellation.Token
                :> Task

            do! this.RoundTrip()

            if not (payload.AsSpan().SequenceEqual(response.AsSpan())) then
                invalidOp "The benchmark preflight response did not match the request payload."
        }

    [<Params(64, 4096, 1048576)>]
    member val PayloadSize = 0 with get, set

    [<GlobalSetup(Target = "Direct")>]
    member this.SetupDirect() = this.Setup(viaPipeL4 = false)

    [<GlobalSetup(Target = "ViaPipeL4")>]
    member this.SetupViaPipeL4() = this.Setup(viaPipeL4 = true)

    [<GlobalCleanup>]
    member _.Cleanup() =
        task {
            match lifetimeCancellation with
            | Some cancellation -> cancellation.Cancel()
            | None -> ()

            match client with
            | Some connection -> connection.Dispose()
            | None -> ()

            match backend with
            | Some connection -> connection.Dispose()
            | None -> ()

            match backendListener with
            | Some listener -> listener.Stop()
            | None -> ()

            do! Task.WhenAll(echoCompletion, proxyCompletion)

            match lifetimeCancellation with
            | Some cancellation -> cancellation.Dispose()
            | None -> ()

            backendListener <- None
            client <- None
            backend <- None
            lifetimeCancellation <- None
            proxyCompletion <- Task.CompletedTask
            echoCompletion <- Task.CompletedTask
        }

    [<Benchmark(Baseline = true)>]
    member this.Direct() = this.RoundTrip()

    [<Benchmark>]
    member this.ViaPipeL4() = this.RoundTrip()
