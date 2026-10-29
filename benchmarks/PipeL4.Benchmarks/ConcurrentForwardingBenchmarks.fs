namespace PipeL4.Benchmarks

open System
open System.Collections.Concurrent
open System.IO
open System.Net
open System.Net.Sockets
open System.Threading
open System.Threading.Tasks
open BenchmarkDotNet.Attributes
open Microsoft.Extensions.Logging.Abstractions
open PipeL4.Core

[<MemoryDiagnoser>]
type ConcurrentForwardingBenchmarks() =
    let loopback = IPAddress.Loopback
    let mutable backendListener: TcpListener option = None
    let mutable clients = Array.empty<TcpClient>
    let mutable backends = Array.empty<TcpClient>
    let mutable clientStreams = Array.empty<NetworkStream>
    let mutable responses = Array.empty<byte array>
    let mutable payload = Array.empty<byte>
    let mutable echoCompletions = Array.empty<Task>
    let mutable proxyCompletion: Task = Task.CompletedTask
    let mutable lifetimeCancellation: CancellationTokenSource option = None
    let observedMetrics = ConcurrentBag<ConnectionRuntimeMetrics>()

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
               string backendPort
               "--client-to-server-logical-capacity-bytes"
               "262144"
               "--server-to-client-logical-capacity-bytes"
               "262144"
               "--connection-physical-capacity-bytes"
               "262144" |]

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
                    do! readExactly stream buffer cancellationToken
                    do! stream.WriteAsync(buffer.AsMemory(), cancellationToken)
            with
            | :? OperationCanceledException when cancellationToken.IsCancellationRequested -> ()
            | :? EndOfStreamException -> ()
            | :? IOException when cancellationToken.IsCancellationRequested -> ()
            | :? ObjectDisposedException when cancellationToken.IsCancellationRequested -> ()
        }

    let passThroughProcessor () =
        { new IByteStreamProcessor with
            member _.ProcessAsync(input, output, _) =
                output.AppendInput(0L, input.Bytes.Length)
                ValueTask<ProcessorResult>({ RetainedLength = 0L }) }

    let createProcessorPipelines (_: ConnectionState) =
        { ClientToBackend = [| passThroughProcessor () |]
          BackendToClient = [| passThroughProcessor () |] }

    member private this.RoundTripAll() =
        task {
            let writes =
                clientStreams
                |> Array.map (fun stream -> stream.WriteAsync(payload.AsMemory(), CancellationToken.None).AsTask())

            do! Task.WhenAll writes

            let reads =
                Array.mapi
                    (fun index stream -> readExactly stream responses[index] CancellationToken.None)
                    clientStreams

            let! _ = Task.WhenAll reads
            return ()
        }

    member private this.ValidateResults() =
        for response in responses do
            if not (payload.AsSpan().SequenceEqual(response.AsSpan())) then
                invalidOp "The concurrent forwarding benchmark returned incorrect bytes."

        if observedMetrics.Count <> this.ConnectionCount then
            invalidOp "The concurrent forwarding benchmark did not observe every connection."

        for metrics in observedMetrics do
            let snapshot = metrics.GetSnapshot()

            if snapshot.PeakPhysicalBufferCapacityBytes > 262_144L
               || snapshot.ClientToBackend.PeakLogicalBufferedBytes > 262_144L
               || snapshot.BackendToClient.PeakLogicalBufferedBytes > 262_144L then
                invalidOp "The concurrent forwarding benchmark exceeded its capacity limits."

            if snapshot.ClientToBackend.PhysicalShare.LimitBytes > 262_144L
               || snapshot.BackendToClient.PhysicalShare.LimitBytes > 262_144L then
                invalidOp "The concurrent forwarding benchmark observed an invalid physical share."

    [<Params(1, 8)>]
    member val ConnectionCount = 1 with get, set

    [<Params(4096, 1048576)>]
    member val PayloadSize = 0 with get, set

    [<GlobalSetup>]
    member this.Setup() =
        task {
            payload <- Array.init this.PayloadSize (fun index -> byte (index % 251))
            responses <- Array.init this.ConnectionCount (fun _ -> Array.zeroCreate this.PayloadSize)

            let cancellation = new CancellationTokenSource()
            lifetimeCancellation <- Some cancellation

            let listener = new TcpListener(loopback, 0)
            listener.Start()
            backendListener <- Some listener

            use setupTimeout = new CancellationTokenSource(TimeSpan.FromSeconds 60.0)
            let backendPort = endpointPort listener
            let proxyPort = reservePort ()
            let acceptTasks =
                Array.init this.ConnectionCount (fun _ -> listener.AcceptTcpClientAsync(setupTimeout.Token).AsTask())

            let configuration = createConfiguration proxyPort backendPort

            proxyCompletion <-
                TcpForwarder.runWithOptionalProcessorPipelinesAndMemoryObserverAsync
                    configuration
                    createProcessorPipelines
                    (fun metrics -> observedMetrics.Add metrics)
                    NullLogger.Instance
                    cancellation.Token
                :> Task

            let! connectedClients =
                Array.init this.ConnectionCount (fun _ -> connect proxyPort setupTimeout.Token)
                |> Task.WhenAll

            let! acceptedBackends = Task.WhenAll acceptTasks
            clients <- connectedClients
            backends <- acceptedBackends
            clientStreams <- clients |> Array.map (fun connection -> connection.GetStream())

            echoCompletions <-
                backends
                |> Array.map (fun connection ->
                    runEchoServer
                        (connection.GetStream())
                        this.PayloadSize
                        cancellation.Token
                    :> Task)

            do! this.RoundTripAll()
            this.ValidateResults()
        }

    [<IterationCleanup>]
    member this.ValidateIteration() = this.ValidateResults()

    [<GlobalCleanup>]
    member _.Cleanup() =
        task {
            match lifetimeCancellation with
            | Some cancellation -> cancellation.Cancel()
            | None -> ()

            for connection in clients do
                connection.Dispose()

            for connection in backends do
                connection.Dispose()

            match backendListener with
            | Some listener -> listener.Stop()
            | None -> ()

            do! Task.WhenAll echoCompletions
            do! proxyCompletion

            for metrics in observedMetrics do
                let snapshot = metrics.GetSnapshot()

                if snapshot.CurrentPhysicalBufferCapacityBytes <> 0L
                   || snapshot.ClientToBackend.CurrentLogicalBufferedBytes <> 0L
                   || snapshot.BackendToClient.CurrentLogicalBufferedBytes <> 0L then
                    invalidOp "The concurrent forwarding benchmark left capacity reserved."

            match lifetimeCancellation with
            | Some cancellation -> cancellation.Dispose()
            | None -> ()

            backendListener <- None
            clients <- Array.empty
            backends <- Array.empty
            clientStreams <- Array.empty
            responses <- Array.empty
            echoCompletions <- Array.empty
            lifetimeCancellation <- None
            proxyCompletion <- Task.CompletedTask
            observedMetrics.Clear()
        }

    [<Benchmark(Baseline = true)>]
    member this.BoundedConcurrentRoundTrip() = this.RoundTripAll()
