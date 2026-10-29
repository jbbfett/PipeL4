namespace PipeL4.Benchmarks

open System
open System.IO
open System.Net
open System.Net.Sockets
open System.Threading
open System.Threading.Tasks
open BenchmarkDotNet.Attributes
open Microsoft.Extensions.Logging.Abstractions
open PipeL4.Core

[<MemoryDiagnoser>]
type CapacityDiagnosticsBenchmarks() =
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
    let mutable observedMetrics: ConnectionRuntimeMetrics option = None

    let endpointPort (listener: TcpListener) =
        (listener.LocalEndpoint :?> IPEndPoint).Port

    let reservePort () =
        use listener = new TcpListener(loopback, 0)
        listener.Start()
        endpointPort listener

    let createConfiguration listenPort backendPort bounded diagnosticsEnabled =
        let capacityArguments =
            if bounded then
                [| "--client-to-server-logical-capacity-bytes"
                   "262144"
                   "--server-to-client-logical-capacity-bytes"
                   "262144"
                   "--connection-physical-capacity-bytes"
                   "262144" |]
            else
                Array.empty

        let diagnosticsArguments =
            if diagnosticsEnabled then Array.empty else [| "--disable-runtime-diagnostics" |]

        let arguments =
            Array.concat
                [| [| "--listen-address"
                      string loopback
                      "--listen-port"
                      string listenPort
                      "--backend-host"
                      string loopback
                      "--backend-port"
                      string backendPort |]
                   capacityArguments
                   diagnosticsArguments |]

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

    member private this.RoundTrip() =
        task {
            do! clientStream.WriteAsync(payload.AsMemory(), CancellationToken.None)
            do! readExactly clientStream response CancellationToken.None
        }

    member private this.ValidateMetrics() =
        match observedMetrics with
        | None -> invalidOp "The diagnostics benchmark did not receive a runtime metrics object."
        | Some metrics ->
            let snapshot = metrics.GetSnapshot()
            let maximum = if this.Bounded then 262_144L else Int64.MaxValue
            let directionalPhysicalMaximum =
                if this.Bounded then 131_072L else Int64.MaxValue

            if snapshot.ConnectionPhysicalCapacity.LimitBytes <> maximum then
                invalidOp "The diagnostics benchmark observed an unexpected physical limit."

            if snapshot.CurrentPhysicalBufferCapacityBytes
               <> snapshot.ConnectionPhysicalCapacity.CurrentBytes
               || snapshot.PeakPhysicalBufferCapacityBytes
                  <> snapshot.ConnectionPhysicalCapacity.PeakBytes then
                invalidOp "The diagnostics benchmark observed inconsistent connection physical snapshots."

            if snapshot.CurrentPhysicalBufferCapacityBytes > maximum
               || snapshot.PeakPhysicalBufferCapacityBytes > maximum then
                invalidOp "The diagnostics benchmark exceeded its configured connection physical capacity."

            if snapshot.PeakPhysicalBufferCapacityBytes <= 0L then
                invalidOp "The diagnostics benchmark observed no physical ownership while work occurred."

            let directions = [| snapshot.ClientToBackend; snapshot.BackendToClient |]

            for direction in directions do
                if direction.LogicalCapacityLimitBytes <> maximum then
                    invalidOp "The diagnostics benchmark observed an unexpected logical limit."

                if direction.CurrentLogicalBufferedBytes <> direction.LogicalCapacity.CurrentBytes
                   || direction.PeakLogicalBufferedBytes <> direction.LogicalCapacity.PeakBytes then
                    invalidOp "The diagnostics benchmark observed inconsistent logical snapshots."

                if direction.PeakLogicalBufferedBytes <= 0L
                   || direction.LogicalCapacity.PeakBytes <= 0L
                   || direction.PeakLogicalBufferedBytes > direction.LogicalCapacityLimitBytes then
                    invalidOp "The diagnostics benchmark observed an invalid logical ownership peak."

                if direction.PhysicalShare.LimitBytes <> directionalPhysicalMaximum then
                    invalidOp "The diagnostics benchmark observed an unexpected physical-share limit."

                if direction.PhysicalShare.PeakBytes <= 0L
                   || direction.PhysicalShare.PeakBytes > direction.PhysicalShare.LimitBytes then
                    invalidOp "The diagnostics benchmark observed an invalid physical-share peak."

                if this.RuntimeDiagnosticsEnabled then
                    if direction.ReceiveRentalAttempts <= 0L then
                        invalidOp "Enabled diagnostics did not record receive rentals."
                elif direction.ReceiveRentalAttempts <> 0L
                     || direction.ReceiveReturnedOwnerCapacityBytes <> 0L
                     || direction.TerminalOutcome <> "disabled" then
                    invalidOp "Disabled diagnostics changed its zero-observation contract."

    [<Params(4096, 1048576)>]
    member val PayloadSize = 0 with get, set

    [<Params(false, true)>]
    member val Bounded = false with get, set

    [<Params(true, false)>]
    member val RuntimeDiagnosticsEnabled = true with get, set

    [<GlobalSetup>]
    member this.Setup() =
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
            let proxyPort = reservePort ()
            let configuration =
                createConfiguration
                    proxyPort
                    backendPort
                    this.Bounded
                    this.RuntimeDiagnosticsEnabled

            proxyCompletion <-
                TcpForwarder.runWithOptionalProcessorPipelinesAndMemoryObserverAsync
                    configuration
                    createProcessorPipelines
                    (fun metrics -> observedMetrics <- Some metrics)
                    NullLogger.Instance
                    cancellation.Token
                :> Task

            let! clientConnection = connect proxyPort setupTimeout.Token
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
                invalidOp "The diagnostics benchmark preflight response did not match the request."

            this.ValidateMetrics()
        }

    [<IterationCleanup>]
    member this.ValidateIteration() =
        if not (obj.ReferenceEquals(response, null)) then
            if not (payload.AsSpan().SequenceEqual(response.AsSpan())) then
                invalidOp "The diagnostics benchmark response did not match the request."

        this.ValidateMetrics()

    member private _.ValidateFinalAccounting() =
        match observedMetrics with
        | Some metrics ->
            let snapshot = metrics.GetSnapshot()

            if snapshot.CurrentPhysicalBufferCapacityBytes <> 0L
               || snapshot.ConnectionPhysicalCapacity.CurrentBytes <> 0L
               || snapshot.ClientToBackendPhysicalShare.CurrentBytes <> 0L
               || snapshot.BackendToClientPhysicalShare.CurrentBytes <> 0L
               || snapshot.ClientToBackend.CurrentLogicalBufferedBytes <> 0L
               || snapshot.BackendToClient.CurrentLogicalBufferedBytes <> 0L
               || snapshot.ClientToBackend.LogicalCapacity.CurrentBytes <> 0L
               || snapshot.BackendToClient.LogicalCapacity.CurrentBytes <> 0L then
                invalidOp "The diagnostics benchmark left runtime capacity reserved after cleanup."
        | None -> invalidOp "The diagnostics benchmark lost its metrics object during cleanup."

    [<GlobalCleanup>]
    member this.Cleanup() =
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

            this.ValidateFinalAccounting()

            match lifetimeCancellation with
            | Some cancellation -> cancellation.Dispose()
            | None -> ()

            backendListener <- None
            client <- None
            backend <- None
            lifetimeCancellation <- None
            proxyCompletion <- Task.CompletedTask
            echoCompletion <- Task.CompletedTask
            observedMetrics <- None
        }

    [<Benchmark(Baseline = true)>]
    member this.ForwardingWithSelectedDiagnostics() = this.RoundTrip()
