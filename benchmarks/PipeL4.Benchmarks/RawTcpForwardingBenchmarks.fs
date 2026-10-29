namespace PipeL4.Benchmarks

open System
open System.Diagnostics
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

[<RequireQualifiedAccess>]
type private ScriptFixture =
    | InputBacked
    | GeneratedOutput
    | StateAccess
    | CachedState

    member this.FileName =
        match this with
        | InputBacked -> "PassThrough.fsx"
        | GeneratedOutput -> "GeneratedOutput.fsx"
        | StateAccess -> "StateAccess.fsx"
        | CachedState -> "CachedState.fsx"

    member this.ResourceName =
        $"PipeL4.Benchmarks.Scripts.{this.FileName}"

[<RequireQualifiedAccess>]
type private ForwardingRoute =
    | Direct
    | RawPipeL4
    | InternalProcessorPipeline of depth: int
    | BoundedInternalProcessorPipeline of depth: int
    | FsxProcessorPipeline of fixture: ScriptFixture * depth: int

type LogicalThroughputColumn() =
    let calculateValue (summary: Summary) (benchmarkCase: BenchmarkCase) =
        let isExternallyGatedPressure =
            benchmarkCase.Descriptor.Type.Name = "BackpressurePressureBenchmarks"

        let payloadSize =
            benchmarkCase.Parameters.Items
            |> Seq.tryFind (fun parameter -> parameter.Name = "PayloadSize")

        let connectionCount =
            benchmarkCase.Parameters.Items
            |> Seq.tryFind (fun parameter -> parameter.Name = "ConnectionCount")

        let statistics =
            summary[benchmarkCase]
            |> Option.ofObj
            |> Option.bind (fun report -> report.ResultStatistics |> Option.ofObj)

        match payloadSize, connectionCount, statistics with
        | _, _, _ when isExternallyGatedPressure -> "?"
        | Some parameter, connectionCount, Some result when result.Mean > 0.0 ->
            let connections =
                connectionCount
                |> Option.map (fun value -> Convert.ToDouble(value.Value, CultureInfo.InvariantCulture))
                |> Option.defaultValue 1.0

            let logicalBytes =
                2.0
                * connections
                * Convert.ToDouble(parameter.Value, CultureInfo.InvariantCulture)
            let mebibytesPerSecond = logicalBytes * 1_000_000_000.0 / result.Mean / (1024.0 * 1024.0)
            mebibytesPerSecond.ToString("N2", CultureInfo.InvariantCulture)
        | _ -> "?"

    interface IColumn with
        member _.Id = "LogicalThroughput"
        member _.ColumnName = "Logical MiB/s"
        member _.AlwaysShow = false
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
    let mutable scriptWorkspace: string option = None
    let mutable observedMetrics: ConnectionRuntimeMetrics option = None

    let endpointPort (listener: TcpListener) =
        (listener.LocalEndpoint :?> IPEndPoint).Port

    let reservePort () =
        use listener = new TcpListener(loopback, 0)
        listener.Start()
        endpointPort listener

    let createConfiguration listenPort backendPort clientPaths serverPaths extraArguments =
        let pluginArguments optionName paths =
            paths |> Array.collect (fun path -> [| optionName; path |])

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
                   pluginArguments "--client-to-server-plugin" clientPaths
                   pluginArguments "--server-to-client-plugin" serverPaths
                   extraArguments |]

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

    let passThroughProcessor () =
        { new IByteStreamProcessor with
            member _.ProcessAsync(input, output, _) =
                output.AppendInput(0L, input.Bytes.Length)
                ValueTask<ProcessorResult>({ RetainedLength = 0L }) }

    let createProcessorPipelines depth (_: ConnectionState) =
        let createPipeline () = Array.init depth (fun _ -> passThroughProcessor ())

        { ClientToBackend = createPipeline ()
          BackendToClient = createPipeline () }

    let materializeScript (fixture: ScriptFixture) =
        let workspace =
            Path.Combine(
                Path.GetTempPath(),
                "PipeL4.Benchmarks",
                Guid.NewGuid().ToString "N"
            )

        Directory.CreateDirectory workspace |> ignore
        scriptWorkspace <- Some workspace

        let assembly = typeof<RawTcpForwardingBenchmarks>.Assembly

        use sourceStream =
            assembly.GetManifestResourceStream(fixture.ResourceName)
            |> Option.ofObj
            |> Option.defaultWith (fun () ->
                invalidOp $"Embedded benchmark script '{fixture.ResourceName}' was not found.")

        use reader = new StreamReader(sourceStream)
        let scriptPath = Path.Combine(workspace, fixture.FileName)
        File.WriteAllText(scriptPath, reader.ReadToEnd())
        scriptPath

    let loadScriptDefinitionAsync
        (fixture: ScriptFixture)
        (depth: int)
        (cancellationToken: CancellationToken)
        : Task<PipeL4.Plugins.ScriptPipelineDefinition>
        =
        task {
            let scriptPath = materializeScript fixture
            let paths = Array.replicate depth scriptPath
            let compiler = PipeL4.Plugins.ScriptCompiler()
            let mutable compilationCount = 0
            let stopwatch = Stopwatch.StartNew()

            let compile (path: string) (token: CancellationToken) =
                Interlocked.Increment(&compilationCount) |> ignore
                compiler.CompileAndLoadAsync(path, token)

            let! definition =
                PipeL4.Plugins.ScriptPipelineLoader.loadWithCompilerAsync
                    compile
                    paths
                    paths
                    cancellationToken

            stopwatch.Stop()

            if compilationCount <> 1 then
                invalidOp
                    $"The benchmark expected one unique script compilation, but observed {compilationCount}."

            Console.WriteLine(
                "FSX startup observation: fixture={0}; depth-per-direction={1}; unique-compilations={2}; compile-load-discover-ms={3:F3}",
                fixture.FileName,
                depth,
                compilationCount,
                stopwatch.Elapsed.TotalMilliseconds
            )

            return definition
        }

    member private _.RoundTrip() =
        task {
            do! clientStream.WriteAsync(payload.AsMemory(), CancellationToken.None)
            do! readExactly clientStream response CancellationToken.None
        }

    member private this.Setup(route: ForwardingRoute) =
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
                match route with
                | ForwardingRoute.Direct -> connect backendPort setupTimeout.Token
                | ForwardingRoute.RawPipeL4 ->
                    let proxyPort = reservePort ()
                    let configuration =
                        createConfiguration proxyPort backendPort Array.empty Array.empty [||]

                    proxyCompletion <-
                        TcpForwarder.runAsync configuration NullLogger.Instance cancellation.Token
                        :> Task

                    connect proxyPort setupTimeout.Token
                | ForwardingRoute.InternalProcessorPipeline depth ->
                    let proxyPort = reservePort ()
                    let configuration =
                        createConfiguration proxyPort backendPort Array.empty Array.empty [||]

                    proxyCompletion <-
                        TcpForwarder.runWithProcessorPipelinesAsync
                            configuration
                            (createProcessorPipelines depth)
                            NullLogger.Instance
                            cancellation.Token
                        :> Task

                    connect proxyPort setupTimeout.Token
                | ForwardingRoute.BoundedInternalProcessorPipeline depth ->
                    let proxyPort = reservePort ()
                    let configuration =
                        createConfiguration
                            proxyPort
                            backendPort
                            Array.empty
                            Array.empty
                            [| "--client-to-server-logical-capacity-bytes"
                               "262144"
                               "--server-to-client-logical-capacity-bytes"
                               "262144"
                               "--connection-physical-capacity-bytes"
                               "262144" |]

                    proxyCompletion <-
                        TcpForwarder.runWithOptionalProcessorPipelinesAndMemoryObserverAsync
                            configuration
                            (createProcessorPipelines depth)
                            (fun metrics -> observedMetrics <- Some metrics)
                            NullLogger.Instance
                            cancellation.Token
                        :> Task

                    connect proxyPort setupTimeout.Token
                | ForwardingRoute.FsxProcessorPipeline(fixture, depth) ->
                    task {
                        let proxyPort = reservePort ()
                        let! definition =
                            loadScriptDefinitionAsync fixture depth setupTimeout.Token

                        let paths = definition.ClientToServerPaths

                        let configuration =
                            createConfiguration proxyPort backendPort paths paths [||]

                        proxyCompletion <-
                            TcpForwarder.runWithOptionalProcessorPipelineFactoryAsync
                                configuration
                                definition.CreateProcessorPipelinesAsync
                                NullLogger.Instance
                                cancellation.Token
                            :> Task

                        return! connect proxyPort setupTimeout.Token
                    }

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

            match observedMetrics with
            | Some metrics ->
                let snapshot = metrics.GetSnapshot()
                let clientGeneratedOutputBytes =
                    snapshot.ClientToBackend.Stages
                    |> List.sumBy (fun stage -> stage.GeneratedOutputBytes)

                let backendGeneratedOutputBytes =
                    snapshot.BackendToClient.Stages
                    |> List.sumBy (fun stage -> stage.GeneratedOutputBytes)

                if clientGeneratedOutputBytes <> snapshot.ClientToBackend.GeneratedOutputBytes
                   || backendGeneratedOutputBytes <> snapshot.BackendToClient.GeneratedOutputBytes then
                    invalidOp "The bounded transport generated-output totals did not match stage metrics."

                Console.WriteLine(
                    "Bounded transport observation: payload={0}; logical-limits={1}/{2}; physical-limit={3}; physical-share-limits={4}/{5}; peak-logical-c2b={6}; peak-logical-b2c={7}; peak-physical={8}; processor-generated-output-bytes={9}/{10}; rental-attempts={11}/{12}; returned-owner-capacity={13}/{14}; over-rent-retries={15}/{16}",
                    this.PayloadSize,
                    snapshot.ClientToBackend.LogicalCapacityLimitBytes,
                    snapshot.BackendToClient.LogicalCapacityLimitBytes,
                    snapshot.ConnectionPhysicalCapacity.LimitBytes,
                    snapshot.ClientToBackend.PhysicalShare.LimitBytes,
                    snapshot.BackendToClient.PhysicalShare.LimitBytes,
                    snapshot.ClientToBackend.PeakLogicalBufferedBytes,
                    snapshot.BackendToClient.PeakLogicalBufferedBytes,
                    snapshot.PeakPhysicalBufferCapacityBytes,
                    snapshot.ClientToBackend.GeneratedOutputBytes,
                    snapshot.BackendToClient.GeneratedOutputBytes,
                    snapshot.ClientToBackend.ReceiveRentalAttempts,
                    snapshot.BackendToClient.ReceiveRentalAttempts,
                    snapshot.ClientToBackend.ReceiveReturnedOwnerCapacityBytes,
                    snapshot.BackendToClient.ReceiveReturnedOwnerCapacityBytes,
                    snapshot.ClientToBackend.ReceiveOverRentRetries,
                    snapshot.BackendToClient.ReceiveOverRentRetries
                )
            | None -> ()
        }

    [<Params(64, 4096, 1048576)>]
    member val PayloadSize = 0 with get, set

    [<GlobalSetup(Target = "Direct")>]
    member this.SetupDirect() = this.Setup(ForwardingRoute.Direct)

    [<GlobalSetup(Target = "RawPipeL4")>]
    member this.SetupRawPipeL4() = this.Setup(ForwardingRoute.RawPipeL4)

    [<GlobalSetup(Target = "ViaEmptyProcessorPipeline")>]
    member this.SetupViaEmptyProcessorPipeline() =
        this.Setup(ForwardingRoute.InternalProcessorPipeline 0)

    [<GlobalSetup(Target = "InternalProcessorPipeline")>]
    member this.SetupInternalProcessorPipeline() =
        this.Setup(ForwardingRoute.InternalProcessorPipeline 1)

    [<GlobalSetup(Target = "BoundedInternalProcessorPipeline")>]
    member this.SetupBoundedInternalProcessorPipeline() =
        this.Setup(ForwardingRoute.BoundedInternalProcessorPipeline 1)

    [<GlobalSetup(Target = "InternalFourStagePipeline")>]
    member this.SetupInternalFourStagePipeline() =
        this.Setup(ForwardingRoute.InternalProcessorPipeline 4)

    [<GlobalSetup(Target = "InternalEightStagePipeline")>]
    member this.SetupInternalEightStagePipeline() =
        this.Setup(ForwardingRoute.InternalProcessorPipeline 8)

    [<GlobalSetup(Target = "FsxProcessorPipeline")>]
    member this.SetupFsxProcessorPipeline() =
        this.Setup(ForwardingRoute.FsxProcessorPipeline(ScriptFixture.InputBacked, 1))

    [<GlobalSetup(Target = "FsxFourStagePipeline")>]
    member this.SetupFsxFourStagePipeline() =
        this.Setup(ForwardingRoute.FsxProcessorPipeline(ScriptFixture.InputBacked, 4))

    [<GlobalSetup(Target = "FsxGeneratedOutputPipeline")>]
    member this.SetupFsxGeneratedOutputPipeline() =
        this.Setup(ForwardingRoute.FsxProcessorPipeline(ScriptFixture.GeneratedOutput, 1))

    [<GlobalSetup(Target = "FsxWithStateAccess")>]
    member this.SetupFsxWithStateAccess() =
        this.Setup(ForwardingRoute.FsxProcessorPipeline(ScriptFixture.StateAccess, 1))

    [<GlobalSetup(Target = "FsxWithCachedState")>]
    member this.SetupFsxWithCachedState() =
        this.Setup(ForwardingRoute.FsxProcessorPipeline(ScriptFixture.CachedState, 1))

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

            match scriptWorkspace with
            | Some workspace when Directory.Exists workspace ->
                Directory.Delete(workspace, recursive = true)
            | _ -> ()

            match lifetimeCancellation with
            | Some cancellation -> cancellation.Dispose()
            | None -> ()

            backendListener <- None
            client <- None
            backend <- None
            lifetimeCancellation <- None
            proxyCompletion <- Task.CompletedTask
            echoCompletion <- Task.CompletedTask
            scriptWorkspace <- None
            observedMetrics <- None
        }

    [<Benchmark(Baseline = true)>]
    member this.Direct() = this.RoundTrip()

    [<Benchmark>]
    member this.RawPipeL4() = this.RoundTrip()

    [<Benchmark>]
    member this.ViaEmptyProcessorPipeline() = this.RoundTrip()

    [<Benchmark>]
    member this.InternalProcessorPipeline() = this.RoundTrip()

    [<Benchmark>]
    member this.BoundedInternalProcessorPipeline() = this.RoundTrip()

    [<Benchmark>]
    member this.InternalFourStagePipeline() = this.RoundTrip()

    [<Benchmark>]
    member this.InternalEightStagePipeline() = this.RoundTrip()

    [<Benchmark>]
    member this.FsxProcessorPipeline() = this.RoundTrip()

    [<Benchmark>]
    member this.FsxFourStagePipeline() = this.RoundTrip()

    [<Benchmark>]
    member this.FsxGeneratedOutputPipeline() = this.RoundTrip()

    [<Benchmark>]
    member this.FsxWithStateAccess() = this.RoundTrip()

    [<Benchmark>]
    member this.FsxWithCachedState() = this.RoundTrip()
