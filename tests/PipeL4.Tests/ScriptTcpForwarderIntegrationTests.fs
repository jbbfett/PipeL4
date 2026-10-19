namespace PipeL4.Tests

open System
open System.Collections.Concurrent
open System.IO
open System.Net
open System.Net.Sockets
open System.Reflection
open System.Text
open System.Threading
open System.Threading.Tasks
open FsUnit.Xunit
open Microsoft.Extensions.Logging
open Microsoft.Extensions.Logging.Abstractions
open PipeL4.Core
open PipeL4.Plugins
open Xunit

module ScriptTcpForwarderIntegrationTests =
    let private loopback = IPAddress.Loopback

    [<NoEquality; NoComparison>]
    type private ProxyContext =
        { ListenPort: int
          Shutdown: CancellationTokenSource
          Completion: Task }

    type private TestWorkspace() =
        let rootPath =
            Path.Combine(Path.GetTempPath(), "PipeL4.Tests", Guid.NewGuid().ToString "N")

        let sourceRoot = Path.Combine(rootPath, "sources")
        let compiler = ScriptCompiler(Path.Combine(rootPath, "compiler"))

        do Directory.CreateDirectory(sourceRoot) |> ignore

        member _.Compiler = compiler

        member _.WriteSource(fileName: string, source: string) =
            let path = Path.Combine(sourceRoot, fileName)
            File.WriteAllText(path, source)
            path

        interface IDisposable with
            member _.Dispose() =
                if Directory.Exists rootPath then
                    Directory.Delete(rootPath, recursive = true)

    type private NoopScope() =
        interface IDisposable with
            member _.Dispose() = ()

    type private RecordingLogger() =
        let failures = ConcurrentQueue<exn>()
        let firstFailure = TaskCompletionSource<exn>(TaskCreationOptions.RunContinuationsAsynchronously)
        let failureRecorded = new SemaphoreSlim(0)

        member _.Failures = failures.ToArray()
        member _.FirstFailure = firstFailure.Task

        member _.WaitForFailureCountAsync(expectedCount, cancellationToken: CancellationToken) =
            task {
                while failures.Count < expectedCount do
                    do! failureRecorded.WaitAsync(cancellationToken)
            }

        interface ILogger with
            member _.BeginScope<'TState when 'TState: not null>(_state: 'TState) =
                new NoopScope() :> IDisposable

            member _.IsEnabled(_logLevel) = true

            member _.Log<'TState>
                (
                    _logLevel: LogLevel,
                    _eventId: EventId,
                    _state: 'TState,
                    error: exn | null,
                    _formatter: Func<'TState, exn | null, string>
                ) =
                match Option.ofObj error with
                | Some error ->
                    failures.Enqueue error
                    firstFailure.TrySetResult(error) |> ignore
                    failureRecorded.Release() |> ignore
                | None -> ()

    let private endpointPort (listener: TcpListener) =
        (listener.LocalEndpoint :?> IPEndPoint).Port

    let private reservePort () =
        use listener = new TcpListener(loopback, 0)
        listener.Start()
        endpointPort listener

    let private pluginArguments optionName paths =
        paths |> Array.collect (fun path -> [| optionName; path |])

    let private createConfiguration listenPort backendPort clientPaths serverPaths =
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
                   pluginArguments "--server-to-client-plugin" serverPaths |]

        match ForwardingConfiguration.tryParseArguments arguments with
        | Ok configuration -> configuration
        | Error error -> failwithf "Test configuration was invalid: %s" error

    let private connect port (cancellationToken: CancellationToken) =
        task {
            let client = new TcpClient(AddressFamily.InterNetwork)

            try
                do! client.ConnectAsync(loopback, port, cancellationToken)
                return client
            with error ->
                client.Dispose()
                return raise error
        }

    let private connectEventually
        (proxy: ProxyContext)
        (cancellationToken: CancellationToken)
        =
        task {
            let mutable connection: TcpClient option = None

            while connection.IsNone do
                cancellationToken.ThrowIfCancellationRequested()

                if proxy.Completion.IsCompleted then
                    do! proxy.Completion
                    failwith "The proxy stopped before accepting a connection."

                try
                    let! client = connect proxy.ListenPort cancellationToken
                    connection <- Some client
                with :? SocketException ->
                    do! Task.Delay(20, cancellationToken)

            return connection.Value
        }

    let private readExactly
        (stream: NetworkStream)
        length
        (cancellationToken: CancellationToken)
        =
        task {
            let result = Array.zeroCreate<byte> length
            let mutable offset = 0

            while offset < length do
                let! read =
                    stream.ReadAsync(
                        result.AsMemory(offset, length - offset),
                        cancellationToken
                    )

                if read = 0 then
                    failwithf "Stream ended after %d of %d expected bytes." offset length

                offset <- offset + read

            return result
        }

    let private expectEndOfStream
        (stream: NetworkStream)
        (cancellationToken: CancellationToken)
        =
        task {
            let buffer = Array.zeroCreate<byte> 1
            let! read = stream.ReadAsync(buffer.AsMemory(), cancellationToken)
            read |> should equal 0
        }

    let private expectDisconnect
        (stream: NetworkStream)
        (cancellationToken: CancellationToken)
        =
        task {
            let buffer = Array.zeroCreate<byte> 1

            try
                let! read = stream.ReadAsync(buffer.AsMemory(), cancellationToken)

                if read <> 0 then
                    failwith "Expected the connection to close."
            with
            | :? IOException
            | :? SocketException
            | :? ObjectDisposedException -> ()
        }

    let private expectClientDisconnect
        (client: TcpClient)
        (cancellationToken: CancellationToken)
        =
        task {
            try
                do! expectDisconnect (client.GetStream()) cancellationToken
            with
            | :? InvalidOperationException
            | :? ObjectDisposedException -> ()
        }

    let private expectConnectionFailure port (cancellationToken: CancellationToken) =
        task {
            use client = new TcpClient(AddressFamily.InterNetwork)

            try
                do! client.ConnectAsync(loopback, port, cancellationToken)
                failwith "Expected the connection attempt to fail."
            with :? SocketException ->
                ()
        }

    let private runWithForwarder
        startForwarder
        listenPort
        (testCancellationToken: CancellationToken)
        (body: ProxyContext -> Task<unit>)
        =
        task {
            use proxyShutdown =
                CancellationTokenSource.CreateLinkedTokenSource testCancellationToken

            let proxyTask = startForwarder proxyShutdown.Token

            let proxy =
                { ListenPort = listenPort
                  Shutdown = proxyShutdown
                  Completion = proxyTask :> Task }

            let mutable failure: exn option = None

            try
                do! body proxy
            with error ->
                failure <- Some error

            proxyShutdown.Cancel()

            try
                do! proxyTask.WaitAsync(testCancellationToken)
            with error ->
                if failure.IsNone then
                    failure <- Some error

            match failure with
            | Some error ->
                System.Runtime.ExceptionServices.ExceptionDispatchInfo.Capture(error).Throw()
                return ()
            | None -> return ()
        }

    let private runWithScriptProxy
        clientPaths
        serverPaths
        backendPort
        (testCancellationToken: CancellationToken)
        body
        =
        let listenPort = reservePort ()

        let configuration =
            createConfiguration listenPort backendPort clientPaths serverPaths

        runWithForwarder
            (fun cancellationToken ->
                ScriptTcpForwarder.runAsync
                    configuration
                    NullLogger.Instance
                    cancellationToken)
            listenPort
            testCancellationToken
            body

    let private compilePlugin (workspace: TestWorkspace) path =
        task {
            let! result =
                workspace.Compiler.CompileAndLoadAsync(path, CancellationToken.None)

            match result with
            | Compiled(plugin, _) -> return plugin
            | Failed diagnostics ->
                let details =
                    diagnostics
                    |> Array.map _.Message
                    |> String.concat Environment.NewLine

                return failwithf "Test script did not compile:%s%s" Environment.NewLine details
        }

    let private runWithCompiledProxy
        (clientPlugins: CompiledScriptProcessor array)
        (serverPlugins: CompiledScriptProcessor array)
        backendPort
        (testCancellationToken: CancellationToken)
        body
        =
        let listenPort = reservePort ()
        let configuration = createConfiguration listenPort backendPort Array.empty Array.empty

        runWithForwarder
            (fun cancellationToken ->
                TcpForwarder.runWithOptionalProcessorPipelinesAsync
                    configuration
                    (fun connectionState ->
                        let publicConnectionState =
                            ConnectionStateAdapter(connectionState) :> IConnectionState

                        { ClientToBackend =
                            clientPlugins
                            |> Array.map (fun plugin ->
                                plugin.CreateProcessor(publicConnectionState))
                          BackendToClient =
                            serverPlugins
                            |> Array.map (fun plugin ->
                                plugin.CreateProcessor(publicConnectionState)) })
                    NullLogger.Instance
                    cancellationToken)
            listenPort
            testCancellationToken
            body

    let private bytes (text: string) = Encoding.ASCII.GetBytes text
    let private text (value: byte array) = Encoding.ASCII.GetString value

    let private requireType (assembly: Assembly) (name: string) =
        let simpleName =
            name.Split('.') |> Array.last

        assembly.GetTypes()
        |> Array.tryFind (fun value -> value.FullName = name || value.Name = simpleName)
        |> Option.defaultWith (fun () -> failwithf "Type '%s' was not loaded." name)

    let private requireProperty (value: Type) name =
        value.GetProperty(name, BindingFlags.Public ||| BindingFlags.Static)
        |> Option.ofObj
        |> Option.defaultWith (fun () -> failwithf "Property '%s.%s' was not found." value.FullName name)

    let private requireMethod (value: Type) name =
        value.GetMethod(name, BindingFlags.Public ||| BindingFlags.Static)
        |> Option.ofObj
        |> Option.defaultWith (fun () -> failwithf "Method '%s.%s' was not found." value.FullName name)

    let private requireStaticTask (value: Type) propertyName =
        match (requireProperty value propertyName).GetValue(null) with
        | :? Task as task -> task
        | _ -> failwithf "Property '%s.%s' was not a Task." value.FullName propertyName

    let private staticProperty<'value> (value: Type) propertyName =
        match (requireProperty value propertyName).GetValue(null) with
        | :? 'value as propertyValue -> propertyValue
        | _ ->
            failwithf
                "Property '%s.%s' had an unexpected value."
                value.FullName
                propertyName

    let private generatedTransformSource moduleName delta =
        String.concat
            Environment.NewLine
            [ $"module {moduleName}"
              ""
              "open System"
              "open System.Threading.Tasks"
              "open PipeL4.Plugins"
              ""
              "type Processor() ="
              "    interface IByteStreamProcessor with"
              "        member _.ProcessAsync(input, output, cancellationToken) ="
              "            cancellationToken.ThrowIfCancellationRequested()"
              "            if not input.Bytes.IsEmpty then"
              "                let generated = Array.zeroCreate<byte> (int input.Bytes.Length)"
              "                let mutable offset = 0"
              "                for segment in input.Bytes do"
              "                    segment.Span.CopyTo(generated.AsSpan(offset))"
              "                    offset <- offset + segment.Length"
              $"                for index in 0 .. generated.Length - 1 do generated[index] <- generated[index] + {delta}uy"
              "                generated.AsSpan().CopyTo(output.GetSpan(generated.Length))"
              "                output.Advance(generated.Length)"
              "            ValueTask<ProcessorResult>({ RetainedLength = 0L })" ]

    let rec private exceptionTree (error: exn) =
        seq {
            yield error

            match error with
            | :? AggregateException as aggregate ->
                for inner in aggregate.InnerExceptions do
                    yield! exceptionTree inner
            | _ ->
                match error.InnerException |> Option.ofObj with
                | Some inner -> yield! exceptionTree inner
                | None -> ()
        }

    let private wrapperSource moduleName left right =
        String.concat
            Environment.NewLine
            [ $"module {moduleName}"
              ""
              "open System"
              "open System.Threading.Tasks"
              "open PipeL4.Plugins"
              ""
              "type Processor() ="
              "    interface IByteStreamProcessor with"
              "        member _.ProcessAsync(input, output, _) ="
              "            if input.IsCompleted then"
              $"                let left = [| {left}uy |]"
              "                left.AsSpan().CopyTo(output.GetSpan(1))"
              "                output.Advance(1)"
              "                output.AppendInput(0L, input.Bytes.Length)"
              $"                let right = [| {right}uy |]"
              "                right.AsSpan().CopyTo(output.GetSpan(1))"
              "                output.Advance(1)"
              "                ValueTask<ProcessorResult>({ RetainedLength = 0L })"
              "            else"
              "                ValueTask<ProcessorResult>({ RetainedLength = input.Bytes.Length })" ]

    [<Fact>]
    let ``script-aware forwarder preserves raw forwarding when no plugins are configured`` () =
        task {
            use timeout = new CancellationTokenSource(20_000)
            use backendListener = new TcpListener(loopback, 0)
            backendListener.Start()

            do!
                runWithScriptProxy
                    Array.empty
                    Array.empty
                    (endpointPort backendListener)
                    timeout.Token
                    (fun proxy ->
                        task {
                            let acceptBackend =
                                backendListener.AcceptTcpClientAsync(timeout.Token).AsTask()

                            use! client = connectEventually proxy timeout.Token
                            use! backend = acceptBackend
                            let payload = bytes "raw-without-plugins"
                            let receive =
                                readExactly (backend.GetStream()) payload.Length timeout.Token

                            do! client.GetStream().WriteAsync(payload, timeout.Token)
                            let! received = receive
                            received |> should equal payload
                        })
        }

    [<Fact>]
    let ``real scripts transform both directions concurrently and independently`` () =
        task {
            use timeout = new CancellationTokenSource(30_000)
            use workspace = new TestWorkspace()
            use backendListener = new TcpListener(loopback, 0)
            backendListener.Start()

            let clientPath =
                workspace.WriteSource(
                    "ClientTransform.fsx",
                    generatedTransformSource "ClientTransform" 1
                )

            let serverPath =
                workspace.WriteSource(
                    "ServerTransform.fsx",
                    generatedTransformSource "ServerTransform" 2
                )

            do!
                runWithScriptProxy
                    [| clientPath |]
                    [| serverPath |]
                    (endpointPort backendListener)
                    timeout.Token
                    (fun proxy ->
                        task {
                            let acceptBackend =
                                backendListener.AcceptTcpClientAsync(timeout.Token).AsTask()

                            use! client = connectEventually proxy timeout.Token
                            use! backend = acceptBackend
                            let fromClient = [| 1uy; 2uy; 3uy; 4uy |]
                            let fromBackend = [| 10uy; 20uy; 30uy |]
                            let clientStream = client.GetStream()
                            let backendStream = backend.GetStream()

                            let receiveAtBackend =
                                readExactly backendStream fromClient.Length timeout.Token

                            let receiveAtClient =
                                readExactly clientStream fromBackend.Length timeout.Token

                            do!
                                Task.WhenAll(
                                    clientStream.WriteAsync(fromClient, timeout.Token).AsTask(),
                                    backendStream.WriteAsync(fromBackend, timeout.Token).AsTask()
                                )

                            let! receivedAtBackend = receiveAtBackend
                            let! receivedAtClient = receiveAtClient
                            receivedAtBackend |> should equal [| 2uy; 3uy; 4uy; 5uy |]
                            receivedAtClient |> should equal [| 12uy; 22uy; 32uy |]
                        })
        }

    [<Fact>]
    let ``configured script stages execute in argument order`` () =
        task {
            use timeout = new CancellationTokenSource(30_000)
            use workspace = new TestWorkspace()
            use backendListener = new TcpListener(loopback, 0)
            backendListener.Start()

            let anglePath =
                workspace.WriteSource(
                    "Angle.fsx",
                    wrapperSource "Angle" (int (byte '<')) (int (byte '>'))
                )

            let squarePath =
                workspace.WriteSource(
                    "Square.fsx",
                    wrapperSource "Square" (int (byte '[')) (int (byte ']'))
                )

            do!
                runWithScriptProxy
                    [| anglePath; squarePath |]
                    Array.empty
                    (endpointPort backendListener)
                    timeout.Token
                    (fun proxy ->
                        task {
                            let acceptBackend =
                                backendListener.AcceptTcpClientAsync(timeout.Token).AsTask()

                            use! client = connectEventually proxy timeout.Token
                            use! backend = acceptBackend
                            let clientStream = client.GetStream()
                            let backendStream = backend.GetStream()
                            let expected = bytes "[<X>]"
                            let receive = readExactly backendStream expected.Length timeout.Token

                            do! clientStream.WriteAsync(bytes "X", timeout.Token)
                            client.Client.Shutdown(SocketShutdown.Send)

                            let! received = receive
                            received |> should equal expected
                            do! expectEndOfStream backendStream timeout.Token
                        })
        }

    [<Fact>]
    let ``real script retains segmented input and emits mixed runtime-owned output`` () =
        task {
            use timeout = new CancellationTokenSource(30_000)
            use workspace = new TestWorkspace()
            use backendListener = new TcpListener(loopback, 0)
            backendListener.Start()

            let source =
                """module RetainedMixed

open System
open System.Threading.Tasks
open PipeL4.Plugins

type Processor() =
    interface IByteStreamProcessor with
        member _.ProcessAsync(input, output, _) =
            if not input.IsCompleted && input.Bytes.Length < 4L then
                if input.Bytes.Length = 1L then
                    let ready = [| 63uy |]
                    ready.AsSpan().CopyTo(output.GetSpan(1))
                    output.Advance(1)
                ValueTask<ProcessorResult>({ RetainedLength = input.Bytes.Length })
            elif input.Bytes.IsEmpty then
                ValueTask<ProcessorResult>({ RetainedLength = 0L })
            else
                output.AppendInput(0L, 1L)
                let generated = [| 88uy; 89uy |]
                generated.AsSpan().CopyTo(output.GetSpan(generated.Length))
                output.Advance(generated.Length)
                generated.AsSpan().Clear()
                output.AppendInput(input.Bytes.Length - 1L, 1L)
                ValueTask<ProcessorResult>({ RetainedLength = 0L })
"""

            let scriptPath = workspace.WriteSource("RetainedMixed.fsx", source)

            do!
                runWithScriptProxy
                    [| scriptPath |]
                    Array.empty
                    (endpointPort backendListener)
                    timeout.Token
                    (fun proxy ->
                        task {
                            let acceptBackend =
                                backendListener.AcceptTcpClientAsync(timeout.Token).AsTask()

                            use! client = connectEventually proxy timeout.Token
                            use! backend = acceptBackend
                            let clientStream = client.GetStream()
                            let backendStream = backend.GetStream()

                            do! clientStream.WriteAsync(bytes "A", timeout.Token)
                            let! ready = readExactly backendStream 1 timeout.Token
                            ready |> text |> should equal "?"

                            do! clientStream.WriteAsync(bytes "BCD", timeout.Token)
                            let! received = readExactly backendStream 4 timeout.Token
                            received |> text |> should equal "AXYD"
                        })
        }

    [<Fact>]
    let ``real script can emit and retain the same input range`` () =
        task {
            use timeout = new CancellationTokenSource(30_000)
            use workspace = new TestWorkspace()
            use backendListener = new TcpListener(loopback, 0)
            backendListener.Start()

            let source =
                """module EmitRetain

open System.Threading.Tasks
open PipeL4.Plugins

type Processor() =
    interface IByteStreamProcessor with
        member _.ProcessAsync(input, output, _) =
            if input.IsCompleted then
                output.AppendInput(0L, input.Bytes.Length)
                ValueTask<ProcessorResult>({ RetainedLength = 0L })
            elif input.Bytes.Length = 1L then
                output.AppendInput(0L, 1L)
                ValueTask<ProcessorResult>({ RetainedLength = 1L })
            else
                output.AppendInput(0L, input.Bytes.Length)
                ValueTask<ProcessorResult>({ RetainedLength = 0L })
"""

            let scriptPath = workspace.WriteSource("EmitRetain.fsx", source)

            do!
                runWithScriptProxy
                    [| scriptPath |]
                    Array.empty
                    (endpointPort backendListener)
                    timeout.Token
                    (fun proxy ->
                        task {
                            let acceptBackend =
                                backendListener.AcceptTcpClientAsync(timeout.Token).AsTask()

                            use! client = connectEventually proxy timeout.Token
                            use! backend = acceptBackend
                            let clientStream = client.GetStream()
                            let backendStream = backend.GetStream()

                            do! clientStream.WriteAsync(bytes "A", timeout.Token)
                            let! first = readExactly backendStream 1 timeout.Token
                            first |> text |> should equal "A"

                            do! clientStream.WriteAsync(bytes "B", timeout.Token)
                            let! second = readExactly backendStream 2 timeout.Token
                            second |> text |> should equal "AB"
                        })
        }

    [<Fact>]
    let ``real script can release a prefix while retaining a partial suffix`` () =
        task {
            use timeout = new CancellationTokenSource(30_000)
            use workspace = new TestWorkspace()
            use backendListener = new TcpListener(loopback, 0)
            backendListener.Start()

            let source =
                """module PartialRetention

open System
open System.Threading.Tasks
open PipeL4.Plugins

type Processor() =
    interface IByteStreamProcessor with
        member _.ProcessAsync(input, output, _) =
            let mutable hasDelimiter = false
            for segment in input.Bytes do
                if segment.Span.IndexOf(124uy) >= 0 then
                    hasDelimiter <- true

            if input.IsCompleted || hasDelimiter then
                output.AppendInput(0L, input.Bytes.Length)
                ValueTask<ProcessorResult>({ RetainedLength = 0L })
            elif input.Bytes.Length < 2L then
                ValueTask<ProcessorResult>({ RetainedLength = input.Bytes.Length })
            else
                output.AppendInput(0L, input.Bytes.Length - 1L)
                ValueTask<ProcessorResult>({ RetainedLength = 1L })
"""

            let scriptPath = workspace.WriteSource("PartialRetention.fsx", source)

            do!
                runWithScriptProxy
                    [| scriptPath |]
                    Array.empty
                    (endpointPort backendListener)
                    timeout.Token
                    (fun proxy ->
                        task {
                            let acceptBackend =
                                backendListener.AcceptTcpClientAsync(timeout.Token).AsTask()

                            use! client = connectEventually proxy timeout.Token
                            use! backend = acceptBackend
                            let clientStream = client.GetStream()
                            let backendStream = backend.GetStream()

                            do! clientStream.WriteAsync(bytes "AB", timeout.Token)
                            let! prefix = readExactly backendStream 1 timeout.Token
                            prefix |> text |> should equal "A"

                            do! clientStream.WriteAsync(bytes "C|", timeout.Token)
                            let! suffix = readExactly backendStream 3 timeout.Token
                            suffix |> text |> should equal "BC|"
                        })
        }

    [<Fact>]
    let ``retained script input is finalized before TCP half-close`` () =
        task {
            use timeout = new CancellationTokenSource(30_000)
            use workspace = new TestWorkspace()
            use backendListener = new TcpListener(loopback, 0)
            backendListener.Start()

            let source =
                """module FinalizeAtEof

open System
open System.Collections.Concurrent
open System.Threading.Tasks
open PipeL4.Plugins

type Processor(connectionState: IConnectionState) =
    interface IByteStreamProcessor with
        member _.ProcessAsync(input, output, _) =
            if input.IsCompleted then
                let shared =
                    connectionState.TryGet<ConcurrentDictionary<string, int64>>("eof-state")
                    |> Option.defaultWith (fun () -> invalidOp "EOF state was missing.")

                if shared["retained"] <> input.Bytes.Length then
                    invalidOp "The retained length did not survive until EOF."

                shared["finalized"] <- 1L
                connectionState.GetOrAdd("eof-published", fun () -> obj()) |> ignore
                output.AppendInput(0L, input.Bytes.Length)
                let marker = [| 33uy |]
                marker.AsSpan().CopyTo(output.GetSpan(1))
                output.Advance(1)
                ValueTask<ProcessorResult>({ RetainedLength = 0L })
            else
                let shared =
                    connectionState.GetOrAdd(
                        "eof-state",
                        fun () -> ConcurrentDictionary<string, int64>()
                    )

                shared["retained"] <- input.Bytes.Length
                ValueTask<ProcessorResult>({ RetainedLength = input.Bytes.Length })
"""

            let scriptPath = workspace.WriteSource("FinalizeAtEof.fsx", source)

            do!
                runWithScriptProxy
                    [| scriptPath |]
                    Array.empty
                    (endpointPort backendListener)
                    timeout.Token
                    (fun proxy ->
                        task {
                            let acceptBackend =
                                backendListener.AcceptTcpClientAsync(timeout.Token).AsTask()

                            use! client = connectEventually proxy timeout.Token
                            use! backend = acceptBackend
                            let clientStream = client.GetStream()
                            let backendStream = backend.GetStream()

                            do! clientStream.WriteAsync(bytes "final", timeout.Token)
                            client.Client.Shutdown(SocketShutdown.Send)

                            let! received = readExactly backendStream 6 timeout.Token
                            received |> text |> should equal "final!"
                            do! expectEndOfStream backendStream timeout.Token
                        })
        }

    [<Fact>]
    let ``asynchronous script does not block the opposite TCP direction`` () =
        task {
            use timeout = new CancellationTokenSource(30_000)
            use workspace = new TestWorkspace()
            use backendListener = new TcpListener(loopback, 0)
            backendListener.Start()

            let source =
                """module AsyncGatePlugin

open System.Threading
open System.Threading.Tasks
open PipeL4.Plugins

type Coordination private () =
    static let started = TaskCompletionSource<unit>(TaskCreationOptions.RunContinuationsAsynchronously)
    static let release = TaskCompletionSource<unit>(TaskCreationOptions.RunContinuationsAsynchronously)
    static member Started = started.Task
    static member SignalStarted() = started.TrySetResult(()) |> ignore
    static member Release() = release.TrySetResult(()) |> ignore
    static member WaitAsync(cancellationToken: CancellationToken) = release.Task.WaitAsync(cancellationToken)

type Processor() =
    interface IByteStreamProcessor with
        member _.ProcessAsync(input, output, cancellationToken) =
            if input.IsCompleted then
                ValueTask<ProcessorResult>({ RetainedLength = 0L })
            else
                ValueTask<ProcessorResult>(task {
                    Coordination.SignalStarted()
                    do! Coordination.WaitAsync(cancellationToken)
                    output.AppendInput(0L, input.Bytes.Length)
                    return { RetainedLength = 0L }
                })
"""

            let scriptPath = workspace.WriteSource("AsyncGate.fsx", source)
            let! plugin = compilePlugin workspace scriptPath

            let coordination = requireType plugin.Assembly "AsyncGatePlugin.Coordination"
            let started = requireStaticTask coordination "Started"
            let release = requireMethod coordination "Release"

            do!
                runWithCompiledProxy
                    [| plugin |]
                    Array.empty
                    (endpointPort backendListener)
                    timeout.Token
                    (fun proxy ->
                        task {
                            let acceptBackend =
                                backendListener.AcceptTcpClientAsync(timeout.Token).AsTask()

                            use! client = connectEventually proxy timeout.Token
                            use! backend = acceptBackend
                            let clientStream = client.GetStream()
                            let backendStream = backend.GetStream()
                            let blocked = bytes "blocked"
                            let reverse = bytes "reverse"
                            let receiveBlocked =
                                readExactly backendStream blocked.Length timeout.Token

                            do! clientStream.WriteAsync(blocked, timeout.Token)
                            do! started.WaitAsync(timeout.Token)

                            let receiveReverse =
                                readExactly clientStream reverse.Length timeout.Token

                            do! backendStream.WriteAsync(reverse, timeout.Token)
                            let! receivedReverse = receiveReverse
                            receivedReverse |> should equal reverse

                            release.Invoke(null, Array.empty) |> ignore
                            let! receivedBlocked = receiveBlocked
                            receivedBlocked |> should equal blocked
                        })
        }

    [<Fact>]
    let ``cancelling the proxy cancels an asynchronous real script`` () =
        task {
            use timeout = new CancellationTokenSource(30_000)
            use workspace = new TestWorkspace()
            use backendListener = new TcpListener(loopback, 0)
            backendListener.Start()

            let source =
                """module CancelledPlugin

open System.Threading
open System.Threading.Tasks
open PipeL4.Plugins

type Coordination private () =
    static let started = TaskCompletionSource<unit>(TaskCreationOptions.RunContinuationsAsynchronously)
    static member Started = started.Task
    static member SignalStarted() = started.TrySetResult(()) |> ignore

type Processor() =
    interface IByteStreamProcessor with
        member _.ProcessAsync(input, _, cancellationToken) =
            if input.IsCompleted then
                ValueTask<ProcessorResult>({ RetainedLength = 0L })
            else
                ValueTask<ProcessorResult>(task {
                    Coordination.SignalStarted()
                    do! Task.Delay(Timeout.Infinite, cancellationToken)
                    return { RetainedLength = input.Bytes.Length }
                })
"""

            let scriptPath = workspace.WriteSource("Cancelled.fsx", source)
            let! plugin = compilePlugin workspace scriptPath

            let coordination = requireType plugin.Assembly "CancelledPlugin.Coordination"
            let started = requireStaticTask coordination "Started"

            do!
                runWithCompiledProxy
                    [| plugin |]
                    Array.empty
                    (endpointPort backendListener)
                    timeout.Token
                    (fun proxy ->
                        task {
                            let acceptBackend =
                                backendListener.AcceptTcpClientAsync(timeout.Token).AsTask()

                            use! client = connectEventually proxy timeout.Token
                            use! backend = acceptBackend

                            do! client.GetStream().WriteAsync(bytes "wait", timeout.Token)
                            do! started.WaitAsync(timeout.Token)

                            proxy.Shutdown.Cancel()
                            do! proxy.Completion.WaitAsync(timeout.Token)
                            do! expectDisconnect (client.GetStream()) timeout.Token
                            do! expectDisconnect (backend.GetStream()) timeout.Token
                            do! expectConnectionFailure proxy.ListenPort timeout.Token
                        })
        }

    [<Fact>]
    let ``runtime script failure is isolated to its connection`` () =
        task {
            use timeout = new CancellationTokenSource(30_000)
            use workspace = new TestWorkspace()
            use backendListener = new TcpListener(loopback, 0)
            backendListener.Start()

            let source =
                """module FailingPlugin

open System
open System.Threading.Tasks
open PipeL4.Plugins

type Processor() =
    interface IByteStreamProcessor with
        member _.ProcessAsync(input, output, _) =
            if not input.Bytes.IsEmpty && input.Bytes.FirstSpan[0] = 255uy then
                invalidOp "script processor failed"
            output.AppendInput(0L, input.Bytes.Length)
            ValueTask<ProcessorResult>({ RetainedLength = 0L })
"""

            let scriptPath = workspace.WriteSource("Failing.fsx", source)

            do!
                runWithScriptProxy
                    [| scriptPath |]
                    Array.empty
                    (endpointPort backendListener)
                    timeout.Token
                    (fun proxy ->
                        task {
                            let acceptHealthyBackend =
                                backendListener.AcceptTcpClientAsync(timeout.Token).AsTask()

                            use! healthyClient = connectEventually proxy timeout.Token
                            use! healthyBackend = acceptHealthyBackend

                            let acceptFailedBackend =
                                backendListener.AcceptTcpClientAsync(timeout.Token).AsTask()

                            use! failedClient = connect proxy.ListenPort timeout.Token
                            use! failedBackend = acceptFailedBackend

                            do! failedClient.GetStream().WriteAsync([| 255uy |], timeout.Token)
                            do! expectDisconnect (failedClient.GetStream()) timeout.Token
                            do! expectDisconnect (failedBackend.GetStream()) timeout.Token

                            let healthyPayload = bytes "healthy"
                            let receiveHealthy =
                                readExactly
                                    (healthyBackend.GetStream())
                                    healthyPayload.Length
                                    timeout.Token

                            do! healthyClient.GetStream().WriteAsync(healthyPayload, timeout.Token)
                            let! receivedHealthy = receiveHealthy
                            receivedHealthy |> should equal healthyPayload

                            let acceptNewBackend =
                                backendListener.AcceptTcpClientAsync(timeout.Token).AsTask()

                            use! newClient = connect proxy.ListenPort timeout.Token
                            use! newBackend = acceptNewBackend
                            let newPayload = bytes "new-connection"
                            let receiveNew =
                                readExactly
                                    (newBackend.GetStream())
                                    newPayload.Length
                                    timeout.Token

                            do! newClient.GetStream().WriteAsync(newPayload, timeout.Token)
                            let! receivedNew = receiveNew
                            receivedNew |> should equal newPayload
                        })
        }

    [<Fact>]
    let ``invocation and disposal failures are both logged with script stage context`` () =
        task {
            use timeout = new CancellationTokenSource(30_000)
            use workspace = new TestWorkspace()
            use backendListener = new TcpListener(loopback, 0)
            backendListener.Start()

            let source =
                """module InvocationAndDisposalFailure

open System
open System.Collections.Concurrent
open System.Threading
open System.Threading.Tasks
open PipeL4.Plugins

type Coordination private () =
    static let events = ConcurrentQueue<string>()
    static let allStateDisposals = TaskCompletionSource<unit>(TaskCreationOptions.RunContinuationsAsynchronously)
    static let mutable stateDisposals = 0
    static member RecordStateDisposal(name: string) =
        events.Enqueue(name)
        if Interlocked.Increment(&stateDisposals) = 3 then
            allStateDisposals.TrySetResult(()) |> ignore
    static member Events = events.ToArray()
    static member AllStateDisposals = allStateDisposals.Task

type StateValue(name: string, failure: exn option) =
    interface IAsyncDisposable with
        member _.DisposeAsync() =
            Coordination.RecordStateDisposal(name)
            match failure with
            | Some error -> ValueTask(Task.FromException(error))
            | None -> ValueTask()

type Processor(connectionState: IConnectionState) =
    let mutable initialized = false

    interface IByteStreamProcessor with
        member _.ProcessAsync(input, output, _) =
            if not initialized then
                connectionState.GetOrAdd(
                    "state-first",
                    fun () -> StateValue("first", Some(InvalidOperationException("first state disposal failed")))
                )
                |> ignore

                connectionState.GetOrAdd(
                    "state-middle",
                    fun () -> StateValue("middle", None)
                )
                |> ignore

                connectionState.GetOrAdd(
                    "state-last",
                    fun () -> StateValue("last", Some(InvalidOperationException("last state disposal failed")))
                )
                |> ignore

                initialized <- true

            output.AppendInput(0L, input.Bytes.Length)
            let generated = [| 88uy |]
            generated.AsSpan().CopyTo(output.GetSpan(1))
            output.Advance(1)
            invalidOp "invocation failed"

    interface IAsyncDisposable with
        member _.DisposeAsync() =
            ValueTask(Task.FromException(InvalidOperationException("processor disposal failed")))
"""

            let scriptPath = workspace.WriteSource("InvocationAndDisposalFailure.fsx", source)
            let listenPort = reservePort ()

            let configuration =
                createConfiguration
                    listenPort
                    (endpointPort backendListener)
                    [| scriptPath |]
                    Array.empty

            let logger = RecordingLogger()
            let! plugin = compilePlugin workspace scriptPath
            let coordination = requireType plugin.Assembly "InvocationAndDisposalFailure.Coordination"
            let allStateDisposals = requireStaticTask coordination "AllStateDisposals"
            let compiler _ _ = Task.FromResult(Compiled(plugin, Array.empty))

            do!
                runWithForwarder
                    (fun cancellationToken ->
                        ScriptTcpForwarder.runWithCompilerAsync
                            compiler
                            configuration
                            logger
                            cancellationToken)
                    listenPort
                    timeout.Token
                    (fun proxy ->
                        task {
                            let acceptFailedBackend =
                                backendListener.AcceptTcpClientAsync(timeout.Token).AsTask()

                            use! failedClient = connectEventually proxy timeout.Token
                            use! failedBackend = acceptFailedBackend
                            let failedClientStream = failedClient.GetStream()
                            let failedBackendStream = failedBackend.GetStream()
                            do! failedClientStream.WriteAsync(bytes "fail", timeout.Token)
                            do! expectDisconnect failedBackendStream timeout.Token
                            do! expectDisconnect failedClientStream timeout.Token
                            do! allStateDisposals.WaitAsync(timeout.Token)
                            do! logger.WaitForFailureCountAsync(2, timeout.Token)

                            let! logged = logger.FirstFailure.WaitAsync(timeout.Token)
                            logged |> should be ofExactType<AggregateException>

                            let failures =
                                logger.Failures
                                |> Array.collect (exceptionTree >> Seq.toArray)

                            failures
                            |> Array.exists (fun error -> error.Message = "invocation failed")
                            |> should equal true

                            for expectedMessage in
                                [| "processor disposal failed"
                                   "first state disposal failed"
                                   "last state disposal failed" |] do
                                failures
                                |> Array.exists (fun error -> error.Message = expectedMessage)
                                |> should equal true

                            staticProperty<string array> coordination "Events"
                            |> should equal [| "last"; "middle"; "first" |]

                            let contexts =
                                failures
                                |> Array.choose (function
                                    | :? ScriptProcessorFailureException as error -> Some error
                                    | _ -> None)

                            contexts.Length |> should equal 2

                            for context in contexts do
                                context.ScriptPath |> should equal (Path.GetFullPath scriptPath)
                                context.Direction |> should equal "client-to-server"
                                context.StageNumber |> should equal 1

                            let acceptNewBackend =
                                backendListener.AcceptTcpClientAsync(timeout.Token).AsTask()

                            use! newClient = connect proxy.ListenPort timeout.Token
                            use! newBackend = acceptNewBackend
                            newClient.Connected |> should equal true
                            newBackend.Connected |> should equal true
                        })
        }

    [<Fact>]
    let ``processor construction failure is connection scoped and definition remains reusable`` () =
        task {
            use timeout = new CancellationTokenSource(30_000)
            use workspace = new TestWorkspace()
            use backendListener = new TcpListener(loopback, 0)
            backendListener.Start()

            let source =
                """module ConstructionFailure

open System.Threading
open System.Threading.Tasks
open PipeL4.Plugins

type Processor() =
    static let mutable attempts = 0
    let attempt = Interlocked.Increment(&attempts)
    do if attempt = 2 then invalidOp "second construction failed"

    interface IByteStreamProcessor with
        member _.ProcessAsync(input, output, _) =
            output.AppendInput(0L, input.Bytes.Length)
            ValueTask<ProcessorResult>({ RetainedLength = 0L })
"""

            let scriptPath = workspace.WriteSource("ConstructionFailure.fsx", source)

            do!
                runWithScriptProxy
                    [| scriptPath |]
                    Array.empty
                    (endpointPort backendListener)
                    timeout.Token
                    (fun proxy ->
                        task {
                            let acceptHealthyBackend =
                                backendListener.AcceptTcpClientAsync(timeout.Token).AsTask()

                            use! healthyClient = connectEventually proxy timeout.Token
                            use! healthyBackend = acceptHealthyBackend

                            let firstPayload = bytes "first"
                            let firstReceive =
                                readExactly
                                    (healthyBackend.GetStream())
                                    firstPayload.Length
                                    timeout.Token

                            do! healthyClient.GetStream().WriteAsync(firstPayload, timeout.Token)
                            let! firstReceived = firstReceive
                            firstReceived |> should equal firstPayload

                            let acceptFailedBackend =
                                backendListener.AcceptTcpClientAsync(timeout.Token).AsTask()

                            use! failedClient = connect proxy.ListenPort timeout.Token
                            use! failedBackend = acceptFailedBackend
                            do! expectClientDisconnect failedClient timeout.Token
                            do! expectClientDisconnect failedBackend timeout.Token

                            let secondPayload = bytes "still-healthy"
                            let secondReceive =
                                readExactly
                                    (healthyBackend.GetStream())
                                    secondPayload.Length
                                    timeout.Token

                            do! healthyClient.GetStream().WriteAsync(secondPayload, timeout.Token)
                            let! secondReceived = secondReceive
                            secondReceived |> should equal secondPayload

                            let acceptRetryBackend =
                                backendListener.AcceptTcpClientAsync(timeout.Token).AsTask()

                            use! retryClient = connect proxy.ListenPort timeout.Token
                            use! retryBackend = acceptRetryBackend
                            let retryPayload = bytes "retry"
                            let retryReceive =
                                readExactly
                                    (retryBackend.GetStream())
                                    retryPayload.Length
                                    timeout.Token

                            do! retryClient.GetStream().WriteAsync(retryPayload, timeout.Token)
                            let! retryReceived = retryReceive
                            retryReceived |> should equal retryPayload
                        })
        }

    [<Fact>]
    let ``shutdown cancels a cooperatively executing synchronous script and disposes it`` () =
        task {
            use timeout = new CancellationTokenSource(30_000)
            use workspace = new TestWorkspace()
            use backendListener = new TcpListener(loopback, 0)
            backendListener.Start()

            let source =
                """module SynchronousCancellation

open System
open System.Threading
open System.Threading.Tasks
open PipeL4.Plugins

type Coordination private () =
    static let started = TaskCompletionSource<unit>(TaskCreationOptions.RunContinuationsAsynchronously)
    static let stopped = TaskCompletionSource<unit>(TaskCreationOptions.RunContinuationsAsynchronously)
    static let disposed = TaskCompletionSource<unit>(TaskCreationOptions.RunContinuationsAsynchronously)
    static member Started = started.Task
    static member Stopped = stopped.Task
    static member Disposed = disposed.Task
    static member SignalStarted() = started.TrySetResult(()) |> ignore
    static member SignalStopped() = stopped.TrySetResult(()) |> ignore
    static member SignalDisposed() = disposed.TrySetResult(()) |> ignore

type Processor() =
    interface IByteStreamProcessor with
        member _.ProcessAsync(input, _, cancellationToken) =
            if input.IsCompleted then
                ValueTask<ProcessorResult>({ RetainedLength = 0L })
            else
                Coordination.SignalStarted()

                try
                    while true do
                        cancellationToken.ThrowIfCancellationRequested()
                        Thread.SpinWait(256)
                finally
                    Coordination.SignalStopped()

                ValueTask<ProcessorResult>({ RetainedLength = 0L })

    interface IAsyncDisposable with
        member _.DisposeAsync() =
            Coordination.SignalDisposed()
            ValueTask()
"""

            let scriptPath = workspace.WriteSource("SynchronousCancellation.fsx", source)
            let! plugin = compilePlugin workspace scriptPath
            let coordination = requireType plugin.Assembly "SynchronousCancellation.Coordination"
            let started = requireStaticTask coordination "Started"
            let stopped = requireStaticTask coordination "Stopped"
            let disposed = requireStaticTask coordination "Disposed"

            do!
                runWithCompiledProxy
                    [| plugin |]
                    Array.empty
                    (endpointPort backendListener)
                    timeout.Token
                    (fun proxy ->
                        task {
                            let acceptBackend =
                                backendListener.AcceptTcpClientAsync(timeout.Token).AsTask()

                            use! client = connectEventually proxy timeout.Token
                            use! backend = acceptBackend

                            do! client.GetStream().WriteAsync(bytes "run", timeout.Token)
                            do! started.WaitAsync(timeout.Token)
                            proxy.Shutdown.Cancel()
                            do! proxy.Completion.WaitAsync(timeout.Token)
                            do! stopped.WaitAsync(timeout.Token)
                            do! disposed.WaitAsync(timeout.Token)
                            do! expectDisconnect (client.GetStream()) timeout.Token
                            do! expectDisconnect (backend.GetStream()) timeout.Token
                            do! expectConnectionFailure proxy.ListenPort timeout.Token
                        })
        }

    [<Fact>]
    let ``shutdown cancels retained generated multi-stage work in both directions`` () =
        task {
            use timeout = new CancellationTokenSource(30_000)
            use workspace = new TestWorkspace()
            use backendListener = new TcpListener(loopback, 0)
            backendListener.Start()

            let identityPath =
                workspace.WriteSource(
                    "ShutdownIdentity.fsx",
                    """module ShutdownIdentity

open System.Threading.Tasks
open PipeL4.Plugins

type Processor() =
    interface IByteStreamProcessor with
        member _.ProcessAsync(input, output, _) =
            output.AppendInput(0L, input.Bytes.Length)
            ValueTask<ProcessorResult>({ RetainedLength = 0L })
"""
                )

            let gatePath =
                workspace.WriteSource(
                    "ShutdownGate.fsx",
                    """module ShutdownGate

open System
open System.Threading
open System.Threading.Tasks
open PipeL4.Plugins

type Coordination private () =
    static let retained = TaskCompletionSource<unit>(TaskCreationOptions.RunContinuationsAsynchronously)
    static let started = TaskCompletionSource<unit>(TaskCreationOptions.RunContinuationsAsynchronously)
    static let cancelled = TaskCompletionSource<unit>(TaskCreationOptions.RunContinuationsAsynchronously)
    static let disposed = TaskCompletionSource<unit>(TaskCreationOptions.RunContinuationsAsynchronously)
    static let stateDisposalStarted = TaskCompletionSource<unit>(TaskCreationOptions.RunContinuationsAsynchronously)
    static let stateDisposalRelease = TaskCompletionSource<unit>(TaskCreationOptions.RunContinuationsAsynchronously)
    static let stateDisposed = TaskCompletionSource<unit>(TaskCreationOptions.RunContinuationsAsynchronously)
    static let mutable retainedCount = 0
    static let mutable startedCount = 0
    static let mutable cancelledCount = 0
    static let mutable disposedCount = 0
    static member Retained = retained.Task
    static member Started = started.Task
    static member Cancelled = cancelled.Task
    static member Disposed = disposed.Task
    static member StateDisposalStarted = stateDisposalStarted.Task
    static member StateDisposed = stateDisposed.Task
    static member SignalRetained() =
        if Interlocked.Increment(&retainedCount) = 2 then retained.TrySetResult(()) |> ignore
    static member SignalStarted() =
        if Interlocked.Increment(&startedCount) = 2 then started.TrySetResult(()) |> ignore
    static member SignalCancelled() =
        if Interlocked.Increment(&cancelledCount) = 2 then cancelled.TrySetResult(()) |> ignore
    static member SignalDisposed() =
        if Interlocked.Increment(&disposedCount) = 2 then disposed.TrySetResult(()) |> ignore
    static member SignalStateDisposalStarted() = stateDisposalStarted.TrySetResult(()) |> ignore
    static member ReleaseStateDisposal() = stateDisposalRelease.TrySetResult(()) |> ignore
    static member WaitForStateDisposalRelease() = stateDisposalRelease.Task
    static member SignalStateDisposed() = stateDisposed.TrySetResult(()) |> ignore

type OwnedState() =
    interface IAsyncDisposable with
        member _.DisposeAsync() =
            ValueTask(task {
                Coordination.SignalStateDisposalStarted()
                do! Coordination.WaitForStateDisposalRelease()
                Coordination.SignalStateDisposed()
            })

type Processor(connectionState: IConnectionState) =
    let mutable calls = 0
    let owned = connectionState.GetOrAdd("shutdown-owned", fun () -> OwnedState())

    interface IByteStreamProcessor with
        member _.ProcessAsync(input, output, cancellationToken) =
            if input.IsCompleted then
                ValueTask<ProcessorResult>({ RetainedLength = 0L })
            else
                calls <- calls + 1

                if calls = 1 then
                    Coordination.SignalRetained()
                    ValueTask<ProcessorResult>({ RetainedLength = input.Bytes.Length })
                else
                    output.AppendInput(0L, 1L)
                    let generated = [| 88uy |]
                    generated.AsSpan().CopyTo(output.GetSpan(1))
                    output.Advance(1)
                    output.AppendInput(input.Bytes.Length - 1L, 1L)

                    ValueTask<ProcessorResult>(task {
                        Coordination.SignalStarted()
                        try
                            do! Task.Delay(Timeout.Infinite, cancellationToken)
                        finally
                            Coordination.SignalCancelled()
                        return { RetainedLength = 0L }
                    })

    interface IAsyncDisposable with
        member _.DisposeAsync() =
            Coordination.SignalDisposed()
            ValueTask()
"""
                )

            let! identity = compilePlugin workspace identityPath
            let! gate = compilePlugin workspace gatePath
            let coordination = requireType gate.Assembly "ShutdownGate.Coordination"
            let retained = requireStaticTask coordination "Retained"
            let started = requireStaticTask coordination "Started"
            let cancelled = requireStaticTask coordination "Cancelled"
            let disposed = requireStaticTask coordination "Disposed"
            let stateDisposalStarted = requireStaticTask coordination "StateDisposalStarted"
            let stateDisposed = requireStaticTask coordination "StateDisposed"
            let releaseStateDisposal = requireMethod coordination "ReleaseStateDisposal"

            do!
                runWithCompiledProxy
                    [| identity; gate |]
                    [| identity; gate |]
                    (endpointPort backendListener)
                    timeout.Token
                    (fun proxy ->
                        task {
                            try
                                let acceptBackend =
                                    backendListener.AcceptTcpClientAsync(timeout.Token).AsTask()

                                use! client = connectEventually proxy timeout.Token
                                use! backend = acceptBackend
                                let clientStream = client.GetStream()
                                let backendStream = backend.GetStream()

                                do!
                                    Task.WhenAll(
                                        clientStream.WriteAsync(bytes "A", timeout.Token).AsTask(),
                                        backendStream.WriteAsync(bytes "B", timeout.Token).AsTask()
                                    )

                                do! retained.WaitAsync(timeout.Token)

                                do!
                                    Task.WhenAll(
                                        clientStream.WriteAsync(bytes "C", timeout.Token).AsTask(),
                                        backendStream.WriteAsync(bytes "D", timeout.Token).AsTask()
                                    )

                                do! started.WaitAsync(timeout.Token)
                                proxy.Shutdown.Cancel()
                                do! cancelled.WaitAsync(timeout.Token)
                                do! disposed.WaitAsync(timeout.Token)
                                do! stateDisposalStarted.WaitAsync(timeout.Token)
                                proxy.Completion.IsCompleted |> should equal false
                                releaseStateDisposal.Invoke(null, Array.empty) |> ignore
                                do! proxy.Completion.WaitAsync(timeout.Token)
                                stateDisposed.IsCompletedSuccessfully |> should equal true
                                do! expectDisconnect clientStream timeout.Token
                                do! expectDisconnect backendStream timeout.Token
                                do! expectConnectionFailure proxy.ListenPort timeout.Token
                            finally
                                releaseStateDisposal.Invoke(null, Array.empty) |> ignore
                        })
        }

    [<Fact>]
    let ``shutdown drains stateful async work after the opposite direction completed`` () =
        task {
            use timeout = new CancellationTokenSource(30_000)
            use workspace = new TestWorkspace()
            use backendListener = new TcpListener(loopback, 0)
            backendListener.Start()

            let completedPath =
                workspace.WriteSource(
                    "CompletedStateDirection.fsx",
                    """module CompletedStateDirection

open System.Collections.Concurrent
open System.Threading.Tasks
open PipeL4.Plugins

type Processor(connectionState: IConnectionState) =
    let shared =
        connectionState.GetOrAdd(
            "cancel-after-half-close",
            fun () -> ConcurrentDictionary<string, int64>()
        )

    interface IByteStreamProcessor with
        member _.ProcessAsync(input, output, _) =
            if not input.Bytes.IsEmpty then
                shared["request"] <- input.Bytes.Length

            output.AppendInput(0L, input.Bytes.Length)
            ValueTask<ProcessorResult>({ RetainedLength = 0L })
"""
                )

            let activePath =
                workspace.WriteSource(
                    "ActiveStateDirection.fsx",
                    """module ActiveStateDirection

open System
open System.Collections.Concurrent
open System.Threading
open System.Threading.Tasks
open PipeL4.Plugins

type Coordination private () =
    static let started = TaskCompletionSource<unit>(TaskCreationOptions.RunContinuationsAsynchronously)
    static let cancelled = TaskCompletionSource<unit>(TaskCreationOptions.RunContinuationsAsynchronously)
    static let processorDisposed = TaskCompletionSource<unit>(TaskCreationOptions.RunContinuationsAsynchronously)
    static let stateDisposalStarted = TaskCompletionSource<unit>(TaskCreationOptions.RunContinuationsAsynchronously)
    static let stateDisposalRelease = TaskCompletionSource<unit>(TaskCreationOptions.RunContinuationsAsynchronously)
    static let stateDisposed = TaskCompletionSource<unit>(TaskCreationOptions.RunContinuationsAsynchronously)
    static member Started = started.Task
    static member Cancelled = cancelled.Task
    static member ProcessorDisposed = processorDisposed.Task
    static member StateDisposalStarted = stateDisposalStarted.Task
    static member StateDisposed = stateDisposed.Task
    static member SignalStarted() = started.TrySetResult(()) |> ignore
    static member SignalCancelled() = cancelled.TrySetResult(()) |> ignore
    static member SignalProcessorDisposed() = processorDisposed.TrySetResult(()) |> ignore
    static member SignalStateDisposalStarted() = stateDisposalStarted.TrySetResult(()) |> ignore
    static member ReleaseStateDisposal() = stateDisposalRelease.TrySetResult(()) |> ignore
    static member WaitForStateDisposalRelease() = stateDisposalRelease.Task
    static member SignalStateDisposed() = stateDisposed.TrySetResult(()) |> ignore

type OwnedState() =
    interface IAsyncDisposable with
        member _.DisposeAsync() =
            ValueTask(task {
                Coordination.SignalStateDisposalStarted()
                do! Coordination.WaitForStateDisposalRelease()
                Coordination.SignalStateDisposed()
            })

type Processor(connectionState: IConnectionState) =
    let shared =
        connectionState.GetOrAdd(
            "cancel-after-half-close",
            fun () -> ConcurrentDictionary<string, int64>()
        )

    let owned = connectionState.GetOrAdd("cancel-after-half-close-owned", fun () -> OwnedState())

    interface IByteStreamProcessor with
        member _.ProcessAsync(input, _, cancellationToken) =
            if input.IsCompleted then
                ValueTask<ProcessorResult>({ RetainedLength = 0L })
            else
                ValueTask<ProcessorResult>(task {
                    if not (shared.ContainsKey("request")) then
                        invalidOp "The completed direction's state mutation was missing."

                    Coordination.SignalStarted()

                    try
                        do! Task.Delay(Timeout.Infinite, cancellationToken)
                    finally
                        Coordination.SignalCancelled()

                    return { RetainedLength = 0L }
                })

    interface IAsyncDisposable with
        member _.DisposeAsync() =
            Coordination.SignalProcessorDisposed()
            ValueTask()
"""
                )

            let! completedPlugin = compilePlugin workspace completedPath
            let! activePlugin = compilePlugin workspace activePath
            let coordination = requireType activePlugin.Assembly "ActiveStateDirection.Coordination"
            let started = requireStaticTask coordination "Started"
            let cancelled = requireStaticTask coordination "Cancelled"
            let processorDisposed = requireStaticTask coordination "ProcessorDisposed"
            let stateDisposalStarted = requireStaticTask coordination "StateDisposalStarted"
            let stateDisposed = requireStaticTask coordination "StateDisposed"
            let releaseStateDisposal = requireMethod coordination "ReleaseStateDisposal"

            do!
                runWithCompiledProxy
                    [| completedPlugin |]
                    [| activePlugin |]
                    (endpointPort backendListener)
                    timeout.Token
                    (fun proxy ->
                        task {
                            try
                                let acceptBackend =
                                    backendListener.AcceptTcpClientAsync(timeout.Token).AsTask()

                                use! client = connectEventually proxy timeout.Token
                                use! backend = acceptBackend
                                let clientStream = client.GetStream()
                                let backendStream = backend.GetStream()
                                let request = bytes "complete-first"
                                do! clientStream.WriteAsync(request, timeout.Token)
                                let! forwarded = readExactly backendStream request.Length timeout.Token
                                forwarded |> should equal request

                                client.Client.Shutdown(SocketShutdown.Send)
                                do! expectEndOfStream backendStream timeout.Token

                                do! backendStream.WriteAsync(bytes "await-cancellation", timeout.Token)
                                do! started.WaitAsync(timeout.Token)
                                proxy.Shutdown.Cancel()
                                do! cancelled.WaitAsync(timeout.Token)
                                do! processorDisposed.WaitAsync(timeout.Token)
                                do! stateDisposalStarted.WaitAsync(timeout.Token)
                                proxy.Completion.IsCompleted |> should equal false
                                releaseStateDisposal.Invoke(null, Array.empty) |> ignore
                                do! proxy.Completion.WaitAsync(timeout.Token)
                                stateDisposed.IsCompletedSuccessfully |> should equal true
                                do! expectDisconnect clientStream timeout.Token
                                do! expectDisconnect backendStream timeout.Token
                            finally
                                releaseStateDisposal.Invoke(null, Array.empty) |> ignore
                        })
        }

    [<Fact>]
    let ``final EOF script failure closes only its connection and emits no staged output`` () =
        task {
            use timeout = new CancellationTokenSource(30_000)
            use workspace = new TestWorkspace()
            use backendListener = new TcpListener(loopback, 0)
            backendListener.Start()

            let source =
                """module FinalEofFailure

open System
open System.Threading.Tasks
open PipeL4.Plugins

type Processor() =
    interface IByteStreamProcessor with
        member _.ProcessAsync(input, output, _) =
            if input.IsCompleted then
                output.AppendInput(0L, input.Bytes.Length)
                let marker = [| 33uy |]
                marker.AsSpan().CopyTo(output.GetSpan(1))
                output.Advance(1)

                if not input.Bytes.IsEmpty && input.Bytes.FirstSpan[0] = 255uy then
                    invalidOp "final EOF failed"

                ValueTask<ProcessorResult>({ RetainedLength = 0L })
            else
                ValueTask<ProcessorResult>({ RetainedLength = input.Bytes.Length })
"""

            let scriptPath = workspace.WriteSource("FinalEofFailure.fsx", source)

            do!
                runWithScriptProxy
                    [| scriptPath |]
                    Array.empty
                    (endpointPort backendListener)
                    timeout.Token
                    (fun proxy ->
                        task {
                            let acceptHealthyBackend =
                                backendListener.AcceptTcpClientAsync(timeout.Token).AsTask()

                            use! healthyClient = connectEventually proxy timeout.Token
                            use! healthyBackend = acceptHealthyBackend
                            let healthyClientStream = healthyClient.GetStream()
                            let healthyBackendStream = healthyBackend.GetStream()

                            do! healthyClientStream.WriteAsync(bytes "ok", timeout.Token)
                            healthyClient.Client.Shutdown(SocketShutdown.Send)
                            let! finalized = readExactly healthyBackendStream 3 timeout.Token
                            finalized |> text |> should equal "ok!"
                            do! expectEndOfStream healthyBackendStream timeout.Token

                            let acceptFailedBackend =
                                backendListener.AcceptTcpClientAsync(timeout.Token).AsTask()

                            use! failedClient = connect proxy.ListenPort timeout.Token
                            use! failedBackend = acceptFailedBackend
                            let failedClientStream = failedClient.GetStream()
                            let failedBackendStream = failedBackend.GetStream()
                            do! failedClientStream.WriteAsync([| 255uy |], timeout.Token)
                            failedClient.Client.Shutdown(SocketShutdown.Send)
                            do! expectDisconnect failedBackendStream timeout.Token
                            do! expectDisconnect failedClientStream timeout.Token

                            let reverse = bytes "opposite-still-active"
                            let receiveReverse =
                                readExactly healthyClientStream reverse.Length timeout.Token

                            do! healthyBackendStream.WriteAsync(reverse, timeout.Token)
                            healthyBackend.Client.Shutdown(SocketShutdown.Send)
                            let! receivedReverse = receiveReverse
                            receivedReverse |> should equal reverse
                            do! expectEndOfStream healthyClientStream timeout.Token

                            let acceptNewBackend =
                                backendListener.AcceptTcpClientAsync(timeout.Token).AsTask()

                            use! newClient = connect proxy.ListenPort timeout.Token
                            use! newBackend = acceptNewBackend
                            do! newClient.GetStream().WriteAsync(bytes "new", timeout.Token)
                            newClient.Client.Shutdown(SocketShutdown.Send)
                            let! newBytes = readExactly (newBackend.GetStream()) 4 timeout.Token
                            newBytes |> text |> should equal "new!"
                        })
        }

    [<Fact>]
    let ``real script state factories execute concurrently across connections`` () =
        task {
            use timeout = new CancellationTokenSource(30_000)
            use workspace = new TestWorkspace()
            use backendListener = new TcpListener(loopback, 0)
            backendListener.Start()

            let source =
                """module ConcurrentEntryPlugin

open System.Threading
open System.Threading.Tasks
open PipeL4.Plugins

type Coordination private () =
    static let allStarted = TaskCompletionSource<unit>(TaskCreationOptions.RunContinuationsAsynchronously)
    static let release = new ManualResetEventSlim(false)
    static let mutable started = 0

    static member AllStarted = allStarted.Task

    static member SignalStarted() =
        if Interlocked.Increment(&started) = 4 then
            allStarted.TrySetResult(()) |> ignore

    static member Release() = release.Set()

    static member Create(cancellationToken: CancellationToken) =
        Coordination.SignalStarted()
        release.Wait(cancellationToken)
        obj()

type Processor(connectionState: IConnectionState) =
    interface IByteStreamProcessor with
        member _.ProcessAsync(input, output, cancellationToken) =
            if input.IsCompleted then
                ValueTask<ProcessorResult>({ RetainedLength = 0L })
            else
                let value =
                    connectionState.GetOrAdd(
                        "concurrent-entry",
                        fun () -> Coordination.Create(cancellationToken)
                    )

                if obj.ReferenceEquals(value, null) then
                    invalidOp "The connection-state factory returned null."

                output.AppendInput(0L, input.Bytes.Length)
                ValueTask<ProcessorResult>({ RetainedLength = 0L })
"""

            let scriptPath = workspace.WriteSource("ConcurrentEntry.fsx", source)
            let! plugin = compilePlugin workspace scriptPath
            let coordination =
                requireType plugin.Assembly "ConcurrentEntryPlugin.Coordination"

            let allStarted = requireStaticTask coordination "AllStarted"
            let release = requireMethod coordination "Release"

            do!
                runWithCompiledProxy
                    [| plugin |]
                    Array.empty
                    (endpointPort backendListener)
                    timeout.Token
                    (fun proxy ->
                        task {
                            let clients = ResizeArray<TcpClient>()
                            let backends = ResizeArray<TcpClient>()

                            try
                                for _ in 1 .. 4 do
                                    let acceptBackend =
                                        backendListener.AcceptTcpClientAsync(timeout.Token).AsTask()

                                    let! client = connectEventually proxy timeout.Token
                                    let! backend = acceptBackend
                                    clients.Add client
                                    backends.Add backend

                                let payloads =
                                    [| for index in 0 .. clients.Count - 1 ->
                                           bytes $"connection-{index}" |]

                                let receives =
                                    [| for index in 0 .. backends.Count - 1 ->
                                           readExactly
                                               (backends[index].GetStream())
                                               payloads[index].Length
                                               timeout.Token |]

                                let writes =
                                    [| for index in 0 .. clients.Count - 1 ->
                                           clients[index]
                                               .GetStream()
                                               .WriteAsync(payloads[index], timeout.Token)
                                               .AsTask() |]

                                do! Task.WhenAll writes
                                do! allStarted.WaitAsync(timeout.Token)
                                release.Invoke(null, Array.empty) |> ignore

                                let! received = Task.WhenAll receives
                                received |> should equal payloads
                            finally
                                for client in clients do
                                    client.Dispose()

                                for backend in backends do
                                    backend.Dispose()
                        })
        }

    [<Fact>]
    let ``repeated script stages keep mutable state isolated across directions and connections`` () =
        task {
            use timeout = new CancellationTokenSource(30_000)
            use workspace = new TestWorkspace()
            use backendListener = new TcpListener(loopback, 0)
            backendListener.Start()

            let source =
                """module RepeatedState

open System
open System.Threading.Tasks
open PipeL4.Plugins

type Processor() =
    let mutable completions = 0

    interface IByteStreamProcessor with
        member _.ProcessAsync(input, output, _) =
            if input.IsCompleted then
                completions <- completions + 1
                let marker = [| byte (int '0' + completions) |]
                marker.AsSpan().CopyTo(output.GetSpan(1))
                output.Advance(1)
                output.AppendInput(0L, input.Bytes.Length)
                marker.AsSpan().CopyTo(output.GetSpan(1))
                output.Advance(1)
                ValueTask<ProcessorResult>({ RetainedLength = 0L })
            else
                ValueTask<ProcessorResult>({ RetainedLength = input.Bytes.Length })
"""

            let scriptPath = workspace.WriteSource("RepeatedState.fsx", source)

            do!
                runWithScriptProxy
                    [| scriptPath; scriptPath |]
                    [| scriptPath; scriptPath |]
                    (endpointPort backendListener)
                    timeout.Token
                    (fun proxy ->
                        task {
                            let clients = ResizeArray<TcpClient>()
                            let backends = ResizeArray<TcpClient>()

                            try
                                for _ in 1 .. 2 do
                                    let acceptBackend =
                                        backendListener.AcceptTcpClientAsync(timeout.Token).AsTask()

                                    let! client = connectEventually proxy timeout.Token
                                    let! backend = acceptBackend
                                    clients.Add client
                                    backends.Add backend

                                let transfers =
                                    [| for index in 0 .. clients.Count - 1 ->
                                           task {
                                               let clientStream = clients[index].GetStream()
                                               let backendStream = backends[index].GetStream()
                                               let fromClient = bytes $"C{index}"
                                               let fromBackend = bytes $"S{index}"
                                               let expectedAtBackend = bytes $"11C{index}11"
                                               let expectedAtClient = bytes $"11S{index}11"

                                               do!
                                                   Task.WhenAll(
                                                       clientStream.WriteAsync(fromClient, timeout.Token).AsTask(),
                                                       backendStream.WriteAsync(fromBackend, timeout.Token).AsTask()
                                                   )

                                               clients[index].Client.Shutdown(SocketShutdown.Send)
                                               backends[index].Client.Shutdown(SocketShutdown.Send)

                                               let! atBackend =
                                                   readExactly
                                                       backendStream
                                                       expectedAtBackend.Length
                                                       timeout.Token

                                               let! atClient =
                                                   readExactly
                                                       clientStream
                                                       expectedAtClient.Length
                                                       timeout.Token

                                               atBackend |> should equal expectedAtBackend
                                               atClient |> should equal expectedAtClient
                                               do! expectEndOfStream backendStream timeout.Token
                                               do! expectEndOfStream clientStream timeout.Token
                                           } |]

                                let! _ = Task.WhenAll transfers
                                ()
                            finally
                                for client in clients do
                                    client.Dispose()

                                for backend in backends do
                                    backend.Dispose()
                        })
        }

    [<Fact>]
    let ``deterministic concurrent script stress isolates failing connections`` () =
        task {
            use timeout = new CancellationTokenSource(30_000)
            use workspace = new TestWorkspace()
            use backendListener = new TcpListener(loopback, 0)
            backendListener.Start()

            let source =
                """module StressIsolation

open System
open System.Threading.Tasks
open PipeL4.Plugins

type Processor() =
    let mutable poisoned = false

    interface IByteStreamProcessor with
        member _.ProcessAsync(input, output, _) =
            if poisoned then invalidOp "processor instance was poisoned"

            if not input.Bytes.IsEmpty && input.Bytes.FirstSpan[0] = 255uy then
                poisoned <- true
                invalidOp "selected connection failed"

            output.AppendInput(0L, input.Bytes.Length)
            ValueTask<ProcessorResult>({ RetainedLength = 0L })
"""

            let scriptPath = workspace.WriteSource("StressIsolation.fsx", source)

            do!
                runWithScriptProxy
                    [| scriptPath |]
                    [| scriptPath |]
                    (endpointPort backendListener)
                    timeout.Token
                    (fun proxy ->
                        task {
                            let clients = ResizeArray<TcpClient>()
                            let backends = ResizeArray<TcpClient>()

                            try
                                for _ in 1 .. 6 do
                                    let acceptBackend =
                                        backendListener.AcceptTcpClientAsync(timeout.Token).AsTask()

                                    let! client = connectEventually proxy timeout.Token
                                    let! backend = acceptBackend
                                    clients.Add client
                                    backends.Add backend

                                let transfers =
                                    [| for index in 0 .. clients.Count - 1 ->
                                           task {
                                               let clientStream = clients[index].GetStream()
                                               let backendStream = backends[index].GetStream()

                                               if index % 2 = 1 then
                                                   do! clientStream.WriteAsync([| 255uy |], timeout.Token)
                                                   do! expectDisconnect clientStream timeout.Token
                                                   do! expectDisconnect backendStream timeout.Token
                                               else
                                                   let fromClient = bytes $"client-{index}"
                                                   let fromBackend = bytes $"backend-{index}"
                                                   let receiveAtBackend =
                                                       readExactly
                                                           backendStream
                                                           fromClient.Length
                                                           timeout.Token

                                                   do! clientStream.WriteAsync(fromClient, timeout.Token)
                                                   let! atBackend = receiveAtBackend
                                                   atBackend |> should equal fromClient

                                                   let receiveAtClient =
                                                       readExactly
                                                           clientStream
                                                           fromBackend.Length
                                                           timeout.Token

                                                   do! backendStream.WriteAsync(fromBackend, timeout.Token)
                                                   let! atClient = receiveAtClient
                                                   atClient |> should equal fromBackend
                                           } |]

                                let! _ = Task.WhenAll transfers
                                ()

                                let acceptFinalBackend =
                                    backendListener.AcceptTcpClientAsync(timeout.Token).AsTask()

                                use! finalClient = connect proxy.ListenPort timeout.Token
                                use! finalBackend = acceptFinalBackend
                                let finalPayload = bytes "final-healthy"
                                let finalReceive =
                                    readExactly
                                        (finalBackend.GetStream())
                                        finalPayload.Length
                                        timeout.Token

                                do! finalClient.GetStream().WriteAsync(finalPayload, timeout.Token)
                                let! finalReceived = finalReceive
                                finalReceived |> should equal finalPayload
                            finally
                                for client in clients do
                                    client.Dispose()

                                for backend in backends do
                                    backend.Dispose()
                        })
        }

    [<Fact>]
    let ``invalid configured scripts fail before the listener starts`` () =
        task {
            use timeout = new CancellationTokenSource(30_000)
            use workspace = new TestWorkspace()

            let invalidPaths =
                [| Path.Combine(Path.GetTempPath(), Guid.NewGuid().ToString "N", "Missing.fsx")
                   workspace.WriteSource(
                       "CompileError.fsx",
                       "module CompileError\nlet value: int = \"wrong\""
                   )
                   workspace.WriteSource(
                       "InvalidProcessor.fsx",
                       "module InvalidProcessor\nlet value = 42"
                   ) |]

            for invalidPath in invalidPaths do
                let listenPort = reservePort ()
                let backendPort = reservePort ()

                let configuration =
                    createConfiguration
                        listenPort
                        backendPort
                        [| invalidPath |]
                        Array.empty

                let! _ =
                    Assert.ThrowsAsync<ScriptPluginConfigurationException>(fun () ->
                        ScriptTcpForwarder.runAsync
                            configuration
                            NullLogger.Instance
                            timeout.Token
                        :> Task)

                do! expectConnectionFailure listenPort timeout.Token
        }

    [<Fact>]
    let ``independent scripts share connection state across a TCP half-close`` () =
        task {
            use timeout = new CancellationTokenSource(30_000)
            use workspace = new TestWorkspace()
            use backendListener = new TcpListener(loopback, 0)
            backendListener.Start()

            let clientSource =
                """module ClientStatePublisher

open System
open System.Collections.Concurrent
open System.Threading.Tasks
open PipeL4.Plugins

type Coordination private () =
    static let mutable reference: obj = null
    static let disposed = TaskCompletionSource<unit>(TaskCreationOptions.RunContinuationsAsynchronously)
    static member Reference with get () = reference and set (value: obj) = reference <- value
    static member Disposed = disposed.Task
    static member SignalDisposed() = disposed.TrySetResult(()) |> ignore

type OwnedValue() =
    interface IAsyncDisposable with
        member _.DisposeAsync() =
            Coordination.SignalDisposed()
            ValueTask()

type Processor(connectionState: IConnectionState) =
    interface IByteStreamProcessor with
        member _.ProcessAsync(input, output, _) =
            if not input.Bytes.IsEmpty then
                connectionState.GetOrAdd("owned", fun () -> OwnedValue()) |> ignore

                let shared =
                    connectionState.GetOrAdd(
                        "cross-direction",
                        fun () -> ConcurrentDictionary<string, int64>()
                    )

                shared["request"] <- input.Bytes.Length
                Coordination.Reference <- shared

            output.AppendInput(0L, input.Bytes.Length)
            ValueTask<ProcessorResult>({ RetainedLength = 0L })
"""

            let serverSource =
                """module ServerStateObserver

open System
open System.Collections.Concurrent
open System.Threading.Tasks
open PipeL4.Plugins

type Coordination private () =
    static let mutable reference: obj = null
    static member Reference with get () = reference and set (value: obj) = reference <- value

type Processor(connectionState: IConnectionState) =
    interface IByteStreamProcessor with
        member _.ProcessAsync(input, output, _) =
            if not input.Bytes.IsEmpty then
                let shared =
                    connectionState.TryGet<ConcurrentDictionary<string, int64>>("cross-direction")
                    |> Option.defaultWith (fun () -> invalidOp "Cross-direction state was missing.")

                if not (shared.ContainsKey("request")) then
                    invalidOp "The request marker was missing."

                let reverse =
                    connectionState.GetOrAdd(
                        "reverse-published",
                        fun () -> ConcurrentDictionary<string, int64>()
                    )

                reverse["response"] <- input.Bytes.Length
                Coordination.Reference <- shared

            output.AppendInput(0L, input.Bytes.Length)
            ValueTask<ProcessorResult>({ RetainedLength = 0L })
"""

            let clientPath = workspace.WriteSource("ClientStatePublisher.fsx", clientSource)
            let serverPath = workspace.WriteSource("ServerStateObserver.fsx", serverSource)
            let! clientPlugin = compilePlugin workspace clientPath
            let! serverPlugin = compilePlugin workspace serverPath

            do!
                runWithCompiledProxy
                    [| clientPlugin |]
                    [| serverPlugin |]
                    (endpointPort backendListener)
                    timeout.Token
                    (fun proxy ->
                        task {
                            let acceptBackend =
                                backendListener.AcceptTcpClientAsync(timeout.Token).AsTask()

                            use! client = connectEventually proxy timeout.Token
                            use! backend = acceptBackend
                            let clientStream = client.GetStream()
                            let backendStream = backend.GetStream()
                            let request = bytes "request"
                            do! clientStream.WriteAsync(request, timeout.Token)
                            let! forwarded = readExactly backendStream request.Length timeout.Token
                            forwarded |> should equal request

                            client.Client.Shutdown(SocketShutdown.Send)
                            do! expectEndOfStream backendStream timeout.Token

                            let clientCoordination =
                                requireType clientPlugin.Assembly "Coordination"

                            let disposed = requireStaticTask clientCoordination "Disposed"
                            disposed.IsCompleted |> should equal false

                            let response = bytes "response"
                            do! backendStream.WriteAsync(response, timeout.Token)
                            let! returned = readExactly clientStream response.Length timeout.Token
                            returned |> should equal response

                            let serverCoordination =
                                requireType serverPlugin.Assembly "Coordination"

                            let published =
                                staticProperty<obj> clientCoordination "Reference"

                            let observed =
                                staticProperty<obj> serverCoordination "Reference"

                            Object.ReferenceEquals(published, observed)
                            |> should equal true

                            disposed.IsCompleted |> should equal false
                            backend.Client.Shutdown(SocketShutdown.Send)
                            do! expectEndOfStream clientStream timeout.Token
                            do! disposed.WaitAsync(timeout.Token)
                        })
        }

    [<Fact>]
    let ``opposite directions safely race to initialize one public state value`` () =
        task {
            use timeout = new CancellationTokenSource(30_000)
            use workspace = new TestWorkspace()
            use backendListener = new TcpListener(loopback, 0)
            backendListener.Start()

            let source =
                """module ConcurrentStateInitialization

open System
open System.Collections.Concurrent
open System.Threading
open System.Threading.Tasks
open PipeL4.Plugins

type Coordination private () =
    static let ready = TaskCompletionSource<unit>(TaskCreationOptions.RunContinuationsAsynchronously)
    static let references = ConcurrentQueue<obj>()
    static let mutable arrivals = 0
    static let mutable factoryCalls = 0
    static member Arrive() =
        if Interlocked.Increment(&arrivals) = 2 then
            ready.TrySetResult(()) |> ignore
        ready.Task
    static member Create() =
        Interlocked.Increment(&factoryCalls) |> ignore
        ConcurrentDictionary<string, int64>()
    static member Record(value: obj) = references.Enqueue(value)
    static member FactoryCalls = Volatile.Read(&factoryCalls)
    static member References = references.ToArray()

type Processor(connectionState: IConnectionState) =
    interface IByteStreamProcessor with
        member _.ProcessAsync(input, output, cancellationToken) =
            ValueTask<ProcessorResult>(
                task {
                    if not input.Bytes.IsEmpty then
                        do! Coordination.Arrive().WaitAsync(cancellationToken)
                        let shared = connectionState.GetOrAdd("race", Coordination.Create)
                        Coordination.Record(shared)

                    output.AppendInput(0L, input.Bytes.Length)
                    return { RetainedLength = 0L }
                }
            )
"""

            let scriptPath = workspace.WriteSource("ConcurrentStateInitialization.fsx", source)
            let! plugin = compilePlugin workspace scriptPath

            do!
                runWithCompiledProxy
                    [| plugin |]
                    [| plugin |]
                    (endpointPort backendListener)
                    timeout.Token
                    (fun proxy ->
                        task {
                            let acceptBackend =
                                backendListener.AcceptTcpClientAsync(timeout.Token).AsTask()

                            use! client = connectEventually proxy timeout.Token
                            use! backend = acceptBackend
                            let fromClient = bytes "client"
                            let fromBackend = bytes "backend"
                            let clientStream = client.GetStream()
                            let backendStream = backend.GetStream()
                            let receiveAtBackend =
                                readExactly backendStream fromClient.Length timeout.Token

                            let receiveAtClient =
                                readExactly clientStream fromBackend.Length timeout.Token

                            do!
                                Task.WhenAll(
                                    clientStream.WriteAsync(fromClient, timeout.Token).AsTask(),
                                    backendStream.WriteAsync(fromBackend, timeout.Token).AsTask()
                                )

                            let! atBackend = receiveAtBackend
                            let! atClient = receiveAtClient
                            atBackend |> should equal fromClient
                            atClient |> should equal fromBackend

                            let coordination = requireType plugin.Assembly "Coordination"
                            staticProperty<int> coordination "FactoryCalls"
                            |> should equal 1

                            let references =
                                staticProperty<obj array> coordination "References"

                            references.Length |> should equal 2
                            Object.ReferenceEquals(references[0], references[1])
                            |> should equal true
                        })
        }

    [<Fact>]
    let ``separate TCP connections isolate state and clean it after completion or failure`` () =
        task {
            use timeout = new CancellationTokenSource(30_000)
            use workspace = new TestWorkspace()
            use backendListener = new TcpListener(loopback, 0)
            backendListener.Start()

            let source =
                """module ConnectionStateLifetime

open System
open System.Collections.Concurrent
open System.Threading
open System.Threading.Tasks
open PipeL4.Plugins

type Coordination private () =
    static let states = ConcurrentQueue<IConnectionState>()
    static let values = ConcurrentQueue<obj>()
    static let allDisposed = TaskCompletionSource<unit>(TaskCreationOptions.RunContinuationsAsynchronously)
    static let mutable disposalCount = 0
    static let mutable mutationCount = 0
    static member Record(state: IConnectionState, value: obj) =
        states.Enqueue(state)
        values.Enqueue(value)
    static member RecordMutation() = Interlocked.Increment(&mutationCount) |> ignore
    static member RecordDisposal() =
        if Interlocked.Increment(&disposalCount) = 3 then
            allDisposed.TrySetResult(()) |> ignore
    static member States = states.ToArray()
    static member Values = values.ToArray()
    static member MutationCount = Volatile.Read(&mutationCount)
    static member AllDisposed = allDisposed.Task

type OwnedValue() =
    member _.Modify() = Coordination.RecordMutation()

    interface IAsyncDisposable with
        member _.DisposeAsync() =
            Coordination.RecordDisposal()
            ValueTask()

type Processor(connectionState: IConnectionState) =
    let mutable initialized = false

    interface IByteStreamProcessor with
        member _.ProcessAsync(input, output, _) =
            if not input.Bytes.IsEmpty then
                if not initialized then
                    let value = connectionState.GetOrAdd("owned", fun () -> OwnedValue())
                    Coordination.Record(connectionState, value)
                    initialized <- true

                let value = connectionState.GetOrAdd("owned", fun () -> OwnedValue())
                value.Modify()

                if input.Bytes.FirstSpan[0] = 255uy then
                    invalidOp "requested plugin failure"

            output.AppendInput(0L, input.Bytes.Length)
            ValueTask<ProcessorResult>({ RetainedLength = 0L })
"""

            let scriptPath = workspace.WriteSource("ConnectionStateLifetime.fsx", source)
            let! plugin = compilePlugin workspace scriptPath
            let coordination = requireType plugin.Assembly "Coordination"
            let allDisposed = requireStaticTask coordination "AllDisposed"

            do!
                runWithCompiledProxy
                    [| plugin |]
                    Array.empty
                    (endpointPort backendListener)
                    timeout.Token
                    (fun proxy ->
                        task {
                            let acceptNormalBackend =
                                backendListener.AcceptTcpClientAsync(timeout.Token).AsTask()

                            use! normalClient = connectEventually proxy timeout.Token
                            use! normalBackend = acceptNormalBackend
                            let normalClientStream = normalClient.GetStream()
                            let normalBackendStream = normalBackend.GetStream()
                            let payload = [| 1uy |]
                            do! normalClientStream.WriteAsync(payload, timeout.Token)
                            let! forwarded = readExactly normalBackendStream 1 timeout.Token
                            forwarded |> should equal payload
                            normalClient.Client.Shutdown(SocketShutdown.Send)
                            do! expectEndOfStream normalBackendStream timeout.Token
                            normalBackend.Client.Shutdown(SocketShutdown.Send)
                            do! expectEndOfStream normalClientStream timeout.Token

                            let acceptFailedBackend =
                                backendListener.AcceptTcpClientAsync(timeout.Token).AsTask()

                            use! failedClient = connectEventually proxy timeout.Token
                            use! failedBackend = acceptFailedBackend
                            do! failedClient.GetStream().WriteAsync([| 255uy |], timeout.Token)
                            do! expectDisconnect (failedClient.GetStream()) timeout.Token
                            do! expectDisconnect (failedBackend.GetStream()) timeout.Token

                            let acceptRetryBackend =
                                backendListener.AcceptTcpClientAsync(timeout.Token).AsTask()

                            use! retryClient = connect proxy.ListenPort timeout.Token
                            use! retryBackend = acceptRetryBackend
                            let retryClientStream = retryClient.GetStream()
                            let retryBackendStream = retryBackend.GetStream()
                            let retryPayload = [| 2uy |]
                            do! retryClientStream.WriteAsync(retryPayload, timeout.Token)
                            let! retried =
                                readExactly retryBackendStream retryPayload.Length timeout.Token

                            retried |> should equal retryPayload
                            retryClient.Client.Shutdown(SocketShutdown.Send)
                            do! expectEndOfStream retryBackendStream timeout.Token
                            retryBackend.Client.Shutdown(SocketShutdown.Send)
                            do! expectEndOfStream retryClientStream timeout.Token
                            do! allDisposed.WaitAsync(timeout.Token)

                            let states =
                                staticProperty<IConnectionState array> coordination "States"

                            let values = staticProperty<obj array> coordination "Values"
                            states.Length |> should equal 3
                            values.Length |> should equal 3
                            staticProperty<int> coordination "MutationCount" |> should equal 3

                            for first in 0 .. states.Length - 1 do
                                for second in first + 1 .. states.Length - 1 do
                                    Object.ReferenceEquals(states[first], states[second])
                                    |> should equal false

                                    Object.ReferenceEquals(values[first], values[second])
                                    |> should equal false

                            for state in states do
                                Assert.Throws<ObjectDisposedException>(fun () ->
                                    state.TryGet<obj>("owned") |> ignore)
                                |> ignore
                        })
        }

    [<Fact>]
    let ``configured independent stages share state while upstream input remains retained`` () =
        task {
            use timeout = new CancellationTokenSource(30_000)
            use workspace = new TestWorkspace()
            use backendListener = new TcpListener(loopback, 0)
            backendListener.Start()

            let writerSource =
                """module RetainingStateWriter

open System
open System.Collections.Concurrent
open System.Threading.Tasks
open PipeL4.Plugins

type Processor(connectionState: IConnectionState) =
    let mutable invocation = 0L

    interface IByteStreamProcessor with
        member _.ProcessAsync(input, output, _) =
            if input.IsCompleted || input.Bytes.IsEmpty then
                ValueTask<ProcessorResult>({ RetainedLength = 0L })
            else
                invocation <- invocation + 1L

                let shared =
                    connectionState.GetOrAdd(
                        "ordered-shared-state",
                        fun () -> ConcurrentDictionary<string, int64>()
                    )

                if invocation = 1L then
                    shared["publisher"] <- 41L

                let mutable observed = 0L
                shared.TryGetValue("observer", &observed) |> ignore
                let trigger = [| byte observed |]
                trigger.AsSpan().CopyTo(output.GetSpan(trigger.Length))
                output.Advance(trigger.Length)
                ValueTask<ProcessorResult>({ RetainedLength = input.Bytes.Length })
"""

            let observerSource =
                """module SharedStateObserver

open System
open System.Collections.Concurrent
open System.Threading.Tasks
open PipeL4.Plugins

type Processor(connectionState: IConnectionState) =
    interface IByteStreamProcessor with
        member _.ProcessAsync(input, output, _) =
            if input.Bytes.IsEmpty then
                ValueTask<ProcessorResult>({ RetainedLength = 0L })
            else
                let shared =
                    connectionState.TryGet<ConcurrentDictionary<string, int64>>(
                        "ordered-shared-state"
                    )
                    |> Option.defaultWith (fun () -> invalidOp "Shared state was missing.")

                let publisher = shared["publisher"]

                let observer =
                    shared.AddOrUpdate("observer", 42L, fun _ current -> current)

                let generated =
                    [| byte publisher
                       byte observer
                       input.Bytes.FirstSpan[0] |]

                generated.AsSpan().CopyTo(output.GetSpan(generated.Length))
                output.Advance(generated.Length)
                ValueTask<ProcessorResult>({ RetainedLength = 0L })
"""

            let writerPath = workspace.WriteSource("RetainingStateWriter.fsx", writerSource)
            let observerPath = workspace.WriteSource("SharedStateObserver.fsx", observerSource)

            do!
                runWithScriptProxy
                    [| writerPath; observerPath |]
                    Array.empty
                    (endpointPort backendListener)
                    timeout.Token
                    (fun proxy ->
                        task {
                            let acceptBackend =
                                backendListener.AcceptTcpClientAsync(timeout.Token).AsTask()

                            use! client = connectEventually proxy timeout.Token
                            use! backend = acceptBackend
                            let clientStream = client.GetStream()
                            let backendStream = backend.GetStream()

                            do! clientStream.WriteAsync([| 1uy |], timeout.Token)
                            let! first = readExactly backendStream 3 timeout.Token
                            first |> should equal [| 41uy; 42uy; 0uy |]

                            do! clientStream.WriteAsync([| 2uy |], timeout.Token)
                            let! second = readExactly backendStream 3 timeout.Token
                            second |> should equal [| 41uy; 42uy; 42uy |]

                            client.Client.Shutdown(SocketShutdown.Send)
                            do! expectEndOfStream backendStream timeout.Token
                            backend.Client.Shutdown(SocketShutdown.Send)
                            do! expectEndOfStream clientStream timeout.Token
                        })
        }

    [<Fact>]
    let ``configured opposite directions causally share and modify state`` () =
        task {
            use timeout = new CancellationTokenSource(30_000)
            use workspace = new TestWorkspace()
            use backendListener = new TcpListener(loopback, 0)
            backendListener.Start()

            let requestSource =
                """module RequestStateProcessor

open System
open System.Collections.Concurrent
open System.Threading.Tasks
open PipeL4.Plugins

type Processor(connectionState: IConnectionState) =
    interface IByteStreamProcessor with
        member _.ProcessAsync(input, output, _) =
            if not input.Bytes.IsEmpty then
                let shared =
                    connectionState.GetOrAdd(
                        "bidirectional-state",
                        fun () -> ConcurrentDictionary<string, int64>()
                    )

                let requests =
                    shared.AddOrUpdate("requests", 1L, fun _ current -> current + 1L)

                let mutable responses = 0L
                shared.TryGetValue("responses", &responses) |> ignore
                let marker = [| byte requests; byte responses |]
                marker.AsSpan().CopyTo(output.GetSpan(marker.Length))
                output.Advance(marker.Length)
                output.AppendInput(0L, input.Bytes.Length)

            ValueTask<ProcessorResult>({ RetainedLength = 0L })
"""

            let responseSource =
                """module ResponseStateProcessor

open System
open System.Collections.Concurrent
open System.Threading.Tasks
open PipeL4.Plugins

type Processor(connectionState: IConnectionState) =
    interface IByteStreamProcessor with
        member _.ProcessAsync(input, output, _) =
            if not input.Bytes.IsEmpty then
                let shared =
                    connectionState.TryGet<ConcurrentDictionary<string, int64>>(
                        "bidirectional-state"
                    )
                    |> Option.defaultWith (fun () -> invalidOp "Request state was missing.")

                let responses =
                    shared.AddOrUpdate("responses", 1L, fun _ current -> current + 1L)

                let requests = shared["requests"]
                let marker = [| byte requests; byte responses |]
                marker.AsSpan().CopyTo(output.GetSpan(marker.Length))
                output.Advance(marker.Length)
                output.AppendInput(0L, input.Bytes.Length)

            ValueTask<ProcessorResult>({ RetainedLength = 0L })
"""

            let requestPath = workspace.WriteSource("RequestStateProcessor.fsx", requestSource)
            let responsePath = workspace.WriteSource("ResponseStateProcessor.fsx", responseSource)

            do!
                runWithScriptProxy
                    [| requestPath |]
                    [| responsePath |]
                    (endpointPort backendListener)
                    timeout.Token
                    (fun proxy ->
                        task {
                            let acceptBackend =
                                backendListener.AcceptTcpClientAsync(timeout.Token).AsTask()

                            use! client = connectEventually proxy timeout.Token
                            use! backend = acceptBackend
                            let clientStream = client.GetStream()
                            let backendStream = backend.GetStream()

                            do! clientStream.WriteAsync([| 101uy |], timeout.Token)
                            let! firstRequest = readExactly backendStream 3 timeout.Token
                            firstRequest |> should equal [| 1uy; 0uy; 101uy |]

                            do! backendStream.WriteAsync([| 201uy |], timeout.Token)
                            let! firstResponse = readExactly clientStream 3 timeout.Token
                            firstResponse |> should equal [| 1uy; 1uy; 201uy |]

                            do! clientStream.WriteAsync([| 102uy |], timeout.Token)
                            let! secondRequest = readExactly backendStream 3 timeout.Token
                            secondRequest |> should equal [| 2uy; 1uy; 102uy |]

                            client.Client.Shutdown(SocketShutdown.Send)
                            do! expectEndOfStream backendStream timeout.Token
                            backend.Client.Shutdown(SocketShutdown.Send)
                            do! expectEndOfStream clientStream timeout.Token
                        })
        }

    [<Fact>]
    let ``configured script state is isolated between concurrent TCP connections`` () =
        task {
            use timeout = new CancellationTokenSource(30_000)
            use workspace = new TestWorkspace()
            use backendListener = new TcpListener(loopback, 0)
            backendListener.Start()

            let source =
                """module ConnectionIsolationProcessor

open System
open System.Collections.Concurrent
open System.Threading.Tasks
open PipeL4.Plugins

type Processor(connectionState: IConnectionState) =
    interface IByteStreamProcessor with
        member _.ProcessAsync(input, output, _) =
            if not input.Bytes.IsEmpty then
                let shared =
                    connectionState.GetOrAdd(
                        "connection-isolation",
                        fun () -> ConcurrentDictionary<string, int64>()
                    )

                let suppliedIdentifier = int64 input.Bytes.FirstSpan[0]
                let identifier = shared.GetOrAdd("identifier", suppliedIdentifier)
                let count = shared.AddOrUpdate("count", 1L, fun _ current -> current + 1L)
                let generated = [| byte identifier; byte count |]
                generated.AsSpan().CopyTo(output.GetSpan(generated.Length))
                output.Advance(generated.Length)

            ValueTask<ProcessorResult>({ RetainedLength = 0L })
"""

            let scriptPath = workspace.WriteSource("ConnectionIsolationProcessor.fsx", source)

            do!
                runWithScriptProxy
                    [| scriptPath |]
                    Array.empty
                    (endpointPort backendListener)
                    timeout.Token
                    (fun proxy ->
                        task {
                            let acceptBackendA =
                                backendListener.AcceptTcpClientAsync(timeout.Token).AsTask()

                            use! clientA = connectEventually proxy timeout.Token
                            use! backendA = acceptBackendA

                            let acceptBackendB =
                                backendListener.AcceptTcpClientAsync(timeout.Token).AsTask()

                            use! clientB = connectEventually proxy timeout.Token
                            use! backendB = acceptBackendB
                            let clientStreamA = clientA.GetStream()
                            let clientStreamB = clientB.GetStream()
                            let backendStreamA = backendA.GetStream()
                            let backendStreamB = backendB.GetStream()
                            let receiveFirstA = readExactly backendStreamA 2 timeout.Token
                            let receiveFirstB = readExactly backendStreamB 2 timeout.Token

                            do!
                                Task.WhenAll(
                                    clientStreamA.WriteAsync([| 10uy |], timeout.Token).AsTask(),
                                    clientStreamB.WriteAsync([| 20uy |], timeout.Token).AsTask()
                                )

                            let! firstA = receiveFirstA
                            let! firstB = receiveFirstB
                            firstA |> should equal [| 10uy; 1uy |]
                            firstB |> should equal [| 20uy; 1uy |]

                            do! clientStreamB.WriteAsync([| 21uy |], timeout.Token)
                            let! secondB = readExactly backendStreamB 2 timeout.Token
                            secondB |> should equal [| 20uy; 2uy |]

                            do! clientStreamA.WriteAsync([| 11uy |], timeout.Token)
                            let! secondA = readExactly backendStreamA 2 timeout.Token
                            secondA |> should equal [| 10uy; 2uy |]

                            clientA.Client.Shutdown(SocketShutdown.Send)
                            clientB.Client.Shutdown(SocketShutdown.Send)

                            let! _ =
                                Task.WhenAll(
                                    expectEndOfStream backendStreamA timeout.Token,
                                    expectEndOfStream backendStreamB timeout.Token
                                )

                            backendA.Client.Shutdown(SocketShutdown.Send)
                            backendB.Client.Shutdown(SocketShutdown.Send)

                            let! _ =
                                Task.WhenAll(
                                    expectEndOfStream clientStreamA timeout.Token,
                                    expectEndOfStream clientStreamB timeout.Token
                                )

                            ()
                        })
        }

    [<Fact>]
    let ``repeated configured stages keep local fields separate while sharing state`` () =
        task {
            use timeout = new CancellationTokenSource(30_000)
            use workspace = new TestWorkspace()
            use backendListener = new TcpListener(loopback, 0)
            backendListener.Start()

            let source =
                """module RepeatedSharedStateProcessor

open System
open System.Collections.Concurrent
open System.Threading.Tasks
open PipeL4.Plugins

type Processor(connectionState: IConnectionState) =
    let mutable calls = 0L

    interface IByteStreamProcessor with
        member _.ProcessAsync(input, output, _) =
            if not input.Bytes.IsEmpty then
                calls <- calls + 1L

                let shared =
                    connectionState.GetOrAdd(
                        "repeated-shared-state",
                        fun () -> ConcurrentDictionary<string, int64>()
                    )

                let visits =
                    shared.AddOrUpdate("visits", 1L, fun _ current -> current + 1L)

                let marker = [| byte calls; byte visits |]
                marker.AsSpan().CopyTo(output.GetSpan(marker.Length))
                output.Advance(marker.Length)
                output.AppendInput(0L, input.Bytes.Length)

            ValueTask<ProcessorResult>({ RetainedLength = 0L })
"""

            let scriptPath = workspace.WriteSource("RepeatedSharedStateProcessor.fsx", source)

            do!
                runWithScriptProxy
                    [| scriptPath; scriptPath |]
                    Array.empty
                    (endpointPort backendListener)
                    timeout.Token
                    (fun proxy ->
                        task {
                            let acceptBackend =
                                backendListener.AcceptTcpClientAsync(timeout.Token).AsTask()

                            use! client = connectEventually proxy timeout.Token
                            use! backend = acceptBackend
                            let clientStream = client.GetStream()
                            let backendStream = backend.GetStream()

                            do! clientStream.WriteAsync([| 88uy |], timeout.Token)
                            let! first = readExactly backendStream 5 timeout.Token
                            first |> should equal [| 1uy; 2uy; 1uy; 1uy; 88uy |]

                            do! clientStream.WriteAsync([| 89uy |], timeout.Token)
                            let! second = readExactly backendStream 5 timeout.Token
                            second |> should equal [| 2uy; 4uy; 2uy; 3uy; 89uy |]

                            client.Client.Shutdown(SocketShutdown.Send)
                            do! expectEndOfStream backendStream timeout.Token
                            backend.Client.Shutdown(SocketShutdown.Send)
                            do! expectEndOfStream clientStream timeout.Token
                        })
        }

    [<Fact>]
    let ``state written without output is visible to a later configured stage invocation`` () =
        task {
            use timeout = new CancellationTokenSource(30_000)
            use workspace = new TestWorkspace()
            use backendListener = new TcpListener(loopback, 0)
            backendListener.Start()

            let writerSource =
                """module OutputlessStateWriter

open System
open System.Collections.Concurrent
open System.Threading.Tasks
open PipeL4.Plugins

type Processor(connectionState: IConnectionState) =
    let mutable receivedInput = false

    interface IByteStreamProcessor with
        member _.ProcessAsync(input, output, _) =
            if input.IsCompleted then
                if receivedInput then
                    let shared =
                        connectionState.TryGet<ConcurrentDictionary<string, int64>>(
                            "outputless-state"
                        )
                        |> Option.defaultWith (fun () -> invalidOp "Outputless state was missing.")

                    let trigger = [| byte shared["value"] |]
                    trigger.AsSpan().CopyTo(output.GetSpan(trigger.Length))
                    output.Advance(trigger.Length)

                ValueTask<ProcessorResult>({ RetainedLength = 0L })
            else
                if not input.Bytes.IsEmpty then
                    let shared =
                        connectionState.GetOrAdd(
                            "outputless-state",
                            fun () -> ConcurrentDictionary<string, int64>()
                        )

                    shared["value"] <- 7L
                    receivedInput <- true

                ValueTask<ProcessorResult>({ RetainedLength = 0L })
"""

            let observerSource =
                """module OutputlessStateObserver

open System
open System.Collections.Concurrent
open System.Threading.Tasks
open PipeL4.Plugins

type Processor(connectionState: IConnectionState) =
    interface IByteStreamProcessor with
        member _.ProcessAsync(input, output, _) =
            if not input.Bytes.IsEmpty then
                let shared =
                    connectionState.TryGet<ConcurrentDictionary<string, int64>>(
                        "outputless-state"
                    )
                    |> Option.defaultWith (fun () -> invalidOp "Outputless state was not visible.")

                let generated = [| byte shared["value"]; input.Bytes.FirstSpan[0] |]
                generated.AsSpan().CopyTo(output.GetSpan(generated.Length))
                output.Advance(generated.Length)

            ValueTask<ProcessorResult>({ RetainedLength = 0L })
"""

            let writerPath = workspace.WriteSource("OutputlessStateWriter.fsx", writerSource)
            let observerPath = workspace.WriteSource("OutputlessStateObserver.fsx", observerSource)

            do!
                runWithScriptProxy
                    [| writerPath; observerPath |]
                    Array.empty
                    (endpointPort backendListener)
                    timeout.Token
                    (fun proxy ->
                        task {
                            let acceptBackend =
                                backendListener.AcceptTcpClientAsync(timeout.Token).AsTask()

                            use! client = connectEventually proxy timeout.Token
                            use! backend = acceptBackend
                            let clientStream = client.GetStream()
                            let backendStream = backend.GetStream()

                            do! clientStream.WriteAsync(bytes "silent-state", timeout.Token)
                            client.Client.Shutdown(SocketShutdown.Send)

                            let! observed = readExactly backendStream 2 timeout.Token
                            observed |> should equal [| 7uy; 7uy |]
                            do! expectEndOfStream backendStream timeout.Token
                            backend.Client.Shutdown(SocketShutdown.Send)
                            do! expectEndOfStream clientStream timeout.Token
                        })
        }
