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

        member _.Failures = failures.ToArray()
        member _.FirstFailure = firstFailure.Task

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
                    (fun () ->
                        { ClientToBackend =
                            clientPlugins |> Array.map _.CreateProcessor()
                          BackendToClient =
                            serverPlugins |> Array.map _.CreateProcessor() })
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
                ValueTask<ProcessorResult>({ RetainedLength = 0L })
            else
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
open System.Threading.Tasks
open PipeL4.Plugins

type Processor() =
    interface IByteStreamProcessor with
        member _.ProcessAsync(input, output, _) =
            output.AppendInput(0L, input.Bytes.Length)
            let generated = [| 88uy |]
            generated.AsSpan().CopyTo(output.GetSpan(1))
            output.Advance(1)
            invalidOp "invocation failed"

    interface IAsyncDisposable with
        member _.DisposeAsync() =
            ValueTask(Task.FromException(InvalidOperationException("disposal failed")))
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

            do!
                runWithForwarder
                    (fun cancellationToken ->
                        ScriptTcpForwarder.runAsync
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

                            let! logged = logger.FirstFailure.WaitAsync(timeout.Token)
                            logged |> should be ofExactType<AggregateException>

                            let failures = exceptionTree logged |> Seq.toArray

                            failures
                            |> Array.exists (fun error -> error.Message = "invocation failed")
                            |> should equal true

                            failures
                            |> Array.exists (fun error -> error.Message = "disposal failed")
                            |> should equal true

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
    static let mutable retainedCount = 0
    static let mutable startedCount = 0
    static let mutable cancelledCount = 0
    static let mutable disposedCount = 0
    static member Retained = retained.Task
    static member Started = started.Task
    static member Cancelled = cancelled.Task
    static member Disposed = disposed.Task
    static member SignalRetained() =
        if Interlocked.Increment(&retainedCount) = 2 then retained.TrySetResult(()) |> ignore
    static member SignalStarted() =
        if Interlocked.Increment(&startedCount) = 2 then started.TrySetResult(()) |> ignore
    static member SignalCancelled() =
        if Interlocked.Increment(&cancelledCount) = 2 then cancelled.TrySetResult(()) |> ignore
    static member SignalDisposed() =
        if Interlocked.Increment(&disposedCount) = 2 then disposed.TrySetResult(()) |> ignore

type Processor() =
    let mutable calls = 0

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

            do!
                runWithCompiledProxy
                    [| identity; gate |]
                    [| identity; gate |]
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
                            do! proxy.Completion.WaitAsync(timeout.Token)
                            do! cancelled.WaitAsync(timeout.Token)
                            do! disposed.WaitAsync(timeout.Token)
                            do! expectDisconnect clientStream timeout.Token
                            do! expectDisconnect backendStream timeout.Token
                            do! expectConnectionFailure proxy.ListenPort timeout.Token
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
    let ``real script processors execute concurrently across connections`` () =
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
    static let release = TaskCompletionSource<unit>(TaskCreationOptions.RunContinuationsAsynchronously)
    static let mutable started = 0

    static member AllStarted = allStarted.Task

    static member SignalStarted() =
        if Interlocked.Increment(&started) = 4 then
            allStarted.TrySetResult(()) |> ignore

    static member Release() = release.TrySetResult(()) |> ignore

    static member WaitAsync(cancellationToken: CancellationToken) =
        release.Task.WaitAsync(cancellationToken)

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
