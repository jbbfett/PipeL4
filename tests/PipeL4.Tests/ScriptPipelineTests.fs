namespace PipeL4.Tests

open System
open System.Buffers
open System.Collections.Generic
open System.IO
open System.Net
open System.Reflection
open System.Threading
open System.Threading.Tasks
open FsUnit.Xunit
open Microsoft.Extensions.Logging.Abstractions
open PipeL4.Core
open PipeL4.Plugins
open Xunit

module ScriptPipelineTests =
    type private TestOwner(buffer: byte array) =
        interface IMemoryOwner<byte> with
            member _.Memory = Memory<byte>(buffer)
            member _.Dispose() = ()

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

    let private createConfiguration clientToServerPaths serverToClientPaths =
        let pluginArguments optionName paths =
            paths
            |> Array.collect (fun path -> [| optionName; path |])

        let arguments =
            Array.concat
                [| [| "--listen-address"
                      string IPAddress.Loopback
                      "--listen-port"
                      "5000"
                      "--backend-host"
                      string IPAddress.Loopback
                      "--backend-port"
                      "6000" |]
                   pluginArguments "--client-to-server-plugin" clientToServerPaths
                   pluginArguments "--server-to-client-plugin" serverToClientPaths |]

        match ForwardingConfiguration.tryParseArguments arguments with
        | Ok configuration -> configuration
        | Error error -> failwithf "Test configuration was invalid: %s" error

    let private identitySource moduleName processorName =
        String.concat
            Environment.NewLine
            [ $"module {moduleName}"
              ""
              "open System.Threading.Tasks"
              "open PipeL4.Plugins"
              ""
              $"type {processorName}() ="
              "    interface IByteStreamProcessor with"
              "        member _.ProcessAsync(input, output, _) ="
              "            output.AppendInput(0L, input.Bytes.Length)"
              "            ValueTask<ProcessorResult>({ RetainedLength = 0L })" ]

    let private discardSink =
        { new IProcessorPipelineSink with
            member _.WriteAsync(_, cancellationToken) =
                cancellationToken.ThrowIfCancellationRequested()
                ValueTask()

            member _.CompleteAsync(cancellationToken) =
                cancellationToken.ThrowIfCancellationRequested()
                ValueTask() }

    let private expectCompiled result =
        match result with
        | Compiled(plugin, _) -> plugin
        | Failed diagnostics ->
            diagnostics
            |> Array.map _.Message
            |> String.concat Environment.NewLine
            |> failwith

    let private requireType (assembly: Assembly) name =
        assembly.GetTypes()
        |> Array.tryFind (fun value -> value.Name = name)
        |> Option.defaultWith (fun () -> failwithf "Type '%s' was not found." name)

    let private staticProperty<'value> (processorType: Type) name =
        let property =
            processorType.GetProperty(name, BindingFlags.Public ||| BindingFlags.Static)
            |> Option.ofObj
            |> Option.defaultWith (fun () ->
                failwithf "Property '%s.%s' was not found." processorType.FullName name)

        match property.GetValue(null) with
        | :? 'value as value -> value
        | _ -> failwithf "Property '%s.%s' had an unexpected value." processorType.FullName name

    let private innerMessage (error: exn) =
        error.InnerException
        |> Option.ofObj
        |> Option.map _.Message
        |> Option.defaultValue ""

    [<Fact>]
    let ``loader compiles canonical paths once and creates isolated ordered stages`` () =
        task {
            use workspace = new TestWorkspace()

            let firstPath =
                workspace.WriteSource(
                    "First.fsx",
                    identitySource "FirstPlugin" "FirstProcessor"
                )

            let secondPath =
                workspace.WriteSource(
                    "Second.fsx",
                    identitySource "SecondPlugin" "SecondProcessor"
                )

            let firstAlias =
                let directory =
                    Path.GetDirectoryName firstPath
                    |> Option.ofObj
                    |> Option.defaultWith (fun () -> failwith "The test script path has no directory.")

                let fileName =
                    Path.GetFileName firstPath
                    |> Option.ofObj
                    |> Option.defaultWith (fun () -> failwith "The test script path has no file name.")

                Path.Combine(directory, ".", fileName)

            let configuration =
                createConfiguration
                    [| firstAlias; secondPath; firstPath |]
                    [| firstPath |]

            let counts = Dictionary<string, int>(StringComparer.Ordinal)

            let compile path cancellationToken =
                let current =
                    match counts.TryGetValue path with
                    | true, value -> value
                    | false, _ -> 0

                counts[path] <- current + 1
                workspace.Compiler.CompileAndLoadAsync(path, cancellationToken)

            let! definition =
                ScriptPipelineLoader.loadWithCompilerAsync
                    compile
                    configuration.ClientToServerPluginPaths
                    configuration.ServerToClientPluginPaths
                    CancellationToken.None

            counts.Count |> should equal 2
            counts.Values |> Seq.toArray |> should equal [| 1; 1 |]

            definition.ClientToServerPaths
            |> should equal [| Path.GetFullPath firstPath; Path.GetFullPath secondPath; Path.GetFullPath firstPath |]

            definition.ServerToClientPaths
            |> should equal [| Path.GetFullPath firstPath |]

            let! firstConnection =
                definition.CreateProcessorPipelinesAsync(ConnectionState()).AsTask()

            let! secondConnection =
                definition.CreateProcessorPipelinesAsync(ConnectionState()).AsTask()

            firstConnection.ClientToBackend.Length |> should equal 3
            firstConnection.BackendToClient.Length |> should equal 1

            Object.ReferenceEquals(
                firstConnection.ClientToBackend[0],
                firstConnection.ClientToBackend[2]
            )
            |> should equal false

            Object.ReferenceEquals(
                firstConnection.ClientToBackend[0],
                firstConnection.BackendToClient[0]
            )
            |> should equal false

            for index in 0 .. firstConnection.ClientToBackend.Length - 1 do
                Object.ReferenceEquals(
                    firstConnection.ClientToBackend[index],
                    secondConnection.ClientToBackend[index]
                )
                |> should equal false
        }

    [<Fact>]
    let ``loader propagates cancellation during compilation and skips later scripts`` () =
        task {
            use workspace = new TestWorkspace()
            let firstPath = workspace.WriteSource("First.fsx", "")
            let secondPath = workspace.WriteSource("Second.fsx", "")
            let configuration = createConfiguration [| firstPath; secondPath |] Array.empty
            use cancellation = new CancellationTokenSource()
            let compilerEntered =
                TaskCompletionSource<unit>(TaskCreationOptions.RunContinuationsAsynchronously)
            let calls = ResizeArray<string>()

            let compiler path cancellationToken =
                calls.Add path
                compilerEntered.TrySetResult(()) |> ignore

                task {
                    do! Task.Delay(Timeout.Infinite, cancellationToken)
                    return Failed Array.empty
                }

            let loading =
                ScriptPipelineLoader.loadWithCompilerAsync
                    compiler
                    configuration.ClientToServerPluginPaths
                    configuration.ServerToClientPluginPaths
                    cancellation.Token

            do! compilerEntered.Task
            cancellation.Cancel()

            let! _ =
                Assert.ThrowsAnyAsync<OperationCanceledException>(fun () -> loading :> Task)

            calls |> Seq.toArray |> should equal [| Path.GetFullPath firstPath |]
        }

    [<Fact>]
    let ``cancellation after a completed script prevents later compilation and listener startup`` () =
        task {
            use workspace = new TestWorkspace()
            let firstPath =
                workspace.WriteSource(
                    "First.fsx",
                    identitySource "FirstPlugin" "FirstProcessor"
                )

            let secondPath =
                workspace.WriteSource(
                    "Second.fsx",
                    identitySource "SecondPlugin" "SecondProcessor"
                )

            use reservedListener = new System.Net.Sockets.TcpListener(IPAddress.Loopback, 0)
            reservedListener.Start()
            let listenPort =
                (reservedListener.LocalEndpoint :?> System.Net.IPEndPoint).Port

            let arguments =
                Array.concat
                    [| [| "--listen-address"
                          string IPAddress.Loopback
                          "--listen-port"
                          string listenPort
                          "--backend-host"
                          string IPAddress.Loopback
                          "--backend-port"
                          "6000" |]
                       [| "--client-to-server-plugin"; firstPath
                          "--client-to-server-plugin"; secondPath |] |]

            let configuration =
                match ForwardingConfiguration.tryParseArguments arguments with
                | Ok value -> value
                | Error error -> failwithf "Test configuration was invalid: %s" error

            use cancellation = new CancellationTokenSource()
            let calls = ResizeArray<string>()

            let compiler path cancellationToken =
                task {
                    calls.Add path
                    let! result =
                        workspace.Compiler.CompileAndLoadAsync(path, cancellationToken)

                    if path = Path.GetFullPath firstPath then
                        cancellation.Cancel()

                    return result
                }

            let! _ =
                Assert.ThrowsAnyAsync<OperationCanceledException>(fun () ->
                    ScriptTcpForwarder.runWithCompilerAsync
                        compiler
                        configuration
                        NullLogger.Instance
                        cancellation.Token
                    :> Task)

            calls |> Seq.toArray |> should equal [| Path.GetFullPath firstPath |]
        }

    [<Fact>]
    let ``unrequested compiler cancellation remains an independent cancellation failure`` () =
        task {
            use workspace = new TestWorkspace()
            let scriptPath = workspace.WriteSource("Unrequested.fsx", "")
            let configuration = createConfiguration [| scriptPath |] Array.empty
            let unrelatedCancellation = OperationCanceledException("independent cancellation")

            let! error =
                Assert.ThrowsAnyAsync<OperationCanceledException>(fun () ->
                    ScriptPipelineLoader.loadWithCompilerAsync
                        (fun _ _ -> Task.FromException<ScriptCompilationResult>(unrelatedCancellation))
                        configuration.ClientToServerPluginPaths
                        configuration.ServerToClientPluginPaths
                        CancellationToken.None
                    :> Task)

            error.Message |> should equal unrelatedCancellation.Message
        }

    [<Fact>]
    let ``loader reports every compiler and discovery failure as startup configuration`` () =
        task {
            use workspace = new TestWorkspace()

            let failures =
                [| Path.Combine(Path.GetTempPath(), Guid.NewGuid().ToString "N", "Missing.fsx")
                   workspace.WriteSource(
                       "CompileError.fsx",
                       "module CompileError\nlet value: int = \"wrong\""
                   )
                   workspace.WriteSource(
                       "Forbidden.fsx",
                       "#r \"unavailable.dll\"\nmodule Forbidden"
                   )
                   workspace.WriteSource(
                       "NoProcessor.fsx",
                       "module NoProcessor\nlet value = 42"
                   )
                   workspace.WriteSource(
                       "Ambiguous.fsx",
                       String.concat
                           Environment.NewLine
                           [ "module Ambiguous"
                             "open System.Threading.Tasks"
                             "open PipeL4.Plugins"
                             "let run input output ="
                             "    output.AppendInput(0L, input.Bytes.Length)"
                             "    ValueTask<ProcessorResult>({ RetainedLength = 0L })"
                             "type First() ="
                             "    interface IByteStreamProcessor with"
                             "        member _.ProcessAsync(input, output, _) = run input output"
                             "type Second() ="
                             "    interface IByteStreamProcessor with"
                             "        member _.ProcessAsync(input, output, _) = run input output" ]
                   ) |]

            for path in failures do
                let configuration = createConfiguration [| path |] Array.empty

                let! error =
                    Assert.ThrowsAsync<ScriptPluginConfigurationException>(fun () ->
                        ScriptPipelineLoader.loadWithCompilerAsync
                            (fun scriptPath cancellationToken ->
                                workspace.Compiler.CompileAndLoadAsync(
                                    scriptPath,
                                    cancellationToken
                                ))
                            configuration.ClientToServerPluginPaths
                            configuration.ServerToClientPluginPaths
                            CancellationToken.None
                        :> System.Threading.Tasks.Task)

                let fileName =
                    Path.GetFileName path
                    |> Option.ofObj
                    |> Option.defaultWith (fun () -> failwith "The test script path has no file name.")

                error.Message.Contains(fileName, StringComparison.Ordinal)
                |> should equal true
        }

    [<Fact>]
    let ``configured processor failures include script direction and stage context`` () =
        task {
            use workspace = new TestWorkspace()

            let source =
                String.concat
                    Environment.NewLine
                    [ "module FailingContext"
                      "open System"
                      "open System.Threading.Tasks"
                      "open PipeL4.Plugins"
                      "type Processor() ="
                      "    interface IByteStreamProcessor with"
                      "        member _.ProcessAsync(_, _, _) ="
                      "            invalidOp \"processor failure\""
                      "            ValueTask<ProcessorResult>({ RetainedLength = 0L })" ]

            let scriptPath = workspace.WriteSource("FailingContext.fsx", source)
            let configuration = createConfiguration [| scriptPath |] Array.empty

            let! definition =
                ScriptPipelineLoader.loadWithCompilerAsync
                    (fun path cancellationToken ->
                        workspace.Compiler.CompileAndLoadAsync(path, cancellationToken))
                    configuration.ClientToServerPluginPaths
                    configuration.ServerToClientPluginPaths
                    CancellationToken.None

            let! pipelines =
                definition.CreateProcessorPipelinesAsync(ConnectionState()).AsTask()

            use pipeline =
                new ProcessorPipeline(
                    pipelines.ClientToBackend,
                    discardSink,
                    MemoryPool<byte>.Shared
                )

            let! error =
                Assert.ThrowsAsync<ScriptProcessorFailureException>(fun () ->
                    pipeline.PushAsync(
                        new TestOwner([| 1uy |]),
                        1,
                        CancellationToken.None
                    )
                    :> Task)

            error.ScriptPath |> should equal (Path.GetFullPath scriptPath)
            error.Direction |> should equal "client-to-server"
            error.StageNumber |> should equal 1

            error.InnerException
            |> Option.ofObj
            |> Option.map _.Message
            |> should equal (Some "processor failure")
        }

    [<Fact>]
    let ``post-return script contract failures keep script direction and stage context`` () =
        task {
            use workspace = new TestWorkspace()

            let cases: (string * string * string * (ProcessorPipeline -> Task)) array =
                [| ("InvalidRetention",
                    String.concat
                        Environment.NewLine
                        [ "module InvalidRetention"
                          "open System.Threading.Tasks"
                          "open PipeL4.Plugins"
                          "type Processor() ="
                          "    interface IByteStreamProcessor with"
                          "        member _.ProcessAsync(input, _, _) ="
                          "            ValueTask<ProcessorResult>({ RetainedLength = input.Bytes.Length + 1L })" ],
                    "retained",
                    (fun pipeline ->
                        pipeline.PushAsync(
                            new TestOwner([| 1uy |]),
                            1,
                            CancellationToken.None
                        )
                        :> Task))
                   ("UnresolvedEof",
                    String.concat
                        Environment.NewLine
                        [ "module UnresolvedEof"
                          "open System.Threading.Tasks"
                          "open PipeL4.Plugins"
                          "type Processor() ="
                          "    interface IByteStreamProcessor with"
                          "        member _.ProcessAsync(input, _, _) ="
                          "            ValueTask<ProcessorResult>({ RetainedLength = input.Bytes.Length })" ],
                    "upstream completion",
                    (fun pipeline ->
                        task {
                            do!
                                pipeline.PushAsync(
                                    new TestOwner([| 1uy |]),
                                    1,
                                    CancellationToken.None
                                )

                            do! pipeline.CompleteAsync(CancellationToken.None)
                        }
                        :> Task))
                   ("MalformedGeneratedOutput",
                    String.concat
                        Environment.NewLine
                        [ "module MalformedGeneratedOutput"
                          "open System"
                          "open System.Threading.Tasks"
                          "open PipeL4.Plugins"
                          "type Processor() ="
                          "    interface IByteStreamProcessor with"
                          "        member _.ProcessAsync(_, output, _) ="
                          "            output.GetMemory(1) |> ignore"
                          "            try output.AppendInput(0L, 0L) with :? InvalidOperationException -> ()"
                          "            ValueTask<ProcessorResult>({ RetainedLength = 0L })" ],
                    "advanced",
                    (fun pipeline ->
                        pipeline.PushAsync(
                            new TestOwner([| 1uy |]),
                            1,
                            CancellationToken.None
                        )
                        :> Task)) |]

            for name, source, expectedMessage, execute in cases do
                let scriptPath = workspace.WriteSource($"{name}.fsx", source)
                let configuration = createConfiguration [| scriptPath |] Array.empty

                let! definition =
                    ScriptPipelineLoader.loadWithCompilerAsync
                        (fun path cancellationToken ->
                            workspace.Compiler.CompileAndLoadAsync(path, cancellationToken))
                        configuration.ClientToServerPluginPaths
                        configuration.ServerToClientPluginPaths
                        CancellationToken.None

                let! pipelines =
                    definition.CreateProcessorPipelinesAsync(ConnectionState()).AsTask()

                use pipeline =
                    new ProcessorPipeline(
                        pipelines.ClientToBackend,
                        discardSink,
                        MemoryPool<byte>.Shared
                    )

                let! error =
                    Assert.ThrowsAsync<ScriptProcessorFailureException>(fun () ->
                        execute pipeline)

                error.ScriptPath |> should equal (Path.GetFullPath scriptPath)
                error.Direction |> should equal "client-to-server"
                error.StageNumber |> should equal 1

                (innerMessage error).Contains(expectedMessage, StringComparison.Ordinal)
                |> should equal true
        }

    [<Fact>]
    let ``partial construction failure disposes completed stages and definition remains reusable`` () =
        task {
            use workspace = new TestWorkspace()

            let source =
                """module ConstructionLifecycle

open System
open System.Collections.Generic
open System.Threading
open System.Threading.Tasks
open PipeL4.Plugins

type Coordination private () =
    static let events = ResizeArray<string>()
    static let mutable asyncDisposed = 0
    static let mutable syncDisposed = 0
    static member RecordAsync(instanceNumber: int) =
        events.Add(string instanceNumber)
        Interlocked.Increment(&asyncDisposed) |> ignore
    static member RecordSync() = Interlocked.Increment(&syncDisposed) |> ignore
    static member Events = String.Join(",", events)
    static member AsyncDisposed = Volatile.Read(&asyncDisposed)
    static member SyncDisposed = Volatile.Read(&syncDisposed)

type Processor() =
    static let mutable nextInstance = 0
    let instanceNumber = Interlocked.Increment(&nextInstance)
    do if instanceNumber = 2 then invalidOp "constructor two failed"

    interface IByteStreamProcessor with
        member _.ProcessAsync(input, output, _) =
            output.AppendInput(0L, input.Bytes.Length)
            ValueTask<ProcessorResult>({ RetainedLength = 0L })

    interface IDisposable with
        member _.Dispose() = Coordination.RecordSync()

    interface IAsyncDisposable with
        member _.DisposeAsync() =
            Coordination.RecordAsync(instanceNumber)
            ValueTask()
"""

            let scriptPath = workspace.WriteSource("ConstructionLifecycle.fsx", source)
            let! result = workspace.Compiler.CompileAndLoadAsync(scriptPath, CancellationToken.None)
            let plugin = expectCompiled result
            let configuration = createConfiguration [| scriptPath; scriptPath |] Array.empty

            let! definition =
                ScriptPipelineLoader.loadWithCompilerAsync
                    (fun _ _ -> Task.FromResult(Compiled(plugin, Array.empty)))
                    configuration.ClientToServerPluginPaths
                    configuration.ServerToClientPluginPaths
                    CancellationToken.None

            let! error =
                Assert.ThrowsAsync<ScriptProcessorFailureException>(fun () ->
                    definition.CreateProcessorPipelinesAsync(ConnectionState()).AsTask()
                    :> Task)

            error.Direction |> should equal "client-to-server"
            error.StageNumber |> should equal 2
            innerMessage error |> should equal "constructor two failed"

            let coordination = requireType plugin.Assembly "Coordination"
            staticProperty<string> coordination "Events" |> should equal "1"
            staticProperty<int> coordination "AsyncDisposed" |> should equal 1
            staticProperty<int> coordination "SyncDisposed" |> should equal 0

            let! pipelines =
                definition.CreateProcessorPipelinesAsync(ConnectionState()).AsTask()

            let! disposalFailures =
                ProcessorLifetime.disposeAllAsync pipelines.ClientToBackend
                |> _.AsTask()

            disposalFailures |> should be Empty
            staticProperty<string> coordination "Events" |> should equal "1,4,3"
            staticProperty<int> coordination "AsyncDisposed" |> should equal 3
            staticProperty<int> coordination "SyncDisposed" |> should equal 0
        }

    [<Fact>]
    let ``compiled script disposal failures retain stage context and do not skip earlier stages`` () =
        task {
            use workspace = new TestWorkspace()

            let source =
                """module DisposalLifecycle

open System
open System.Collections.Generic
open System.Threading
open System.Threading.Tasks
open PipeL4.Plugins

type Coordination private () =
    static let events = ResizeArray<string>()
    static let mutable syncDisposed = 0
    static member Record(instanceNumber: int) = events.Add(string instanceNumber)
    static member RecordSync() = Interlocked.Increment(&syncDisposed) |> ignore
    static member Events = String.Join(",", events)
    static member SyncDisposed = Volatile.Read(&syncDisposed)

type Processor() =
    static let mutable nextInstance = 0
    let instanceNumber = Interlocked.Increment(&nextInstance)

    interface IByteStreamProcessor with
        member _.ProcessAsync(input, output, _) =
            output.AppendInput(0L, input.Bytes.Length)
            ValueTask<ProcessorResult>({ RetainedLength = 0L })

    interface IDisposable with
        member _.Dispose() = Coordination.RecordSync()

    interface IAsyncDisposable with
        member _.DisposeAsync() =
            Coordination.Record(instanceNumber)
            if instanceNumber = 2 then
                ValueTask(Task.FromException(InvalidOperationException("dispose two failed")))
            else
                ValueTask()
"""

            let scriptPath = workspace.WriteSource("DisposalLifecycle.fsx", source)
            let! result = workspace.Compiler.CompileAndLoadAsync(scriptPath, CancellationToken.None)
            let plugin = expectCompiled result
            let configuration = createConfiguration [| scriptPath; scriptPath |] Array.empty

            let! definition =
                ScriptPipelineLoader.loadWithCompilerAsync
                    (fun _ _ -> Task.FromResult(Compiled(plugin, Array.empty)))
                    configuration.ClientToServerPluginPaths
                    configuration.ServerToClientPluginPaths
                    CancellationToken.None

            let! pipelines =
                definition.CreateProcessorPipelinesAsync(ConnectionState()).AsTask()
            let pipeline =
                new ProcessorPipeline(
                    pipelines.ClientToBackend,
                    discardSink,
                    MemoryPool<byte>.Shared
                )

            let! error =
                Assert.ThrowsAsync<ScriptProcessorFailureException>(fun () ->
                    (pipeline :> IAsyncDisposable).DisposeAsync().AsTask())

            error.Direction |> should equal "client-to-server"
            error.StageNumber |> should equal 2
            innerMessage error |> should equal "dispose two failed"

            let coordination = requireType plugin.Assembly "Coordination"
            staticProperty<string> coordination "Events" |> should equal "2,1"
            staticProperty<int> coordination "SyncDisposed" |> should equal 0

            do! (pipeline :> IAsyncDisposable).DisposeAsync().AsTask()
            staticProperty<string> coordination "Events" |> should equal "2,1"
        }

    [<Fact>]
    let ``independent scripts share typed state across ordered and repeated stages`` () =
        task {
            use workspace = new TestWorkspace()

            let writerSource =
                """module SharedWriter

open System
open System.Collections.Concurrent
open System.Threading
open System.Threading.Tasks
open PipeL4.Plugins

type Coordination private () =
    static let references = ConcurrentQueue<obj>()
    static let instances = ConcurrentQueue<int>()
    static let mutable nextInstance = 0
    static member NextInstance() = Interlocked.Increment(&nextInstance)
    static member Record(instanceNumber: int, value: obj) =
        instances.Enqueue(instanceNumber)
        references.Enqueue(value)
    static member References = references.ToArray()
    static member Instances = instances.ToArray()

type Processor(connectionState: IConnectionState) =
    let instanceNumber = Coordination.NextInstance()

    interface IByteStreamProcessor with
        member _.ProcessAsync(input, output, _) =
            if not input.Bytes.IsEmpty then
                let shared =
                    connectionState.GetOrAdd(
                        "requests",
                        fun () -> ConcurrentDictionary<string, int64>()
                    )

                shared.AddOrUpdate("count", 1L, fun _ current -> current + 1L)
                |> ignore
                Coordination.Record(instanceNumber, shared)

            output.AppendInput(0L, input.Bytes.Length)
            ValueTask<ProcessorResult>({ RetainedLength = 0L })
"""

            let readerSource =
                """module SharedReader

open System
open System.Collections.Concurrent
open System.Threading.Tasks
open PipeL4.Plugins

type Coordination private () =
    static let references = ConcurrentQueue<obj>()
    static let observations = ConcurrentQueue<int64>()
    static let mutable missingWasAbsent = false
    static let mutable mismatchWasRejected = false
    static let mutable recoveredOriginal = false
    static member Record(value: obj, count: int64) =
        references.Enqueue(value)
        observations.Enqueue(count)
    static member MissingWasAbsent with get () = missingWasAbsent and set value = missingWasAbsent <- value
    static member MismatchWasRejected with get () = mismatchWasRejected and set value = mismatchWasRejected <- value
    static member RecoveredOriginal with get () = recoveredOriginal and set value = recoveredOriginal <- value
    static member References = references.ToArray()
    static member Observations = observations.ToArray()

type Processor(connectionState: IConnectionState) =
    interface IByteStreamProcessor with
        member _.ProcessAsync(input, output, _) =
            if not input.Bytes.IsEmpty then
                Coordination.MissingWasAbsent <-
                    connectionState.TryGet<obj>("missing").IsNone

                let shared =
                    connectionState.TryGet<ConcurrentDictionary<string, int64>>("requests")
                    |> Option.defaultWith (fun () -> invalidOp "Shared state was missing.")

                Coordination.MismatchWasRejected <-
                    try
                        connectionState.TryGet<int>("requests") |> ignore
                        false
                    with :? InvalidOperationException ->
                        true

                let recovered =
                    connectionState.TryGet<ConcurrentDictionary<string, int64>>("requests")
                    |> Option.defaultWith (fun () -> invalidOp "Shared state was corrupted.")

                Coordination.RecoveredOriginal <- Object.ReferenceEquals(shared, recovered)
                Coordination.Record(shared, shared["count"])

            output.AppendInput(0L, input.Bytes.Length)
            ValueTask<ProcessorResult>({ RetainedLength = 0L })
"""

            let writerPath = workspace.WriteSource("SharedWriter.fsx", writerSource)
            let readerPath = workspace.WriteSource("SharedReader.fsx", readerSource)
            let configuration =
                createConfiguration
                    [| writerPath; readerPath; writerPath |]
                    Array.empty

            let! writerResult =
                workspace.Compiler.CompileAndLoadAsync(writerPath, CancellationToken.None)

            let! readerResult =
                workspace.Compiler.CompileAndLoadAsync(readerPath, CancellationToken.None)

            let writerPlugin = expectCompiled writerResult
            let readerPlugin = expectCompiled readerResult

            let! definition =
                ScriptPipelineLoader.loadWithCompilerAsync
                    (fun path _ ->
                        if path = Path.GetFullPath writerPath then
                            Task.FromResult(Compiled(writerPlugin, Array.empty))
                        else
                            Task.FromResult(Compiled(readerPlugin, Array.empty)))
                    configuration.ClientToServerPluginPaths
                    configuration.ServerToClientPluginPaths
                    CancellationToken.None

            let runConnection () =
                task {
                    let state = ConnectionState()
                    let! pipelines =
                        definition.CreateProcessorPipelinesAsync(state).AsTask()

                    let pipeline =
                        new ProcessorPipeline(
                            pipelines.ClientToBackend,
                            discardSink,
                            MemoryPool<byte>.Shared
                        )

                    do!
                        pipeline.PushAsync(
                            new TestOwner([| 1uy |]),
                            1,
                            CancellationToken.None
                        )

                    do! pipeline.CompleteAsync CancellationToken.None
                    do! (pipeline :> IAsyncDisposable).DisposeAsync().AsTask()
                    do! (state :> IAsyncDisposable).DisposeAsync().AsTask()
                }

            do! runConnection ()
            do! runConnection ()

            let writerCoordination = requireType writerPlugin.Assembly "Coordination"
            let readerCoordination = requireType readerPlugin.Assembly "Coordination"
            let writerReferences = staticProperty<obj array> writerCoordination "References"
            let readerReferences = staticProperty<obj array> readerCoordination "References"

            writerReferences.Length |> should equal 4
            readerReferences.Length |> should equal 2
            Object.ReferenceEquals(writerReferences[0], writerReferences[1])
            |> should equal true
            Object.ReferenceEquals(writerReferences[0], readerReferences[0])
            |> should equal true
            Object.ReferenceEquals(writerReferences[2], writerReferences[3])
            |> should equal true
            Object.ReferenceEquals(writerReferences[2], readerReferences[1])
            |> should equal true
            Object.ReferenceEquals(writerReferences[0], writerReferences[2])
            |> should equal false

            staticProperty<int array> writerCoordination "Instances"
            |> Array.sort
            |> should equal [| 1; 2; 3; 4 |]

            staticProperty<int64 array> readerCoordination "Observations"
            |> should equal [| 1L; 1L |]

            staticProperty<bool> readerCoordination "MissingWasAbsent"
            |> should equal true
            staticProperty<bool> readerCoordination "MismatchWasRejected"
            |> should equal true
            staticProperty<bool> readerCoordination "RecoveredOriginal"
            |> should equal true
        }
