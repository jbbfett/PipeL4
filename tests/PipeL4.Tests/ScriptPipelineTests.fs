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

            let! firstConnection = definition.CreateProcessorPipelinesAsync().AsTask()
            let! secondConnection = definition.CreateProcessorPipelinesAsync().AsTask()

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

            let! pipelines = definition.CreateProcessorPipelinesAsync().AsTask()

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

                let! pipelines = definition.CreateProcessorPipelinesAsync().AsTask()

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
                    definition.CreateProcessorPipelinesAsync().AsTask() :> Task)

            error.Direction |> should equal "client-to-server"
            error.StageNumber |> should equal 2
            innerMessage error |> should equal "constructor two failed"

            let coordination = requireType plugin.Assembly "Coordination"
            staticProperty<string> coordination "Events" |> should equal "1"
            staticProperty<int> coordination "AsyncDisposed" |> should equal 1
            staticProperty<int> coordination "SyncDisposed" |> should equal 0

            let! pipelines = definition.CreateProcessorPipelinesAsync().AsTask()

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

            let! pipelines = definition.CreateProcessorPipelinesAsync().AsTask()
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
