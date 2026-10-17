namespace PipeL4.Tests

open System
open System.Buffers
open System.IO
open System.Runtime.Loader
open System.Runtime.InteropServices
open System.Text
open System.Threading
open System.Threading.Tasks
open FsUnit.Xunit
open PipeL4.Plugins
open Xunit

module ScriptCompilerTests =
    let private bytes (text: string) = Encoding.ASCII.GetBytes text

    let private sequenceBytes (sequence: ReadOnlySequence<byte>) =
        let result = Array.zeroCreate<byte> (int sequence.Length)
        sequence.CopyTo(result.AsSpan())
        result

    type private TestOwner(buffer: byte array) =
        member _.Buffer = buffer

        interface IMemoryOwner<byte> with
            member _.Memory = Memory<byte>(buffer)
            member _.Dispose() = ()

    type private TestWorkspace() =
        let rootPath =
            Path.Combine(Path.GetTempPath(), "PipeL4.Tests", Guid.NewGuid().ToString "N")

        let sourceRoot = Path.Combine(rootPath, "sources")
        let compilerRoot = Path.Combine(rootPath, "compiler")
        let compiler = ScriptCompiler compilerRoot

        do Directory.CreateDirectory(sourceRoot) |> ignore

        member _.Compiler = compiler

        member _.CompilerRoot = compilerRoot

        member _.WriteSource(fileName: string, source: string) =
            let path = Path.Combine(sourceRoot, fileName)
            File.WriteAllText(path, source)
            path

        interface IDisposable with
            member _.Dispose() =
                if Directory.Exists rootPath then
                    Directory.Delete(rootPath, recursive = true)

    let private compile (compiler: ScriptCompiler) path =
        compiler.CompileAndLoadAsync(path, CancellationToken.None)

    let private withIdentityProcessor source =
        source
        + """

open System.Threading.Tasks
open PipeL4.Plugins

type Processor() =
    interface IByteStreamProcessor with
        member _.ProcessAsync(input, output, _) =
            output.AppendInput(0L, input.Bytes.Length)
            ValueTask<ProcessorResult>({ RetainedLength = 0L })
"""

    let private processorType declaration extraMembers =
        String.concat
            Environment.NewLine
            [ declaration
              extraMembers
              "    interface IByteStreamProcessor with"
              "        member _.ProcessAsync(input, output, _) ="
              "            output.AppendInput(0L, input.Bytes.Length)"
              "            ValueTask<ProcessorResult>({ RetainedLength = 0L })" ]

    let private processorSource types =
        String.concat
            Environment.NewLine
            [ "module ProcessorCandidates"
              ""
              "open System.Threading.Tasks"
              "open PipeL4.Plugins"
              ""
              String.concat (Environment.NewLine + Environment.NewLine) types ]

    let private diagnosticText diagnostics =
        diagnostics
        |> Array.map (fun value -> value.Message)
        |> String.concat Environment.NewLine

    let private expectCompiledPlugin result =
        match result with
        | Compiled(plugin, diagnostics) -> plugin, diagnostics
        | Failed diagnostics ->
            failwithf "Expected compilation to succeed:%s%s" Environment.NewLine (diagnosticText diagnostics)

    let private expectCompiled result =
        let plugin, diagnostics = expectCompiledPlugin result
        plugin.Assembly, diagnostics

    let private expectFailed result =
        match result with
        | Failed diagnostics -> diagnostics
        | Compiled(plugin, _) ->
            failwithf "Expected compilation to fail, but assembly '%s' was loaded." plugin.Assembly.FullName

    let private compilerArtifacts (workspace: TestWorkspace) =
        if Directory.Exists workspace.CompilerRoot then
            Directory.EnumerateFileSystemEntries workspace.CompilerRoot
            |> Seq.toArray
        else
            Array.empty

    [<Fact>]
    let ``compiles an ordinary script against standard NET and FSharp libraries`` () =
        task {
            use workspace = new TestWorkspace()

            let scriptPath =
                workspace.WriteSource(
                    "StandardLibraries.fsx",
                    withIdentityProcessor
                        """module StandardLibraries

let joined = System.String.Join("-", [ "fsharp"; "script" ])
let count = [ 1; 2; 3 ] |> List.length
"""
                )

            let! result = compile workspace.Compiler scriptPath
            let assembly, diagnostics = expectCompiled result

            let assemblyName =
                assembly.GetName().Name
                |> Option.ofObj
                |> Option.defaultWith (fun () -> failwith "The loaded assembly has no name.")

            assemblyName.StartsWith("PipeL4.Plugin.", StringComparison.Ordinal)
            |> should equal true

            diagnostics
            |> Array.exists (fun value -> value.Severity = ScriptDiagnosticSeverity.Error)
            |> should equal false
        }

    [<Fact>]
    let ``loads the compiled assembly for ordinary reflection and use`` () =
        task {
            use workspace = new TestWorkspace()

            let scriptPath =
                workspace.WriteSource(
                    "InspectionFixture.fsx",
                    withIdentityProcessor
                        """namespace InspectionFixture

type Probe() =
    member _.Value = System.String.Concat("load", "ed")
"""
                )

            let! result = compile workspace.Compiler scriptPath
            let assembly, _ = expectCompiled result

            AssemblyLoadContext.GetLoadContext assembly
            |> should be (sameAs AssemblyLoadContext.Default)

            let probeType =
                match assembly.GetType("InspectionFixture.Probe", throwOnError = true) with
                | null -> failwith "The declared probe type was not present in the loaded assembly."
                | value -> value

            let probe = Activator.CreateInstance probeType

            let property =
                match probeType.GetProperty "Value" with
                | null -> failwith "The declared probe property was not present in the loaded assembly."
                | value -> value

            let value = property.GetValue probe

            value |> should equal (box "loaded")
        }

    [<Fact>]
    let ``returns compiler errors with source positions and no assembly`` () =
        task {
            use workspace = new TestWorkspace()

            let scriptPath =
                workspace.WriteSource(
                    "InvalidScript.fsx",
                    """module InvalidScript

let value: int = "wrong"
"""
                )

            let! result = compile workspace.Compiler scriptPath
            let diagnostics = expectFailed result

            let compilerError =
                diagnostics
                |> Array.find (fun value -> value.Severity = ScriptDiagnosticSeverity.Error)

            compilerError.CompilerCode.IsSome |> should equal true
            compilerError.SourcePath |> should equal (Some(Path.GetFullPath scriptPath))
            compilerError.Range.IsSome |> should equal true
            compilerError.Range.Value.Start.Line |> should be (greaterThan 0)
        }

    [<Fact>]
    let ``preserves warnings separately from errors`` () =
        task {
            use workspace = new TestWorkspace()

            let scriptPath =
                workspace.WriteSource(
                    "WarningScript.fsx",
                    withIdentityProcessor
                        """module WarningScript

let describe value =
    match value with
    | 0 -> "zero"
"""
                )

            let! result = compile workspace.Compiler scriptPath
            let _, diagnostics = expectCompiled result

            diagnostics
            |> Array.exists (fun value -> value.Severity = ScriptDiagnosticSeverity.Warning)
            |> should equal true

            diagnostics
            |> Array.exists (fun value -> value.Severity = ScriptDiagnosticSeverity.Error)
            |> should equal false
        }

    [<Fact>]
    let ``returns controlled failures for missing and non-fsx sources`` () =
        task {
            use workspace = new TestWorkspace()
            let missingPath = Path.Combine(workspace.CompilerRoot, "Missing.fsx")
            let! missingResult = compile workspace.Compiler missingPath
            let missingDiagnostics = expectFailed missingResult

            missingDiagnostics
            |> Array.exists (fun value ->
                value.Severity = ScriptDiagnosticSeverity.Error
                && value.Message.Contains("could not be read", StringComparison.OrdinalIgnoreCase))
            |> should equal true

            let sourcePath = workspace.WriteSource("NotAScript.fs", "module NotAScript")
            let! extensionResult = compile workspace.Compiler sourcePath
            let extensionDiagnostics = expectFailed extensionResult

            extensionDiagnostics
            |> Array.exists (fun value ->
                value.Severity = ScriptDiagnosticSeverity.Error
                && value.Message.Contains(".fsx", StringComparison.Ordinal))
            |> should equal true
        }

    [<Fact>]
    let ``rejects dependency-expanding script directives before compilation`` () =
        task {
            use workspace = new TestWorkspace()

            let directives =
                [| "#r \"nuget: Newtonsoft.Json\""
                   "#r \"local-library.dll\""
                   "#I \"/tmp/arbitrary-assemblies\""
                   "#i \"nuget: https://example.invalid/v3/index.json\""
                   "#load \"Other.fsx\"" |]

            for index, directive in Array.indexed directives do
                let scriptPath =
                    workspace.WriteSource(
                        $"Forbidden{index}.fsx",
                        $"""{directive}
module Forbidden{index}
"""
                    )

                let! result = compile workspace.Compiler scriptPath
                let diagnostics = expectFailed result

                diagnostics
                |> Array.exists (fun value ->
                    value.Severity = ScriptDiagnosticSeverity.Error
                    && value.Message.Contains("not supported", StringComparison.OrdinalIgnoreCase)
                    && value.Range |> Option.exists (fun range -> range.Start.Line = 1))
                |> should equal true
        }

    [<Fact>]
    let ``does not expose incidental PipeL4 or compiler assemblies`` () =
        task {
            use workspace = new TestWorkspace()

            let sources =
                [| """module CompilerAssemblyLeak
open FSharp.Compiler.CodeAnalysis
let checkerType = typeof<FSharpChecker>
"""
                   """module PipeL4AssemblyLeak
open PipeL4.Core
let configurationType = typeof<ForwardingConfiguration>
""" |]

            for index, source in Array.indexed sources do
                let scriptPath = workspace.WriteSource($"AssemblyLeak{index}.fsx", source)
                let! result = compile workspace.Compiler scriptPath
                let diagnostics = expectFailed result

                diagnostics
                |> Array.exists (fun value -> value.Severity = ScriptDiagnosticSeverity.Error)
                |> should equal true
        }

    [<Fact>]
    let ``removes temporary compiler artifacts after success and failure`` () =
        task {
            use workspace = new TestWorkspace()
            let validPath =
                workspace.WriteSource(
                    "Valid.fsx",
                    withIdentityProcessor "module Valid\nlet value = 42"
                )
            let invalidPath = workspace.WriteSource("Invalid.fsx", "module Invalid\nlet value: int = \"wrong\"")

            let! validResult = compile workspace.Compiler validPath
            expectCompiled validResult |> ignore
            compilerArtifacts workspace |> should be Empty

            let! invalidResult = compile workspace.Compiler invalidPath
            expectFailed invalidResult |> ignore
            compilerArtifacts workspace |> should be Empty
        }

    [<Fact>]
    let ``supports concurrent reuse with unique assembly identities`` () =
        task {
            use workspace = new TestWorkspace()
            let firstPath =
                workspace.WriteSource(
                    "First.fsx",
                    withIdentityProcessor "module First\nlet value = 1"
                )

            let secondPath =
                workspace.WriteSource(
                    "Second.fsx",
                    withIdentityProcessor "module Second\nlet value = 2"
                )

            let! results =
                Task.WhenAll(
                    compile workspace.Compiler firstPath,
                    compile workspace.Compiler secondPath
                )

            let firstAssembly, _ = expectCompiled results[0]
            let secondAssembly, _ = expectCompiled results[1]

            firstAssembly.FullName |> should not' (equal secondAssembly.FullName)
            compilerArtifacts workspace |> should be Empty
        }

    [<Fact>]
    let ``discovers one processor and defers construction`` () =
        task {
            use workspace = new TestWorkspace()

            let source =
                processorSource
                    [ processorType
                          "type DeferredProcessor() ="
                          "    do invalidOp \"constructor failure\"" ]

            let scriptPath = workspace.WriteSource("DeferredProcessor.fsx", source)
            let! result = compile workspace.Compiler scriptPath
            let plugin, _ = expectCompiledPlugin result

            plugin.ProcessorType.Name |> should equal "DeferredProcessor"

            let error =
                Assert.Throws<InvalidOperationException>(fun () ->
                    plugin.CreateProcessor() |> ignore)

            error.Message |> should equal "constructor failure"
        }

    [<Fact>]
    let ``creates a fresh adapted processor for each request`` () =
        task {
            use workspace = new TestWorkspace()

            let scriptPath =
                workspace.WriteSource(
                    "FreshProcessor.fsx",
                    processorSource [ processorType "type FreshProcessor() =" "" ]
                )

            let! result = compile workspace.Compiler scriptPath
            let plugin, _ = expectCompiledPlugin result
            let first = plugin.CreateProcessor()
            let second = plugin.CreateProcessor()

            Object.ReferenceEquals(first, second) |> should equal false
        }

    [<Fact>]
    let ``compiled processor runs through retained segmented input and mixed output`` () =
        task {
            use workspace = new TestWorkspace()

            let source =
                """module CompiledMixedProcessor

open System
open System.Threading.Tasks
open PipeL4.Plugins

type Processor() =
    interface IByteStreamProcessor with
        member _.ProcessAsync(input, output, cancellationToken) =
            cancellationToken.ThrowIfCancellationRequested()

            if not input.IsCompleted && input.Bytes.Length < 6L then
                ValueTask<ProcessorResult>({ RetainedLength = input.Bytes.Length })
            elif input.Bytes.IsEmpty then
                ValueTask<ProcessorResult>({ RetainedLength = 0L })
            else
                output.AppendInput(0L, 2L)
                let generated = [| 88uy; 89uy; 90uy |]
                generated.AsSpan().CopyTo(output.GetSpan(generated.Length))
                output.Advance(generated.Length)
                generated.AsSpan().Clear()
                output.AppendInput(4L, 2L)
                ValueTask<ProcessorResult>({ RetainedLength = 0L })
"""

            let scriptPath = workspace.WriteSource("CompiledMixedProcessor.fsx", source)
            let! result = compile workspace.Compiler scriptPath
            let plugin, _ = expectCompiledPlugin result
            let writes = ResizeArray<byte array>()
            let mutable preservedInputBacking = false
            let first = new TestOwner(bytes "AB")
            let second = new TestOwner(bytes "CDEF")

            let sink =
                { new PipeL4.Core.IProcessorPipelineSink with
                    member _.WriteAsync(value, cancellationToken) =
                        cancellationToken.ThrowIfCancellationRequested()

                        let segments = ResizeArray<ReadOnlyMemory<byte>>()
                        let mutable position = value.Start
                        let mutable segment = ReadOnlyMemory<byte>.Empty

                        while value.TryGet(&position, &segment) do
                            segments.Add segment

                        segments.Count |> should equal 3

                        let mutable firstSegment = Unchecked.defaultof<ArraySegment<byte>>
                        let mutable lastSegment = Unchecked.defaultof<ArraySegment<byte>>

                        MemoryMarshal.TryGetArray(segments[0], &firstSegment)
                        |> should equal true

                        MemoryMarshal.TryGetArray(segments[2], &lastSegment)
                        |> should equal true

                        preservedInputBacking <-
                            Object.ReferenceEquals(firstSegment.Array, first.Buffer)
                            && Object.ReferenceEquals(lastSegment.Array, second.Buffer)

                        writes.Add(sequenceBytes value)
                        ValueTask()

                    member _.CompleteAsync(cancellationToken) =
                        cancellationToken.ThrowIfCancellationRequested()
                        ValueTask() }

            use pipeline =
                new PipeL4.Core.ProcessorPipeline(
                    [| plugin.CreateProcessor() |],
                    sink,
                    MemoryPool<byte>.Shared
                )

            do! pipeline.PushAsync(first, 2, CancellationToken.None)
            writes |> should be Empty
            do! pipeline.PushAsync(second, 4, CancellationToken.None)
            do! pipeline.CompleteAsync CancellationToken.None

            writes
            |> Seq.toArray
            |> Array.concat
            |> Encoding.ASCII.GetString
            |> should equal "ABXYZEF"

            preservedInputBacking |> should equal true
        }

    [<Fact>]
    let ``rejects scripts without a usable processor entry point`` () =
        task {
            use workspace = new TestWorkspace()

            let candidates =
                [| "NoProcessor", "module NoProcessor\nlet value = 42"
                   "PrivateProcessor",
                   processorSource [ processorType "type private PrivateProcessor() =" "" ]
                   "AbstractProcessor",
                   processorSource
                       [ processorType
                             "[<AbstractClass>]\ntype AbstractProcessor() ="
                             "    abstract Marker: unit -> unit" ]
                   "GenericProcessor",
                   processorSource [ processorType "type GenericProcessor<'T>() =" "" ]
                   "ConstructedProcessor",
                   processorSource [ processorType "type ConstructedProcessor(value: int) =" "" ] |]

            for name, source in candidates do
                let scriptPath = workspace.WriteSource($"{name}.fsx", source)
                let! result = compile workspace.Compiler scriptPath
                let diagnostics = expectFailed result

                diagnostics
                |> Array.exists (fun value ->
                    value.Severity = ScriptDiagnosticSeverity.Error
                    && value.Message.Contains("no usable implementation", StringComparison.Ordinal))
                |> should equal true
        }

    [<Fact>]
    let ``rejects ambiguous processor entry points and names the candidates`` () =
        task {
            use workspace = new TestWorkspace()

            let source =
                processorSource
                    [ processorType "type FirstProcessor() =" ""
                      processorType "type SecondProcessor() =" "" ]

            let scriptPath = workspace.WriteSource("AmbiguousProcessors.fsx", source)
            let! result = compile workspace.Compiler scriptPath
            let diagnostics = expectFailed result

            let diagnostic =
                diagnostics
                |> Array.find (fun value ->
                    value.Severity = ScriptDiagnosticSeverity.Error
                    && value.Message.Contains("multiple implementations", StringComparison.Ordinal))

            diagnostic.Message.Contains("FirstProcessor", StringComparison.Ordinal)
            |> should equal true

            diagnostic.Message.Contains("SecondProcessor", StringComparison.Ordinal)
            |> should equal true
        }
