namespace PipeL4.Tests

open System
open System.Buffers
open System.IO
open System.Net
open System.Threading
open System.Threading.Tasks
open FsUnit.Xunit
open PipeL4.Core
open PipeL4.Plugins
open Xunit

module ExampleProcessorTests =
    let private examplePath =
        Path.GetFullPath(Path.Combine(__SOURCE_DIRECTORY__, "..", "..", "examples", "AsciiUppercase.fsx"))

    let private sequenceSegmentCount (sequence: ReadOnlySequence<byte>) =
        let mutable position = sequence.Start
        let mutable segment = ReadOnlyMemory<byte>.Empty
        let mutable count = 0

        while sequence.TryGet(&position, &segment) do
            count <- count + 1

        count

    type private TestWorkspace() =
        let rootPath =
            Path.Combine(Path.GetTempPath(), "PipeL4.Tests", Guid.NewGuid().ToString "N")

        let compiler = ScriptCompiler(Path.Combine(rootPath, "compiler"))

        member _.Compiler = compiler

        interface IDisposable with
            member _.Dispose() =
                if Directory.Exists rootPath then
                    Directory.Delete(rootPath, recursive = true)

    type private TrackingOwner(buffer: byte array) =
        let mutable disposeCount = 0

        member _.Buffer = buffer
        member _.DisposeCount = Volatile.Read(&disposeCount)

        interface IMemoryOwner<byte> with
            member _.Memory = Memory<byte>(buffer)

            member _.Dispose() =
                Interlocked.Increment(&disposeCount) |> ignore

    type private TrackingPool() =
        inherit MemoryPool<byte>()

        let owners = ResizeArray<TrackingOwner>()

        member _.Owners = owners
        override _.MaxBufferSize = Int32.MaxValue

        override _.Rent(sizeHint) =
            let owner = new TrackingOwner(Array.zeroCreate<byte> (max sizeHint 1))
            owners.Add owner
            owner :> IMemoryOwner<byte>

        override _.Dispose(_disposing) = ()

    type private RecordingSink() =
        let writes = ResizeArray<byte array>()
        let mutable completionCount = 0

        member _.Bytes = writes |> Seq.toArray |> Array.concat
        member _.CompletionCount = completionCount

        interface IProcessorPipelineSink with
            member _.WriteAsync(sequence, cancellationToken) =
                cancellationToken.ThrowIfCancellationRequested()
                let bytes = Array.zeroCreate<byte> (int sequence.Length)
                sequence.CopyTo(bytes.AsSpan())
                writes.Add bytes
                ValueTask()

            member _.CompleteAsync(cancellationToken) =
                cancellationToken.ThrowIfCancellationRequested()
                completionCount <- completionCount + 1
                ValueTask()

    let private createConfiguration scriptPath =
        let arguments =
            [| "--listen-address"
               string IPAddress.Loopback
               "--listen-port"
               "5000"
               "--backend-host"
               string IPAddress.Loopback
               "--backend-port"
               "6000"
               "--client-to-server-plugin"
               scriptPath |]

        match ForwardingConfiguration.tryParseArguments arguments with
        | Ok configuration -> configuration
        | Error error -> failwithf "Test configuration was invalid: %s" error

    let private loadDefinition (workspace: TestWorkspace) =
        task {
            let configuration = createConfiguration examplePath

            let! definition =
                ScriptPipelineLoader.loadWithCompilerAsync
                    (fun path cancellationToken ->
                        workspace.Compiler.CompileAndLoadAsync(path, cancellationToken))
                    configuration.ClientToServerPluginPaths
                    configuration.ServerToClientPluginPaths
                    CancellationToken.None

            return definition
        }

    let private runPipeline createStages chunks =
        task {
            use workspace = new TestWorkspace()
            let! definition = loadDefinition workspace
            let state = ConnectionState()
            let! pipelines = definition.CreateProcessorPipelinesAsync(state).AsTask()
            let pool = new TrackingPool()
            let sink = new RecordingSink()
            let pipeline =
                new ProcessorPipeline(
                    createStages pipelines.ClientToBackend,
                    sink,
                    pool
                )

            let owners =
                chunks
                |> Array.map (fun bytes -> new TrackingOwner(bytes))

            for owner in owners do
                do! pipeline.PushAsync(owner, owner.Buffer.Length, CancellationToken.None)

            do! pipeline.CompleteAsync(CancellationToken.None)
            let metrics = pipeline.GetStageMetricsSnapshot()
            do! (pipeline :> IAsyncDisposable).DisposeAsync().AsTask()
            do! (state :> IAsyncDisposable).DisposeAsync().AsTask()

            return sink, pool, owners, metrics
        }

    let private ascii (text: string) =
        System.Text.Encoding.ASCII.GetBytes text

    let private coreCompleted retainedLength =
        let result: PipeL4.Core.ProcessorResult =
            { RetainedLength = retainedLength }

        ValueTask<PipeL4.Core.ProcessorResult>(result)

    let private collectThreeSegments () =
        let mutable invocation = 0

        { new PipeL4.Core.IByteStreamProcessor with
            member _.ProcessAsync(input, output, _) =
                invocation <- invocation + 1

                match invocation with
                | 1
                | 2 ->
                    coreCompleted input.Bytes.Length
                | 3 ->
                    sequenceSegmentCount input.Bytes |> should equal 3
                    output.AppendInput(0L, input.Bytes.Length)
                    coreCompleted 0L
                | _ -> coreCompleted 0L }

    let private observeSegments
        (segmentCounts: ResizeArray<int>)
        (processor: PipeL4.Core.IByteStreamProcessor)
        =
        { new PipeL4.Core.IByteStreamProcessor with
            member _.ProcessAsync(input, output, cancellationToken) =
                if not input.Bytes.IsEmpty then
                    segmentCounts.Add(sequenceSegmentCount input.Bytes)

                processor.ProcessAsync(input, output, cancellationToken)

          interface IAsyncDisposable with
              member _.DisposeAsync() =
                  match processor with
                  | :? IAsyncDisposable as disposable -> disposable.DisposeAsync()
                  | :? IDisposable as disposable ->
                      disposable.Dispose()
                      ValueTask()
                  | _ -> ValueTask() }

    [<Fact>]
    let ``repository example compiles and transforms real segmented input`` () =
        task {
            let! sink, pool, owners, _ =
                runPipeline
                    id
                    [| ascii "aZ-"
                       [| 0x62uy; 0x31uy; 0x80uy |]
                       ascii "z!" |]

            sink.Bytes |> should equal [| 0x41uy; 0x5Auy; 0x2Duy; 0x42uy; 0x31uy; 0x80uy; 0x5Auy; 0x21uy |]
            sink.CompletionCount |> should equal 1
            owners |> Array.forall (fun owner -> owner.DisposeCount = 1) |> should equal true
            pool.Owners |> Seq.forall (fun owner -> owner.DisposeCount = 1) |> should equal true
        }

    [<Fact>]
    let ``repository example handles empty input and EOF completion`` () =
        task {
            let! sink, pool, owners, _ = runPipeline id Array.empty

            sink.Bytes |> should be Empty
            sink.CompletionCount |> should equal 1
            owners |> should be Empty
            pool.Owners |> should be Empty
        }

    [<Fact>]
    let ``repository example transforms three segments in one loaded-processor invocation`` () =
        task {
            let observedSegmentCounts = ResizeArray<int>()

            let! sink, pool, owners, metrics =
                runPipeline
                    (fun configured ->
                        [| collectThreeSegments ()
                           observeSegments observedSegmentCounts configured[0] |])
                    [| ascii "a"
                       ascii "b2"
                       ascii "c!" |]

            observedSegmentCounts |> Seq.toArray |> should equal [| 3 |]
            sink.Bytes |> should equal [| 0x41uy; 0x42uy; 0x32uy; 0x43uy; 0x21uy |]
            sink.CompletionCount |> should equal 1
            metrics |> List.map _.GeneratedOutputBytes |> should equal [ 0L; 5L ]
            metrics |> List.map _.InputBytes |> should equal [ 5L; 5L ]
            owners |> Array.forall (fun owner -> owner.DisposeCount = 1) |> should equal true
            pool.Owners |> Seq.forall (fun owner -> owner.DisposeCount = 1) |> should equal true
        }
