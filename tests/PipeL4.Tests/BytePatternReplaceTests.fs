namespace PipeL4.Tests

open System
open System.Buffers
open System.IO
open System.Net
open System.Text
open System.Threading
open System.Threading.Tasks
open FsUnit.Xunit
open PipeL4.Core
open PipeL4.Plugins
open Xunit

module BytePatternReplaceTests =
    let private examplePath =
        Path.GetFullPath(Path.Combine(__SOURCE_DIRECTORY__, "..", "..", "examples", "BytePatternReplace.fsx"))

    let private ascii (value: string) = Encoding.ASCII.GetBytes value

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

    [<NoEquality; NoComparison>]
    type private RunResult =
        { Bytes: byte array
          CompletionCount: int
          Metrics: ProcessorStageMetricsSnapshot list
          Logical: CapacityUsageSnapshot
          Physical: ConnectionMemorySnapshot
          Owners: TrackingOwner array
          Pool: TrackingPool }

    [<NoEquality; NoComparison>]
    type private ProductionRunResult =
        { Bytes: byte array
          CompletionCount: int
          Logical: CapacityUsageSnapshot
          PhysicalShare: CapacityUsageSnapshot
          Physical: ConnectionMemorySnapshot }

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

    let private loadDefinition (workspace: TestWorkspace) clientToServerPaths serverToClientPaths =
        task {
            let configuration = createConfiguration clientToServerPaths serverToClientPaths

            return!
                ScriptPipelineLoader.loadWithCompilerAsync
                    (fun path cancellationToken ->
                        workspace.Compiler.CompileAndLoadAsync(path, cancellationToken))
                    configuration.ClientToServerPluginPaths
                    configuration.ServerToClientPluginPaths
                    CancellationToken.None
        }

    let private runStages
        (stages: PipeL4.Core.IByteStreamProcessor array)
        (chunks: byte array array)
        (logicalCapacityLimit: int64)
        =
        task {
            let pool = new TrackingPool()
            let sink = new RecordingSink()
            let accounting = new ConnectionMemoryAccounting()
            let logicalCapacity = new CapacityBudget(logicalCapacityLimit)
            let pipeline =
                new ProcessorPipeline(
                    stages,
                    sink,
                    pool,
                    accounting,
                    logicalCapacity
                )

            let owners = chunks |> Array.map (fun buffer -> new TrackingOwner(buffer))

            for owner in owners do
                do! pipeline.PushAsync(owner, owner.Buffer.Length, CancellationToken.None)

            do! pipeline.CompleteAsync(CancellationToken.None)

            let result =
                { Bytes = sink.Bytes
                  CompletionCount = sink.CompletionCount
                  Metrics = pipeline.GetStageMetricsSnapshot()
                  Logical = pipeline.GetLogicalCapacitySnapshot()
                  Physical = pipeline.GetConnectionMemorySnapshot()
                  Owners = owners
                  Pool = pool }

            do! (pipeline :> IAsyncDisposable).DisposeAsync().AsTask()

            owners |> Array.forall (fun owner -> owner.DisposeCount = 1) |> should equal true
            pool.Owners |> Seq.forall (fun owner -> owner.DisposeCount = 1) |> should equal true

            return result
        }

    let private runConnection
        (definition: ScriptPipelineDefinition)
        (chunks: byte array array)
        (logicalCapacityLimit: int64)
        (createStages:
            PipeL4.Core.IByteStreamProcessor array -> PipeL4.Core.IByteStreamProcessor array)
        =
        task {
            let state = ConnectionState()
            let! pipelines = definition.CreateProcessorPipelinesAsync(state).AsTask()
            let! result = runStages (createStages pipelines.ClientToBackend) chunks logicalCapacityLimit
            do! (state :> IAsyncDisposable).DisposeAsync().AsTask()
            return result
        }

    let private runProductionDirectional
        (definition: ScriptPipelineDefinition)
        (chunks: byte array array)
        (logicalCapacityLimit: int64)
        (connectionPhysicalCapacityLimit: int64)
        =
        task {
            let state = ConnectionState()
            let limits =
                ConnectionCapacityLimits(
                    logicalCapacityLimit,
                    Int64.MaxValue,
                    connectionPhysicalCapacityLimit
                )

            let capacityAccounting = new ConnectionCapacityAccounting(limits)
            let! pipelines = definition.CreateProcessorPipelinesAsync(state).AsTask()
            let sink = new RecordingSink()
            let pipeline =
                new ProcessorPipeline(
                    pipelines.ClientToBackend,
                    sink,
                    MemoryPool<byte>.Shared,
                    capacityAccounting.ClientToBackendPhysical,
                    capacityAccounting.ClientToBackendLogical
                )

            for chunk in chunks do
                let owner = MemoryPool<byte>.Shared.Rent(max chunk.Length 1)

                if chunk.Length > 0 then
                    chunk.AsSpan().CopyTo(owner.Memory.Span)

                do! pipeline.PushAsync(owner, chunk.Length, CancellationToken.None)

            do! pipeline.CompleteAsync(CancellationToken.None)
            do! (pipeline :> IAsyncDisposable).DisposeAsync().AsTask()

            let snapshot = capacityAccounting.GetSnapshot()

            let result =
                { Bytes = sink.Bytes
                  CompletionCount = sink.CompletionCount
                  Logical = snapshot.ClientToBackendLogical
                  PhysicalShare = snapshot.ClientToBackendPhysicalShare
                  Physical = snapshot.Physical }

            do! (state :> IAsyncDisposable).DisposeAsync().AsTask()
            capacityAccounting.Dispose()
            return result
        }

    let private coreCompleted retainedLength =
        let result: PipeL4.Core.ProcessorResult =
            { RetainedLength = retainedLength }

        ValueTask<PipeL4.Core.ProcessorResult>(result)

    let private retainUntilThirdInvocation () =
        let mutable invocation = 0

        { new PipeL4.Core.IByteStreamProcessor with
            member _.ProcessAsync(input, output, cancellationToken) =
                cancellationToken.ThrowIfCancellationRequested()
                invocation <- invocation + 1

                if invocation < 3 then
                    coreCompleted input.Bytes.Length
                else
                    output.AppendInput(0L, input.Bytes.Length)
                    coreCompleted 0L }

    type private ObservingProcessor(
        inner: PipeL4.Core.IByteStreamProcessor,
        observe: ReadOnlySequence<byte> -> unit
    ) =
        interface PipeL4.Core.IByteStreamProcessor with
            member _.ProcessAsync(input, output, cancellationToken) =
                if not input.Bytes.IsEmpty then
                    observe input.Bytes

                inner.ProcessAsync(input, output, cancellationToken)

        interface IAsyncDisposable with
            member _.DisposeAsync() =
                match inner with
                | :? IAsyncDisposable as disposable -> disposable.DisposeAsync()
                | :? IDisposable as disposable ->
                    disposable.Dispose()
                    ValueTask()
                | _ -> ValueTask()

    [<Fact>]
    let ``complete HELLO in one segment is replaced`` () =
        task {
            use workspace = new TestWorkspace()
            let! definition = loadDefinition workspace [| examplePath |] Array.empty
            let! result = runConnection definition [| ascii "HELLO" |] Int64.MaxValue id

            result.Bytes |> should equal (ascii "WORLD")
            result.CompletionCount |> should equal 1
        }

    [<Fact>]
    let ``every split position across separate pushes has the same result`` () =
        task {
            use workspace = new TestWorkspace()
            let! definition = loadDefinition workspace [| examplePath |] Array.empty
            let input = ascii "HELLO"

            for split in 1 .. input.Length - 1 do
                let chunks = [| input[0 .. split - 1]; input[split ..] |]
                let! result = runConnection definition chunks Int64.MaxValue id
                result.Bytes |> should equal (ascii "WORLD")
                result.CompletionCount |> should equal 1
        }

    [<Fact>]
    let ``one invocation containing multiple input segments is processed as one stream`` () =
        task {
            use workspace = new TestWorkspace()
            let! definition = loadDefinition workspace [| examplePath |] Array.empty
            let observedSegmentCounts = ResizeArray<int>()

            let createStages (configured: PipeL4.Core.IByteStreamProcessor array) :
                PipeL4.Core.IByteStreamProcessor array =
                [| retainUntilThirdInvocation ()
                   (new ObservingProcessor(
                       configured[0],
                       fun sequence -> observedSegmentCounts.Add(sequenceSegmentCount sequence)
                   ) :> PipeL4.Core.IByteStreamProcessor) |]

            let! result =
                runConnection
                    definition
                    [| ascii "HE"; ascii "L"; ascii "LO" |]
                    Int64.MaxValue
                    createStages

            observedSegmentCounts |> Seq.toArray |> should equal [| 3 |]
            result.Bytes |> should equal (ascii "WORLD")
            result.CompletionCount |> should equal 1
        }

    [<Fact>]
    let ``adjacent repeated and near-miss patterns preserve non-matches`` () =
        task {
            use workspace = new TestWorkspace()
            let! definition = loadDefinition workspace [| examplePath |] Array.empty

            let cases =
                [| ascii "HELLOHELLO", ascii "WORLDWORLD"
                   ascii "prefixHELLXsuffix", ascii "prefixHELLXsuffix"
                   ascii "HEHELLO", ascii "HEWORLD"
                   ascii "aHELLOHELLOz", ascii "aWORLDWORLDz" |]

            for input, expected in cases do
                let! result = runConnection definition [| input |] Int64.MaxValue id
                result.Bytes |> should equal expected
        }

    [<Fact>]
    let ``incomplete matches flush unchanged at EOF and empty input completes`` () =
        task {
            use workspace = new TestWorkspace()
            let! definition = loadDefinition workspace [| examplePath |] Array.empty

            for length in 1 .. 4 do
                let partial = ("HELLO").Substring(0, length) |> ascii
                let! result = runConnection definition [| partial |] Int64.MaxValue id
                result.Bytes |> should equal partial
                result.CompletionCount |> should equal 1

            let! empty = runConnection definition Array.empty Int64.MaxValue id
            empty.Bytes |> should be Empty
            empty.CompletionCount |> should equal 1
        }

    [<Fact>]
    let ``binary data and zero bytes are preserved`` () =
        task {
            use workspace = new TestWorkspace()
            let! definition = loadDefinition workspace [| examplePath |] Array.empty

            let input =
                [| 0uy; 0xFFuy; byte 'H'; 0uy; byte 'E'; byte 'L'; byte 'L'; byte 'O'; 0uy
                   byte 'H'; byte 'E'; byte 'L'; byte 'L'; byte 'O'; 0x80uy |]

            let expected =
                [| 0uy; 0xFFuy; byte 'H'; 0uy; byte 'E'; byte 'L'; byte 'L'; byte 'O'; 0uy
                   byte 'W'; byte 'O'; byte 'R'; byte 'L'; byte 'D'; 0x80uy |]

            let! result = runConnection definition [| input |] Int64.MaxValue id
            result.Bytes |> should equal expected
            result.Bytes.Length |> should equal input.Length
        }

    [<Fact>]
    let ``output is independent of input partitioning`` () =
        task {
            use workspace = new TestWorkspace()
            let! definition = loadDefinition workspace [| examplePath |] Array.empty

            let input = Array.concat [| ascii "xHELLOHE-HELLO"; [| 0uy |]; ascii "HELLO" |]
            let expected = Array.concat [| ascii "xWORLDHE-WORLD"; [| 0uy |]; ascii "WORLD" |]

            let partitions =
                [| [| input |]
                   Array.init input.Length (fun index -> [| input[index] |])
                   [| input[0 .. 2]; input[3 .. 8]; input[9 ..] |] |]

            for chunks in partitions do
                let! result = runConnection definition chunks Int64.MaxValue id
                result.Bytes |> should equal expected
        }

    [<Fact>]
    let ``state is isolated across interleaved live connections and directions`` () =
        task {
            use workspace = new TestWorkspace()
            let! definition = loadDefinition workspace [| examplePath |] [| examplePath |]

            let stateA = ConnectionState()
            let stateB = ConnectionState()
            let! pipelinesA = definition.CreateProcessorPipelinesAsync(stateA).AsTask()
            let! pipelinesB = definition.CreateProcessorPipelinesAsync(stateB).AsTask()
            let sinkA = new RecordingSink()
            let sinkB = new RecordingSink()
            let poolA = new TrackingPool()
            let poolB = new TrackingPool()
            let accountingA = new ConnectionMemoryAccounting()
            let accountingB = new ConnectionMemoryAccounting()
            let pipelineA =
                new ProcessorPipeline(
                    pipelinesA.ClientToBackend,
                    sinkA,
                    poolA,
                    accountingA
                )

            let pipelineB =
                new ProcessorPipeline(
                    pipelinesB.BackendToClient,
                    sinkB,
                    poolB,
                    accountingB
                )

            let a1 = new TrackingOwner(ascii "HEL")
            let b1 = new TrackingOwner(ascii "HE")
            let a2 = new TrackingOwner(ascii "L")
            let b2 = new TrackingOwner(ascii "LL")
            let a3 = new TrackingOwner(ascii "X")
            let b3 = new TrackingOwner(ascii "O")

            // Both processors have partial prefixes pending before either
            // stream receives its next bytes. The pushes and completions are
            // deliberately interleaved without relying on timing.
            do! pipelineA.PushAsync(a1, a1.Buffer.Length, CancellationToken.None)
            do! pipelineB.PushAsync(b1, b1.Buffer.Length, CancellationToken.None)
            do! pipelineA.PushAsync(a2, a2.Buffer.Length, CancellationToken.None)
            do! pipelineB.PushAsync(b2, b2.Buffer.Length, CancellationToken.None)
            do! pipelineA.PushAsync(a3, a3.Buffer.Length, CancellationToken.None)
            do! pipelineB.PushAsync(b3, b3.Buffer.Length, CancellationToken.None)
            do! pipelineB.CompleteAsync(CancellationToken.None)
            do! pipelineA.CompleteAsync(CancellationToken.None)

            sinkA.Bytes |> should equal (ascii "HELLX")
            sinkB.Bytes |> should equal (ascii "WORLD")
            sinkA.CompletionCount |> should equal 1
            sinkB.CompletionCount |> should equal 1

            do! (pipelineB :> IAsyncDisposable).DisposeAsync().AsTask()
            do! (pipelineA :> IAsyncDisposable).DisposeAsync().AsTask()
            do! (stateB :> IAsyncDisposable).DisposeAsync().AsTask()
            do! (stateA :> IAsyncDisposable).DisposeAsync().AsTask()
            (accountingB :> IDisposable).Dispose()
            (accountingA :> IDisposable).Dispose()

            [| a1; a2; a3 |]
            |> Array.forall (fun owner -> owner.DisposeCount = 1)
            |> should equal true

            [| b1; b2; b3 |]
            |> Array.forall (fun owner -> owner.DisposeCount = 1)
            |> should equal true
            poolA.Owners |> Seq.forall (fun owner -> owner.DisposeCount = 1) |> should equal true
            poolB.Owners |> Seq.forall (fun owner -> owner.DisposeCount = 1) |> should equal true
        }

    [<Fact>]
    let ``batched output keeps 5000 non-matching bytes within the finite physical share`` () =
        task {
            use workspace = new TestWorkspace()
            let! definition = loadDefinition workspace [| examplePath |] Array.empty
            let input = Array.create 5_000 0x41uy
            let! result =
                runProductionDirectional
                    definition
                    [| input |]
                    12_000L
                    155_000L

            result.Bytes |> should equal input
            result.CompletionCount |> should equal 1
            result.PhysicalShare.LimitBytes |> should equal 77_500L
            result.PhysicalShare.CurrentBytes |> should equal 0L
            result.PhysicalShare.PeakBytes
            |> should be (lessThanOrEqualTo result.PhysicalShare.LimitBytes)
            result.PhysicalShare.PeakBytes |> should be (lessThan 38_750L)
            result.Physical.CurrentPhysicalBufferCapacityBytes |> should equal 0L
            result.Logical.LimitBytes |> should equal 12_000L
            result.Logical.CurrentBytes |> should equal 0L
            result.Logical.PeakBytes |> should be (lessThanOrEqualTo 12_000L)
        }

    [<Fact>]
    let ``one byte output fits the 256 byte directional share`` () =
        task {
            use workspace = new TestWorkspace()
            let! definition = loadDefinition workspace [| examplePath |] Array.empty
            let! result =
                runProductionDirectional
                    definition
                    [| ascii "A" |]
                    2L
                    512L

            result.Bytes |> should equal (ascii "A")
            result.CompletionCount |> should equal 1
            result.PhysicalShare.LimitBytes |> should equal 256L
            result.PhysicalShare.CurrentBytes |> should equal 0L
            result.PhysicalShare.PeakBytes
            |> should be (lessThanOrEqualTo result.PhysicalShare.LimitBytes)
            result.PhysicalShare.PeakBytes |> should be (lessThan 256L)
            result.Physical.CurrentPhysicalBufferCapacityBytes |> should equal 0L
            result.Logical.LimitBytes |> should equal 2L
            result.Logical.CurrentBytes |> should equal 0L
            result.Logical.PeakBytes |> should equal 2L
        }

    [<Fact>]
    let ``output sizes around the batch boundary use bounded partial batches`` () =
        task {
            use workspace = new TestWorkspace()
            let! definition = loadDefinition workspace [| examplePath |] Array.empty

            for length in [| 255; 256; 257 |] do
                let input = Array.create length 0x41uy
                let! result =
                    runProductionDirectional
                        definition
                        [| input |]
                        (int64 (2 * length))
                        8_192L

                result.Bytes |> should equal input
                result.CompletionCount |> should equal 1
                result.PhysicalShare.LimitBytes |> should equal 4_096L
                result.PhysicalShare.CurrentBytes |> should equal 0L
                result.PhysicalShare.PeakBytes
                |> should be (lessThanOrEqualTo result.PhysicalShare.LimitBytes)
                result.Physical.CurrentPhysicalBufferCapacityBytes |> should equal 0L
                result.Logical.LimitBytes |> should equal (int64 (2 * length))
                result.Logical.CurrentBytes |> should equal 0L
                result.Logical.PeakBytes |> should equal (int64 (2 * length))
        }

    [<Fact>]
    let ``delayed output after HELL fits finite logical and physical limits`` () =
        task {
            use workspace = new TestWorkspace()
            let! definition = loadDefinition workspace [| examplePath |] Array.empty
            let! result =
                runProductionDirectional
                    definition
                    [| ascii "HELL"; ascii "X" |]
                    6L
                    512L

            result.Bytes |> should equal (ascii "HELLX")
            result.CompletionCount |> should equal 1
            result.PhysicalShare.LimitBytes |> should equal 256L
            result.PhysicalShare.CurrentBytes |> should equal 0L
            result.PhysicalShare.PeakBytes
            |> should be (lessThanOrEqualTo result.PhysicalShare.LimitBytes)
            result.Physical.CurrentPhysicalBufferCapacityBytes |> should equal 0L
            result.Logical.LimitBytes |> should equal 6L
            result.Logical.CurrentBytes |> should equal 0L
            result.Logical.PeakBytes |> should equal 6L
        }

    [<Fact>]
    let ``EOF flushes a pending prefix within finite capacity`` () =
        task {
            use workspace = new TestWorkspace()
            let! definition = loadDefinition workspace [| examplePath |] Array.empty
            let! result =
                runProductionDirectional
                    definition
                    [| ascii "H"; ascii "E"; ascii "L"; ascii "L" |]
                    4L
                    512L

            result.Bytes |> should equal (ascii "HELL")
            result.CompletionCount |> should equal 1
            result.PhysicalShare.LimitBytes |> should equal 256L
            result.PhysicalShare.CurrentBytes |> should equal 0L
            result.PhysicalShare.PeakBytes
            |> should be (lessThanOrEqualTo result.PhysicalShare.LimitBytes)
            result.Physical.CurrentPhysicalBufferCapacityBytes |> should equal 0L
            result.Logical.LimitBytes |> should equal 4L
            result.Logical.CurrentBytes |> should equal 0L
            result.Logical.PeakBytes |> should equal 4L
        }

    [<Fact>]
    let ``matching state stays bounded without runtime retention`` () =
        task {
            use workspace = new TestWorkspace()
            let! definition = loadDefinition workspace [| examplePath |] Array.empty
            let input = Array.concat (Array.init 20 (fun _ -> ascii "HELLO"))
            let chunks = input |> Array.map (fun value -> [| value |])
            let! result = runConnection definition chunks 6L id

            result.Bytes |> should equal (Array.concat (Array.init 20 (fun _ -> ascii "WORLD")))
            result.Bytes.Length |> should equal input.Length
            result.Logical.CurrentBytes |> should equal 0L
            result.Logical.PeakBytes |> should be (lessThanOrEqualTo 6L)
            result.Physical.CurrentPhysicalBufferCapacityBytes |> should equal 0L
            result.Physical.PeakPhysicalBufferCapacityBytes
            |> should be (lessThanOrEqualTo 257L)

            let stage = result.Metrics |> List.head
            stage.CurrentRetainedBytes |> should equal 0L
            stage.InputBytes |> should equal (int64 input.Length)
            stage.GeneratedOutputBytes |> should equal (int64 input.Length)
            stage.InputBackedOutputBytes |> should equal 0L
        }
