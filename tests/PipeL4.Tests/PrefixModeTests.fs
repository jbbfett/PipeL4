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

module PrefixModeTests =
    let private examplePath =
        Path.GetFullPath(Path.Combine(__SOURCE_DIRECTORY__, "..", "..", "examples", "PrefixMode.fsx"))

    let private ascii (value: string) = System.Text.Encoding.ASCII.GetBytes value

    let private segmentCount (sequence: ReadOnlySequence<byte>) =
        let mutable position = sequence.Start
        let mutable segment = ReadOnlyMemory<byte>.Empty
        let mutable count = 0
        while sequence.TryGet(&position, &segment) do count <- count + 1
        count

    type private Workspace() =
        let root = Path.Combine(Path.GetTempPath(), "PipeL4.Tests", Guid.NewGuid().ToString "N")
        let compiler = ScriptCompiler(Path.Combine(root, "compiler"))
        member _.Compiler = compiler
        interface IDisposable with
            member _.Dispose() =
                if Directory.Exists root then Directory.Delete(root, recursive = true)

    type private Owner(buffer: byte array) =
        let mutable disposed = 0
        member _.DisposeCount = Volatile.Read(&disposed)
        interface IMemoryOwner<byte> with
            member _.Memory = Memory<byte>(buffer)
            member _.Dispose() = Interlocked.Increment(&disposed) |> ignore

    type private Pool() =
        inherit MemoryPool<byte>()
        let owners = ResizeArray<Owner>()
        let requests = ResizeArray<int>()
        member _.Owners = owners
        // Input owners are supplied directly by push; this pool records generated-output leases.
        member _.Requests = requests
        override _.MaxBufferSize = Int32.MaxValue
        override _.Rent(sizeHint) =
            requests.Add sizeHint
            let owner = new Owner(Array.zeroCreate<byte> (max sizeHint 1))
            owners.Add owner
            owner :> IMemoryOwner<byte>
        override _.Dispose(_disposing) = ()

    type private Sink() =
        let writes = ResizeArray<byte array>()
        let mutable completions = 0
        member _.Bytes = writes |> Seq.toArray |> Array.concat
        member _.Completions = completions
        interface IProcessorPipelineSink with
            member _.WriteAsync(sequence, token) =
                token.ThrowIfCancellationRequested()
                let bytes = Array.zeroCreate<byte> (int sequence.Length)
                sequence.CopyTo(bytes.AsSpan())
                writes.Add bytes
                ValueTask()
            member _.CompleteAsync(token) =
                token.ThrowIfCancellationRequested()
                completions <- completions + 1
                ValueTask()

    [<NoEquality; NoComparison>]
    type private Harness =
        { State: ConnectionState
          Capacity: ConnectionCapacityAccounting
          Pool: Pool
          Sink: Sink
          Pipeline: ProcessorPipeline
          Inputs: ResizeArray<Owner>
          SegmentCounts: ResizeArray<int> }

    [<NoEquality; NoComparison>]
    type private Result =
        { Bytes: byte array
          Completions: int
          Metrics: ProcessorStageMetricsSnapshot list
          InputOwnerCount: int
          OutputLeaseCount: int
          OutputLeaseRequests: int array
          Capacity: ConnectionCapacitySnapshot }

    let private loadDefinition (workspace: Workspace) =
        task {
            let args =
                [| "--listen-address"; string IPAddress.Loopback
                   "--listen-port"; "5000"
                   "--backend-host"; string IPAddress.Loopback
                   "--backend-port"; "6000"
                   "--client-to-server-plugin"; examplePath |]
            let configuration =
                match ForwardingConfiguration.tryParseArguments args with
                | Ok value -> value
                | Error error -> failwithf "Invalid test configuration: %s" error

            return!
                ScriptPipelineLoader.loadWithCompilerAsync
                    (fun path token -> workspace.Compiler.CompileAndLoadAsync(path, token))
                    configuration.ClientToServerPluginPaths
                    configuration.ServerToClientPluginPaths
                    CancellationToken.None
        }

    let private completed retained =
        let value: PipeL4.Core.ProcessorResult = { RetainedLength = retained }
        ValueTask<PipeL4.Core.ProcessorResult>(value)

    let private holdFirstPush () =
        let mutable hold = true
        { new PipeL4.Core.IByteStreamProcessor with
            member _.ProcessAsync(input, output, token) =
                token.ThrowIfCancellationRequested()
                if hold && not input.IsCompleted then
                    hold <- false
                    completed input.Bytes.Length
                else
                    if not input.Bytes.IsEmpty then
                        output.AppendInput(0L, input.Bytes.Length)
                    completed 0L }

    let private observeSegments (segmentCounts: ResizeArray<int>) (processor: PipeL4.Core.IByteStreamProcessor) =
        { new PipeL4.Core.IByteStreamProcessor with
            member _.ProcessAsync(input, output, token) =
                if input.Bytes.Length >= 4L then
                    segmentCounts.Add(segmentCount input.Bytes)
                processor.ProcessAsync(input, output, token)

          interface IAsyncDisposable with
              member _.DisposeAsync() =
                  match processor with
                  | :? IAsyncDisposable as disposable -> disposable.DisposeAsync()
                  | :? IDisposable as disposable ->
                      disposable.Dispose()
                      ValueTask()
                  | _ -> ValueTask() }

    let private createHarness
        (definition: ScriptPipelineDefinition)
        (logicalLimit: int64)
        (physicalLimit: int64)
        (splitInput: bool)
        =
        task {
            let state = ConnectionState()
            let! scripts = definition.CreateProcessorPipelinesAsync(state).AsTask()
            let limits = ConnectionCapacityLimits(logicalLimit, Int64.MaxValue, physicalLimit)
            let capacity = new ConnectionCapacityAccounting(limits)
            let pool = new Pool()
            let sink = new Sink()
            let counts = ResizeArray<int>()
            let scriptStages =
                if splitInput then
                    scripts.ClientToBackend |> Array.map (observeSegments counts)
                else
                    scripts.ClientToBackend
            let stages =
                if splitInput then Array.append [| holdFirstPush () |] scriptStages
                else scriptStages
            let pipeline =
                new ProcessorPipeline(
                    stages, sink, pool,
                    capacity.ClientToBackendPhysical,
                    capacity.ClientToBackendLogical
                )
            return
                { State = state; Capacity = capacity; Pool = pool; Sink = sink
                  Pipeline = pipeline; Inputs = ResizeArray<Owner>(); SegmentCounts = counts }
        }

    let private push (harness: Harness) (bytes: byte array) =
        let owner = new Owner(bytes)
        harness.Inputs.Add owner
        harness.Pipeline.PushAsync(owner, bytes.Length, CancellationToken.None)

    let private finish (harness: Harness) =
        task {
            do! harness.Pipeline.CompleteAsync(CancellationToken.None)
            let metrics = harness.Pipeline.GetStageMetricsSnapshot()
            let outputLeaseRequests = harness.Pool.Requests.ToArray()
            do! (harness.Pipeline :> IAsyncDisposable).DisposeAsync().AsTask()
            harness.Capacity.CompletePhysicalDirection(true)
            let capacity = harness.Capacity.GetSnapshot()
            do! (harness.State :> IAsyncDisposable).DisposeAsync().AsTask()
            (harness.Capacity :> IDisposable).Dispose()

            harness.Sink.Completions |> should equal 1
            capacity.ClientToBackendLogical.CurrentBytes |> should equal 0L
            capacity.ClientToBackendPhysicalShare.CurrentBytes |> should equal 0L
            capacity.Physical.CurrentPhysicalBufferCapacityBytes |> should equal 0L
            harness.Inputs |> Seq.forall (fun owner -> owner.DisposeCount = 1) |> should equal true
            harness.Pool.Owners |> Seq.forall (fun owner -> owner.DisposeCount = 1) |> should equal true
            return
                { Bytes = harness.Sink.Bytes; Completions = harness.Sink.Completions
                  Metrics = metrics; InputOwnerCount = harness.Inputs.Count
                  OutputLeaseCount = harness.Pool.Owners.Count
                  OutputLeaseRequests = outputLeaseRequests
                  Capacity = capacity }
        }

    let private run definition chunks logicalLimit physicalLimit splitInput =
        task {
            let! harness = createHarness definition logicalLimit physicalLimit splitInput
            for chunk in chunks do do! push harness chunk
            let! result = finish harness
            return result, harness.SegmentCounts.ToArray()
        }

    [<Fact>]
    let mask_prefix_is_preserved_and_binary_suffix_is_handled () =
        task {
            use workspace = new Workspace()
            let! definition = loadDefinition workspace
            let input = Array.concat [| ascii "MASK"; [| 0uy; byte '0'; byte '9'; byte 'A'; 0xFFuy |] |]
            let expected = Array.concat [| ascii "MASK"; [| 0uy; byte 'X'; byte 'X'; byte 'A'; 0xFFuy |] |]
            let! result, _ = run definition [| input |] 64L 1_024L false
            result.Bytes |> should equal expected
            result.Completions |> should equal 1
            let stage = List.last result.Metrics
            stage.InputBackedOutputBytes |> should equal 4L
            stage.GeneratedOutputBytes |> should equal 5L
            result.OutputLeaseRequests |> should equal [| 5 |]
        }

    [<Fact>]
    let generated_output_leases_match_bounded_batch_requests () =
        task {
            use workspace = new Workspace()
            let! definition = loadDefinition workspace
            let cases =
                [| 1, [| 1 |]
                   255, [| 255 |]
                   256, [| 256 |]
                   257, [| 256; 1 |] |]

            for suffixLength, expectedRequests in cases do
                let suffix = Array.create suffixLength (byte '7')
                let expected = Array.concat [| ascii "MASK"; Array.create suffixLength (byte 'X') |]
                let scenarios =
                    [| [| Array.concat [| ascii "MASK"; suffix |] |]
                       [| ascii "M"; ascii "AS"; ascii "K"; suffix |] |]

                for chunks in scenarios do
                    let! result, _ =
                        run definition chunks 2_048L 8_192L false

                    result.Bytes |> should equal expected
                    result.Completions |> should equal 1
                    result.InputOwnerCount |> should equal chunks.Length
                    result.OutputLeaseCount |> should equal expectedRequests.Length
                    result.OutputLeaseRequests |> should equal expectedRequests
                    result.Capacity.ClientToBackendLogical.PeakBytes
                    |> should be (lessThanOrEqualTo 2_048L)
                    result.Capacity.ClientToBackendPhysicalShare.PeakBytes
                    |> should be (lessThanOrEqualTo 4_096L)
                    result.Capacity.Physical.PeakPhysicalBufferCapacityBytes
                    |> should be (lessThanOrEqualTo 8_192L)
        }

    [<Fact>]
    let every_two_push_prefix_split_is_handled () =
        task {
            use workspace = new Workspace()
            let! definition = loadDefinition workspace
            for split in 1 .. 3 do
                let prefix = ascii "MASK"
                let chunks = [| prefix[0 .. split - 1]; Array.concat [| prefix[split ..]; ascii "4a" |] |]
                let! result, _ = run definition chunks 64L 1_024L false
                result.Bytes |> should equal (ascii "MASKXa")
        }

    [<Fact>]
    let one_byte_pushes_and_later_bytes_keep_the_selected_mode () =
        task {
            use workspace = new Workspace()
            let! definition = loadDefinition workspace
            let chunks = ascii "MASK012MASK3" |> Array.map (fun value -> [| value |])
            let! result, _ = run definition chunks 128L 2_048L false
            result.Bytes |> should equal (ascii "MASKXXXMASKX")
        }

    [<Fact>]
    let multi_segment_invocation_can_contain_the_prefix () =
        task {
            use workspace = new Workspace()
            let! definition = loadDefinition workspace
            let! result, counts = run definition [| ascii "MA"; ascii "SK8" |] 64L 1_024L true
            counts |> should contain 2
            result.Bytes |> should equal (ascii "MASKX")
        }

    [<Fact>]
    let nonmatching_prefix_selects_permanent_passthrough () =
        task {
            use workspace = new Workspace()
            let! definition = loadDefinition workspace
            let! result, _ = run definition [| ascii "PASS"; ascii "MASK123" |] 64L 1_024L false
            result.Bytes |> should equal (ascii "PASSMASK123")
        }

    [<Fact>]
    let push_boundaries_publish_only_decided_prefix_data () =
        task {
            use workspace = new Workspace()
            let! definition = loadDefinition workspace

            let! matching = createHarness definition 128L 2_048L false
            do! push matching (ascii "MAS")
            matching.Sink.Bytes |> should be Empty
            do! push matching (ascii "K123")
            matching.Sink.Bytes |> should equal (ascii "MASKXXX")
            let! matchingResult = finish matching
            matchingResult.Bytes |> should equal (ascii "MASKXXX")
            matching.Sink.Bytes |> should equal (ascii "MASKXXX")

            let! nonmatching = createHarness definition 128L 2_048L false
            do! push nonmatching (ascii "PASS")
            nonmatching.Sink.Bytes |> should equal (ascii "PASS")
            let! nonmatchingResult = finish nonmatching
            nonmatchingResult.Bytes |> should equal (ascii "PASS")

            let! short = createHarness definition 128L 2_048L false
            do! push short (ascii "MAS")
            short.Sink.Bytes |> should be Empty
            let! shortResult = finish short
            shortResult.Bytes |> should equal (ascii "MAS")
            short.Sink.Bytes |> should equal (ascii "MAS")
        }

    [<Fact>]
    let empty_input_and_short_eof_complete_cleanly () =
        task {
            use workspace = new Workspace()
            let! definition = loadDefinition workspace
            let! empty, _ = run definition [| Array.empty |] 8L 64L false
            empty.Bytes |> should be Empty
            empty.Completions |> should equal 1
            let! partial, _ = run definition [| ascii "MA" |] 8L 64L false
            partial.Bytes |> should equal (ascii "MA")
            partial.Completions |> should equal 1
        }

    [<Fact>]
    let large_masked_input_is_exact_with_finite_capacity () =
        task {
            use workspace = new Workspace()
            let! definition = loadDefinition workspace
            let payload = Array.init 8_192 (fun index -> byte (index % 256))
            let expected =
                payload
                |> Array.map (fun value ->
                    if value >= byte '0' && value <= byte '9' then byte 'X' else value)
            let! result, _ =
                run definition [| Array.concat [| ascii "MASK"; payload |] |] 20_000L 40_000L false
            result.Bytes |> should equal (Array.concat [| ascii "MASK"; expected |])
            result.Capacity.ClientToBackendLogical.PeakBytes |> should be (lessThanOrEqualTo 20_000L)
            result.Capacity.ClientToBackendPhysicalShare.PeakBytes |> should be (lessThanOrEqualTo 20_000L)
            result.Capacity.Physical.PeakPhysicalBufferCapacityBytes |> should be (lessThanOrEqualTo 40_000L)
            (List.last result.Metrics).GeneratedOutputBytes |> should equal (int64 payload.Length)
        }

    [<Fact>]
    let one_byte_pending_prefix_fits_finite_capacity_and_flushes_at_eof () =
        task {
            use workspace = new Workspace()
            let! definition = loadDefinition workspace
            let! result, _ = run definition [| ascii "M" |] 2L 8L false
            result.Bytes |> should equal (ascii "M")
            result.Capacity.ClientToBackendLogical.PeakBytes |> should be (lessThanOrEqualTo 2L)
            result.Capacity.ClientToBackendPhysicalShare.PeakBytes |> should be (lessThanOrEqualTo 4L)
            result.Capacity.Physical.PeakPhysicalBufferCapacityBytes |> should be (lessThanOrEqualTo 8L)
        }

    [<Fact>]
    let live_instances_keep_independent_prefix_decisions () =
        task {
            use workspace = new Workspace()
            let! definition = loadDefinition workspace
            let! a = createHarness definition 128L 2_048L false
            let! b = createHarness definition 128L 2_048L false
            do! push a (ascii "MA")
            do! push b (ascii "PA")
            do! push a (ascii "SK12")
            do! push b (ascii "SS34")
            let! resultA = finish a
            let! resultB = finish b
            resultA.Bytes |> should equal (ascii "MASKXX")
            resultB.Bytes |> should equal (ascii "PASS34")
        }
