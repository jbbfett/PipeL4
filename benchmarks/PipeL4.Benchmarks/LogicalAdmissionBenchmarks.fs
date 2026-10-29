namespace PipeL4.Benchmarks

open System
open System.Buffers
open System.Threading
open System.Threading.Tasks
open BenchmarkDotNet.Attributes
open PipeL4.Core

type private AdmissionSink() =
    interface IProcessorPipelineSink with
        member _.WriteAsync(_, _) = ValueTask()
        member _.CompleteAsync _ = ValueTask()

[<MemoryDiagnoser>]
type LogicalAdmissionBenchmarks() =
    let mutable unbounded = Unchecked.defaultof<ProcessorPipeline>
    let mutable bounded = Unchecked.defaultof<ProcessorPipeline>
    let mutable exhaustion = Unchecked.defaultof<ProcessorPipeline>
    let mutable boundedBudget = Unchecked.defaultof<CapacityBudget>
    let mutable exhaustionBudget = Unchecked.defaultof<CapacityBudget>
    let mutable held = Unchecked.defaultof<CapacityReservation>
    let mutable exhaustionCompletedCount = 0L

    [<GlobalSetup>]
    member _.Setup() =
        unbounded <- new ProcessorPipeline([||], AdmissionSink(), MemoryPool<byte>.Shared)
        boundedBudget <- new CapacityBudget 4096L
        let boundedAccounting = new ConnectionMemoryAccounting()
        bounded <-
            new ProcessorPipeline(
                [||],
                AdmissionSink(),
                MemoryPool<byte>.Shared,
                boundedAccounting,
                boundedBudget
            )

        exhaustionBudget <- new CapacityBudget 4096L
        held <-
            match exhaustionBudget.TryReserve 4096L with
            | ValueSome reservation -> reservation
            | ValueNone -> failwith "Could not seed the exhaustion benchmark reservation."

        let exhaustionAccounting = new ConnectionMemoryAccounting()
        exhaustion <-
            new ProcessorPipeline(
                [||],
                AdmissionSink(),
                MemoryPool<byte>.Shared,
                exhaustionAccounting,
                exhaustionBudget
            )

    [<GlobalCleanup>]
    member _.Cleanup() =
        if exhaustionCompletedCount > 0L then
            let snapshot = exhaustionBudget.GetSnapshot()

            if snapshot.QueuedWaitCount <> exhaustionCompletedCount
               || snapshot.CurrentWaiterCount <> 0L
               || snapshot.PeakWaiterCount <= 0L then
                invalidOp "The exhaustion benchmark did not observe completed capacity waits."

        if not (obj.ReferenceEquals(unbounded, null)) then
            (unbounded :> IDisposable).Dispose()

        if not (obj.ReferenceEquals(bounded, null)) then
            (bounded :> IDisposable).Dispose()

        if not (obj.ReferenceEquals(exhaustion, null)) then
            (exhaustion :> IDisposable).Dispose()

        if not (obj.ReferenceEquals(held, null)) then
            held.Release()

    [<Benchmark(Baseline = true)>]
    member _.Unbounded4KiB() =
        let owner = MemoryPool<byte>.Shared.Rent(4096)
        unbounded.PushAsync(owner, 4096, CancellationToken.None)

    [<Benchmark>]
    member _.BoundedUncontended4KiB() =
        let owner = MemoryPool<byte>.Shared.Rent(4096)
        bounded.PushAsync(owner, 4096, CancellationToken.None)

    [<Benchmark>]
    member _.BoundedExhaustionAndResumption4KiB() =
        task {
            let owner = MemoryPool<byte>.Shared.Rent(4096)
            let operation = exhaustion.PushAsync(owner, 4096, CancellationToken.None)
            held.Release()
            do! operation

            exhaustionCompletedCount <- exhaustionCompletedCount + 1L

            held <-
                match exhaustionBudget.TryReserve 4096L with
                | ValueSome reservation -> reservation
                | ValueNone -> failwith "Could not restore the exhaustion benchmark reservation."
        }
