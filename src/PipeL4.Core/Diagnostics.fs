namespace PipeL4.Core

open System
open System.Threading

[<RequireQualifiedAccess>]
type internal ProgressFailureCategory =
    | LogicalReceiveExhaustion
    | SynchronousLogicalOutputExhaustion
    | SynchronousPhysicalOutputExhaustion
    | PhysicalReceiveExhaustion
    | PoolOwnerFitFailure

[<Struct; NoEquality; NoComparison>]
type internal DirectionDiagnosticsSnapshot =
    { ReceiveRentalAttempts: int64
      ReceiveRequestedBytes: int64
      ReceiveReturnedOwnerCapacityBytes: int64
      ReceiveSuccessfullyExposedOwners: int64
      ReceiveOverRentRetries: int64
      LogicalReceiveExhaustionCount: int64
      SynchronousLogicalOutputExhaustionCount: int64
      SynchronousPhysicalOutputExhaustionCount: int64
      PhysicalReceiveExhaustionCount: int64
      PoolOwnerFitFailureCount: int64
      TerminalOutcome: string
      CleanupFailurePresent: bool }

type internal IConnectionMemoryDiagnostics =
    abstract ReceiveRentalAttempt: requestedBytes: int64 -> unit
    abstract ReceiveReturnedOwnerCapacity: capacityBytes: int64 -> unit
    abstract ReceiveSuccessfullyExposedOwner: unit -> unit
    abstract ReceiveOverRentRetry: unit -> unit
    abstract ProgressFailure: ProgressFailureCategory -> unit
    abstract CleanupFailure: unit -> unit

type internal NullConnectionMemoryDiagnostics() =
    interface IConnectionMemoryDiagnostics with
        member _.ReceiveRentalAttempt(_) = ()
        member _.ReceiveReturnedOwnerCapacity(_) = ()
        member _.ReceiveSuccessfullyExposedOwner() = ()
        member _.ReceiveOverRentRetry() = ()
        member _.ProgressFailure(_) = ()
        member _.CleanupFailure() = ()

type internal DirectionRuntimeDiagnostics(enabled: bool) =
    let mutable receiveRentalAttempts = 0L
    let mutable receiveRequestedBytes = 0L
    let mutable receiveReturnedOwnerCapacityBytes = 0L
    let mutable receiveSuccessfullyExposedOwners = 0L
    let mutable receiveOverRentRetries = 0L
    let mutable logicalReceiveExhaustionCount = 0L
    let mutable synchronousLogicalOutputExhaustionCount = 0L
    let mutable synchronousPhysicalOutputExhaustionCount = 0L
    let mutable physicalReceiveExhaustionCount = 0L
    let mutable poolOwnerFitFailureCount = 0L
    let mutable terminalOutcome = "not-started"
    let mutable cleanupFailurePresent = 0

    let add (target: byref<int64>) value =
        if enabled && value > 0L then
            let mutable completed = false

            while not completed do
                let current = Volatile.Read(&target)

                if current = Int64.MaxValue then
                    completed <- true
                else
                    let updated =
                        if value > Int64.MaxValue - current then
                            Int64.MaxValue
                        else
                            current + value

                    completed <- Interlocked.CompareExchange(&target, updated, current) = current

    member _.Enabled = enabled

    member _.RecordReceiveRentalAttempt(requestedBytes) =
        if enabled then
            add &receiveRentalAttempts 1L
            add &receiveRequestedBytes requestedBytes

    member _.RecordReceiveReturnedOwnerCapacity(capacityBytes) =
        add &receiveReturnedOwnerCapacityBytes capacityBytes

    member _.RecordReceiveSuccessfullyExposedOwner() =
        add &receiveSuccessfullyExposedOwners 1L

    member _.RecordReceiveOverRentRetry() =
        add &receiveOverRentRetries 1L

    member _.RecordProgressFailure(category) =
        if enabled then
            match category with
            | ProgressFailureCategory.LogicalReceiveExhaustion ->
                add &logicalReceiveExhaustionCount 1L
            | ProgressFailureCategory.SynchronousLogicalOutputExhaustion ->
                add &synchronousLogicalOutputExhaustionCount 1L
            | ProgressFailureCategory.SynchronousPhysicalOutputExhaustion ->
                add &synchronousPhysicalOutputExhaustionCount 1L
            | ProgressFailureCategory.PhysicalReceiveExhaustion ->
                add &physicalReceiveExhaustionCount 1L
            | ProgressFailureCategory.PoolOwnerFitFailure ->
                add &poolOwnerFitFailureCount 1L

    member _.RecordTerminalOutcome(outcome: string) =
        if enabled then
            Volatile.Write(&terminalOutcome, outcome)

    member _.RecordCleanupFailure() =
        if enabled then
            Volatile.Write(&cleanupFailurePresent, 1)

    member _.GetSnapshot() =
        if not enabled then
            { ReceiveRentalAttempts = 0L
              ReceiveRequestedBytes = 0L
              ReceiveReturnedOwnerCapacityBytes = 0L
              ReceiveSuccessfullyExposedOwners = 0L
              ReceiveOverRentRetries = 0L
              LogicalReceiveExhaustionCount = 0L
              SynchronousLogicalOutputExhaustionCount = 0L
              SynchronousPhysicalOutputExhaustionCount = 0L
              PhysicalReceiveExhaustionCount = 0L
              PoolOwnerFitFailureCount = 0L
              TerminalOutcome = "disabled"
              CleanupFailurePresent = false }
        else
            { ReceiveRentalAttempts = Volatile.Read(&receiveRentalAttempts)
              ReceiveRequestedBytes = Volatile.Read(&receiveRequestedBytes)
              ReceiveReturnedOwnerCapacityBytes = Volatile.Read(&receiveReturnedOwnerCapacityBytes)
              ReceiveSuccessfullyExposedOwners = Volatile.Read(&receiveSuccessfullyExposedOwners)
              ReceiveOverRentRetries = Volatile.Read(&receiveOverRentRetries)
              LogicalReceiveExhaustionCount = Volatile.Read(&logicalReceiveExhaustionCount)
              SynchronousLogicalOutputExhaustionCount =
                  Volatile.Read(&synchronousLogicalOutputExhaustionCount)
              SynchronousPhysicalOutputExhaustionCount =
                  Volatile.Read(&synchronousPhysicalOutputExhaustionCount)
              PhysicalReceiveExhaustionCount = Volatile.Read(&physicalReceiveExhaustionCount)
              PoolOwnerFitFailureCount = Volatile.Read(&poolOwnerFitFailureCount)
              TerminalOutcome = Volatile.Read(&terminalOutcome)
              CleanupFailurePresent = Volatile.Read(&cleanupFailurePresent) <> 0 }

    member this.AsMemoryDiagnostics() =
        { new IConnectionMemoryDiagnostics with
            member _.ReceiveRentalAttempt(requestedBytes) = this.RecordReceiveRentalAttempt requestedBytes
            member _.ReceiveReturnedOwnerCapacity(capacityBytes) =
                this.RecordReceiveReturnedOwnerCapacity capacityBytes
            member _.ReceiveSuccessfullyExposedOwner() = this.RecordReceiveSuccessfullyExposedOwner()
            member _.ReceiveOverRentRetry() = this.RecordReceiveOverRentRetry()
            member _.ProgressFailure(category) = this.RecordProgressFailure category
            member _.CleanupFailure() = this.RecordCleanupFailure() }
