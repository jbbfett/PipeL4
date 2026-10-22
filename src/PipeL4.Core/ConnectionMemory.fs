namespace PipeL4.Core

open System
open System.Buffers
open System.Runtime.ExceptionServices
open System.Threading

module private ConnectionMemoryException =
    let raisePreserving<'value> (error: exn) : 'value =
        ExceptionDispatchInfo.Capture(error).Throw()
        Unchecked.defaultof<'value>

[<Struct; NoEquality; NoComparison>]
type internal ConnectionMemorySnapshot =
    { CurrentPhysicalBufferCapacityBytes: int64 }

type internal ConnectionMemoryAccounting() =
    let mutable currentPhysicalBufferCapacityBytes = 0L

    member internal _.Acquire(capacity: int64) =
        if capacity < 0L then
            invalidArg (nameof capacity) "A buffer capacity cannot be negative."

        let rec acquire () =
            let current = Volatile.Read(&currentPhysicalBufferCapacityBytes)

            if current > Int64.MaxValue - capacity then
                invalidOp "Physical buffer capacity accounting overflowed."

            if
                Interlocked.CompareExchange(
                    &currentPhysicalBufferCapacityBytes,
                    current + capacity,
                    current
                )
                <> current
            then
                acquire ()

        acquire ()

    member internal _.Release(capacity: int64) =
        if capacity < 0L then
            invalidArg (nameof capacity) "A buffer capacity cannot be negative."

        let rec release () =
            let current = Volatile.Read(&currentPhysicalBufferCapacityBytes)

            if current < capacity then
                invalidOp "Physical buffer capacity accounting became negative."

            let updated = current - capacity

            if Interlocked.CompareExchange(&currentPhysicalBufferCapacityBytes, updated, current)
               <> current then
                release ()

        release ()

    member _.GetSnapshot() =
        { CurrentPhysicalBufferCapacityBytes =
              Volatile.Read(&currentPhysicalBufferCapacityBytes) }

    member internal this.Track(owner: IMemoryOwner<byte>) : IMemoryOwner<byte> =
        if obj.ReferenceEquals(owner, null) then
            nullArg (nameof owner)

        match owner with
        | :? TrackedMemoryOwner as tracked when obj.ReferenceEquals(tracked.Accounting, this) ->
            owner
        | :? TrackedMemoryOwner ->
            invalidOp "A runtime-owned buffer lease cannot move between connections."
        | _ ->
            let capacity =
                try
                    int64 owner.Memory.Length
                with error ->
                    try
                        owner.Dispose()
                    with _ ->
                        ()

                    ConnectionMemoryException.raisePreserving error

            try
                this.Acquire capacity
            with error ->
                // Track owns a newly-rented owner while registration is in
                // progress, so a failed registration must release it.
                try
                    owner.Dispose()
                with _ ->
                    ()

                ConnectionMemoryException.raisePreserving error

            try
                new TrackedMemoryOwner(owner, this, capacity) :> IMemoryOwner<byte>
            with error ->
                try
                    this.Release capacity
                with _ ->
                    ()

                try
                    owner.Dispose()
                with _ ->
                    ()

                ConnectionMemoryException.raisePreserving error

    member internal this.TrackArrayCapacity(capacity: int64) : IDisposable =
        this.Acquire capacity

        try
            new TrackedCapacityLease(this, capacity) :> IDisposable
        with error ->
            this.Release capacity
            raise error

and private TrackedMemoryOwner
    (owner: IMemoryOwner<byte>, accounting: ConnectionMemoryAccounting, capacity: int64)
    =
    let mutable disposed = 0

    member _.Accounting = accounting

    interface IMemoryOwner<byte> with
        member _.Memory = owner.Memory

        member _.Dispose() =
            if Interlocked.Exchange(&disposed, 1) = 0 then
                try
                    owner.Dispose()
                with error ->
                    // The underlying owner may have released its storage before
                    // throwing, but the runtime cannot determine that reliably.
                    // Keep the lease accounted as active and do not retry an
                    // owner whose release state is uncertain.
                    ConnectionMemoryException.raisePreserving error

                accounting.Release capacity

and private TrackedCapacityLease(accounting: ConnectionMemoryAccounting, capacity: int64) =
    let mutable disposed = 0

    interface IDisposable with
        member _.Dispose() =
            if Interlocked.Exchange(&disposed, 1) = 0 then
                accounting.Release capacity
