namespace PipeL4.Plugins

open System.Buffers
open System.Threading
open System.Threading.Tasks

/// Runtime-owned typed state shared by every processor participating in one TCP connection.
/// The registry is safe for concurrent access, but stored mutable values must provide their own
/// synchronization when processors can access them concurrently.
type IConnectionState =
    /// Returns the value already published for the key, or creates and publishes it once.
    abstract GetOrAdd<'T>: key: string * factory: (unit -> 'T) -> 'T

    /// Returns the published value for the key without creating one.
    abstract TryGet<'T>: key: string -> 'T option

/// The logical input available to one processor invocation.
/// The sequence and its underlying memory are runtime-owned and must not be retained after
/// ProcessAsync completes. A retained suffix is presented again by the runtime on a later invocation.
[<Struct; NoEquality; NoComparison>]
type ProcessorInput =
    { Bytes: ReadOnlySequence<byte>
      IsCompleted: bool }

/// The outcome of one successful processor invocation.
[<Struct; NoEquality; NoComparison>]
type ProcessorResult =
    { /// Length of the unresolved suffix of the current input.
      RetainedLength: int64 }

/// Ordered output for one processor invocation.
/// Input ranges and writable memory are runtime-owned. Uncommitted writable memory remains valid
/// across asynchronous suspension, becomes runtime-owned output when advanced, and must not be used
/// after that Advance call. The output object and memory obtained from it must not be retained after
/// ProcessAsync completes.
type IProcessorOutput =
    inherit IBufferWriter<byte>

    /// Appends a range from the current invocation input without transferring ownership.
    /// Appending a range is independent from retaining the same bytes in ProcessorResult.
    abstract AppendInput: offset: int64 * length: int64 -> unit

/// A byte-stream processor controlled and scheduled by the PipeL4 runtime.
/// A processor may also implement IAsyncDisposable or IDisposable for instance-owned resources.
/// The runtime prefers asynchronous disposal when both interfaces are implemented and disposes
/// the instance when its directional pipeline ends.
type IByteStreamProcessor =
    /// Processes the currently available logical input.
    /// Input and output remain valid across asynchronous suspension until the returned value task completes.
    abstract ProcessAsync:
        input: ProcessorInput *
        output: IProcessorOutput *
        cancellationToken: CancellationToken ->
            ValueTask<ProcessorResult>
