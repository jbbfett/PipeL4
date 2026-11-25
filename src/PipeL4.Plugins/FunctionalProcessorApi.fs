namespace PipeL4.Plugins

open System
open System.Buffers
open System.Threading

/// Non-owning access to connection state when creating a functional processor's local state.
/// The runtime manages the registry and its published values separately from local state.
[<Struct; NoEquality; NoComparison>]
type CreateContext =
    { ConnectionState: IConnectionState }

/// The runtime-owned input and output available to one functional processor invocation.
/// Input may be segmented. The context, input memory, output object, and writable memory must
/// not be retained after invocation completion, including in processor-local or connection state.
/// Uncommitted writable memory remains valid across asynchronous suspension; Advance commits it
/// to the runtime, after which the processor must not use it.
[<Struct; NoEquality; NoComparison>]
type ProcessContext =
    { Input: ReadOnlySequence<byte>
      /// True only for normal upstream completion, not cancellation or failure.
      IsCompleted: bool
      Output: IProcessorOutput
      CancellationToken: CancellationToken
      ConnectionState: IConnectionState }

/// Functional conveniences over the existing processor output and retained-suffix contracts.
/// The script host discovers handle and optional create/dispose functions using these contracts.
[<AutoOpen>]
module FunctionalProcessor =
    /// Appends the given range of the current input without copying its bytes.
    /// Offsets are relative to Input's start; calls preserve emission order, including repeats.
    /// Emitting bytes is independent from retaining them in the result.
    let emitInput (ctx: ProcessContext) (offset: int64) (length: int64) =
        ctx.Output.AppendInput(offset, length)

    /// Copies bytes directly into runtime-owned output and commits them in emission order.
    /// Empty bytes produce no output and request no writable memory. Any previously requested
    /// writable memory must be advanced before emitting more output.
    let emitBytes (ctx: ProcessContext) (bytes: ReadOnlySpan<byte>) =
        if not bytes.IsEmpty then
            let destination = ctx.Output.GetSpan(bytes.Length)
            bytes.CopyTo(destination)
            ctx.Output.Advance(bytes.Length)

    /// Releases all current input on successful result validation; emits nothing by itself.
    let releaseAll: ProcessorResult = { RetainedLength = 0L }

    /// Selects the unresolved input suffix; emits nothing by itself.
    /// The runtime validates that length is within Input and is zero at normal upstream completion.
    let retainSuffix (length: int64) : ProcessorResult = { RetainedLength = length }
