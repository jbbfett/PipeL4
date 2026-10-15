namespace PipeL4.Core

open System
open System.Buffers
open System.Threading
open System.Threading.Tasks

[<Struct; NoEquality; NoComparison>]
type internal ProcessorInput =
    { Bytes: ReadOnlySequence<byte>
      IsCompleted: bool }

[<Struct; NoEquality; NoComparison>]
type internal ProcessorResult =
    { /// Length of the suffix retained from this stage's current input.
      RetainedLength: int64 }

type internal IProcessorOutput =
    inherit IBufferWriter<byte>

    /// Appends a range from this stage's current invocation input without transferring ownership.
    /// Appending a range is independent from retaining the same bytes in ProcessorResult.
    abstract AppendInput: offset: int64 * length: int64 -> unit

type internal IByteStreamProcessor =
    abstract ProcessAsync:
        input: ProcessorInput *
        output: IProcessorOutput *
        cancellationToken: CancellationToken ->
            ValueTask<ProcessorResult>

[<RequireQualifiedAccess>]
module internal ProcessorContract =
    let getInputRange input offset length =
        let inputLength = input.Bytes.Length

        if offset < 0L || offset > inputLength then
            invalidOp
                $"A processor output range starts at offset {offset} in an input of {inputLength} bytes."

        if length < 0L || length > inputLength - offset then
            invalidOp
                $"A processor output range has length {length} at offset {offset} in an input of {inputLength} bytes."

        input.Bytes.Slice(offset, length)

    let getRetainedSuffix input result =
        let inputLength = input.Bytes.Length

        if result.RetainedLength < 0L || result.RetainedLength > inputLength then
            invalidOp
                $"A processor retained {result.RetainedLength} bytes from an input of {inputLength} bytes."

        if input.IsCompleted && result.RetainedLength <> 0L then
            invalidOp
                $"A processor retained {result.RetainedLength} bytes after upstream completion."

        input.Bytes.Slice(inputLength - result.RetainedLength)
