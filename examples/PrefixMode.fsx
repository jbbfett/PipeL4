open System
open System.Buffers
open System.Threading
open System.Threading.Tasks
open PipeL4.Plugins

let private prefixLength = 4L
let private outputBatchSize = 256

type private OutputBatch(output: IProcessorOutput, maximumOutputBytes: uint64) =
    let mutable writable = Memory<byte>.Empty
    let mutable count = 0
    let mutable writableLimit = 0
    let mutable remainingOutputUpperBound = maximumOutputBytes
    let mutable hasWritable = false

    member private this.Commit() =
        if hasWritable then
            output.Advance(count)
            writable <- Memory<byte>.Empty
            count <- 0
            writableLimit <- 0
            hasWritable <- false

    member this.Emit(value: byte) =
        if remainingOutputUpperBound = 0UL then
            invalidOp "The output exceeded its per-invocation upper bound."

        if not hasWritable then
            writableLimit <- min (uint64 outputBatchSize) remainingOutputUpperBound |> int
            writable <- output.GetMemory(writableLimit)
            hasWritable <- true
        elif count = writableLimit then
            this.Commit()
            writableLimit <- min (uint64 outputBatchSize) remainingOutputUpperBound |> int
            writable <- output.GetMemory(writableLimit)
            hasWritable <- true

        writable.Span[count] <- value
        count <- count + 1
        remainingOutputUpperBound <- remainingOutputUpperBound - 1UL

    member this.Flush() = this.Commit()

type Processor() =
    // 0 = awaiting the initial four-byte decision, 1 = pass-through, 2 = mask digits.
    let mutable mode = 0
    let prefix = Array.zeroCreate<byte> 4

    let readPrefix (input: ReadOnlySequence<byte>) =
        let mutable copied = 0

        for segment in input do
            if copied < prefix.Length then
                let source = segment.Span
                let count = min (prefix.Length - copied) source.Length

                if count > 0 then
                    source.Slice(0, count).CopyTo(prefix.AsSpan(copied, count))
                    copied <- copied + count

        copied = prefix.Length

    let isMaskPrefix () =
        prefix[0] = byte 'M'
        && prefix[1] = byte 'A'
        && prefix[2] = byte 'S'
        && prefix[3] = byte 'K'

    let emitMaskedSuffix
        (sequence: ReadOnlySequence<byte>)
        (output: IProcessorOutput)
        (cancellationToken: CancellationToken)
        =
        let maximumOutputBytes = uint64 sequence.Length
        let batchedOutput = OutputBatch(output, maximumOutputBytes)

        for segment in sequence do
            let source = segment.Span

            if source.Length > 0 then
                for index in 0 .. source.Length - 1 do
                    cancellationToken.ThrowIfCancellationRequested()
                    let value = source[index]

                    if value >= byte '0' && value <= byte '9' then
                        batchedOutput.Emit(byte 'X')
                    else
                        batchedOutput.Emit(value)

        batchedOutput.Flush()

    interface IByteStreamProcessor with
        member _.ProcessAsync(input, output, cancellationToken) =
            cancellationToken.ThrowIfCancellationRequested()

            if mode = 0 then
                if input.Bytes.Length >= prefixLength then
                    readPrefix (input.Bytes.Slice(0L, prefixLength)) |> ignore
                    mode <- if isMaskPrefix () then 2 else 1

                    // The initial prefix remains borrowed from runtime-owned input.
                    output.AppendInput(0L, prefixLength)
                    let suffix = input.Bytes.Slice(prefixLength)

                    if mode = 1 then
                        if suffix.Length > 0L then
                            output.AppendInput(prefixLength, suffix.Length)
                    else
                        emitMaskedSuffix suffix output cancellationToken

                    ValueTask<ProcessorResult>({ RetainedLength = 0L })
                elif input.IsCompleted then
                    // An incomplete prefix is ordinary stream data at EOF.
                    if input.Bytes.Length > 0L then
                        output.AppendInput(0L, input.Bytes.Length)

                    ValueTask<ProcessorResult>({ RetainedLength = 0L })
                else
                    // Retain only the unresolved prefix; its length is below four bytes.
                    ValueTask<ProcessorResult>({ RetainedLength = input.Bytes.Length })
            elif mode = 1 then
                if input.Bytes.Length > 0L then
                    output.AppendInput(0L, input.Bytes.Length)

                ValueTask<ProcessorResult>({ RetainedLength = 0L })
            else
                emitMaskedSuffix input.Bytes output cancellationToken
                ValueTask<ProcessorResult>({ RetainedLength = 0L })
