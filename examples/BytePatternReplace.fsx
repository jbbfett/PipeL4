open System
open System.Threading.Tasks
open PipeL4.Plugins

let private pattern = [| byte 'H'; byte 'E'; byte 'L'; byte 'L'; byte 'O' |]
let private replacement = [| byte 'W'; byte 'O'; byte 'R'; byte 'L'; byte 'D' |]
let private outputBatchSize = 256

type private OutputBatch(output: IProcessorOutput, maximumOutputBytes: uint64) =
    let mutable writable = Memory<byte>.Empty
    let mutable count = 0
    // The hint cannot cap pool allocation; larger owners are charged, so use only the requested bound.
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
    // HELLO has no non-empty prefix that is also a suffix. A mismatch can
    // therefore flush the pending prefix and retry the current byte as a new
    // possible match start.
    let pending = Array.zeroCreate<byte> (pattern.Length - 1)
    let mutable pendingLength = 0

    let emitByte (output: OutputBatch) value = output.Emit(value)

    let emitPending output =
        if pendingLength > 0 then
            for index in 0 .. pendingLength - 1 do
                emitByte output pending[index]

            pendingLength <- 0

    let processByte (output: OutputBatch) value =
        let mutable current = value
        let mutable retry = true

        while retry do
            if pendingLength = 0 then
                if current = pattern[0] then
                    pending[0] <- current
                    pendingLength <- 1
                else
                    emitByte output current

                retry <- false
            elif current = pattern[pendingLength] then
                if pendingLength = pattern.Length - 1 then
                    for index in 0 .. replacement.Length - 1 do
                        emitByte output replacement[index]

                    pendingLength <- 0
                else
                    pending[pendingLength] <- current
                    pendingLength <- pendingLength + 1

                retry <- false
            else
                emitPending output
                // Reprocess the mismatching byte. It may itself begin HELLO.

    interface IByteStreamProcessor with
        member _.ProcessAsync(input, output, cancellationToken) =
            cancellationToken.ThrowIfCancellationRequested()
            // The replacement is the same length as the pattern, so pending
            // bytes plus this input bound all output from this invocation.
            let maximumOutputBytes = uint64 pendingLength + uint64 input.Bytes.Length
            let batchedOutput = OutputBatch(output, maximumOutputBytes)

            for segment in input.Bytes do
                let source = segment.Span

                if source.Length > 0 then
                    for index in 0 .. source.Length - 1 do
                        cancellationToken.ThrowIfCancellationRequested()
                        processByte batchedOutput source[index]

            if input.IsCompleted then
                cancellationToken.ThrowIfCancellationRequested()
                emitPending batchedOutput

            batchedOutput.Flush()

            // Matching state is plugin-owned and bounded; no runtime input
            // memory is retained between invocations.
            ValueTask<ProcessorResult>({ RetainedLength = 0L })
