#if INTERACTIVE
#r "nuget: PipeL4.Plugins, 0.1.0-alpha.2"
#endif

open System
open PipeL4.Plugins

let private pattern = [| byte 'H'; byte 'E'; byte 'L'; byte 'L'; byte 'O' |]
let private replacement = [| byte 'W'; byte 'O'; byte 'R'; byte 'L'; byte 'D' |]

// Only the matched prefix length is needed: pending bytes equal pattern[0..length).
// This is bounded local matching state, not retained runtime input.
let create () = ref 0

let handle (ctx: ProcessContext) (pendingLength: int ref) =
    ctx.CancellationToken.ThrowIfCancellationRequested()
    let mutable remaining = uint64 pendingLength.Value + uint64 ctx.Input.Length
    let mutable writable = Memory<byte>.Empty
    let mutable count = 0
    let mutable limit = 0

    let flush () =
        if limit > 0 then
            ctx.Output.Advance(count)
            writable <- Memory<byte>.Empty
            count <- 0
            limit <- 0

    let emit value =
        if remaining = 0UL then
            invalidOp "The output exceeded its per-invocation upper bound."

        if count = limit then
            flush ()
            limit <- int (min 256UL remaining)
            writable <- ctx.Output.GetMemory(limit)

        writable.Span[count] <- value
        count <- count + 1
        remaining <- remaining - 1UL

    let emitPending () =
        for index in 0 .. pendingLength.Value - 1 do
            emit pattern[index]
        pendingLength.Value <- 0

    for segment in ctx.Input do
        let source = segment.Span

        for index in 0 .. source.Length - 1 do
            ctx.CancellationToken.ThrowIfCancellationRequested()
            let value = source[index]

            // HELLO has no non-empty prefix that is also a suffix. On mismatch,
            // flush and retry this byte; it might start a new match (HEHELLO).
            if value <> pattern[pendingLength.Value] then
                emitPending ()

            if value = pattern[pendingLength.Value] then
                if pendingLength.Value = pattern.Length - 1 then
                    for replacementByte in replacement do emit replacementByte
                    pendingLength.Value <- 0
                else
                    pendingLength.Value <- pendingLength.Value + 1
            else
                emit value

    if ctx.IsCompleted then
        ctx.CancellationToken.ThrowIfCancellationRequested()
        emitPending ()

    flush ()
    releaseAll
