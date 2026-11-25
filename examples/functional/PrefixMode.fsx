#if INTERACTIVE
#r "nuget: PipeL4.Plugins, 0.1.0-alpha.2"
#endif

open System.Buffers
open PipeL4.Plugins

// 0 = awaiting four bytes, 1 = pass-through, 2 = mask subsequent digits.
let create () = ref 0

let private emitMasked (ctx: ProcessContext) (offset: int64) =
    let mutable reader = SequenceReader<byte>(ctx.Input.Slice(offset))

    while reader.Remaining > 0L do
        ctx.CancellationToken.ThrowIfCancellationRequested()
        let count = int (min 256L reader.Remaining)
        let destination = ctx.Output.GetSpan(count)

        for index in 0 .. count - 1 do
            ctx.CancellationToken.ThrowIfCancellationRequested()
            let mutable value = 0uy
            reader.TryRead(&value) |> ignore
            destination[index] <-
                if value >= byte '0' && value <= byte '9' then byte 'X'
                else value

        ctx.Output.Advance(count)

let handle (ctx: ProcessContext) (mode: int ref) =
    ctx.CancellationToken.ThrowIfCancellationRequested()

    if mode.Value = 0 && ctx.Input.Length < 4L then
        if ctx.IsCompleted then
            // An incomplete prefix is ordinary stream data at EOF.
            emitInput ctx 0L ctx.Input.Length
            releaseAll
        else
            retainSuffix ctx.Input.Length
    else
        let mutable offset = 0L

        if mode.Value = 0 then
            // SequenceReader handles a prefix split across runtime input segments.
            let mutable reader = SequenceReader<byte>(ctx.Input)
            let mutable matches = true

            for expected in "MASK" do
                let mutable value = 0uy
                reader.TryRead(&value) |> ignore
                matches <- matches && value = byte expected

            mode.Value <- if matches then 2 else 1
            emitInput ctx 0L 4L
            offset <- 4L

        if mode.Value = 1 then
            emitInput ctx offset (ctx.Input.Length - offset)
        else
            // The borrowed prefix precedes generated masked bytes.
            emitMasked ctx offset

        releaseAll
