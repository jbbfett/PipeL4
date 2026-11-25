#if INTERACTIVE
#r "nuget: PipeL4.Plugins, 0.1.0-alpha.2"
#endif

open PipeL4.Plugins

let private toUpperAscii (value: byte) =
    if value >= byte 'a' && value <= byte 'z' then
        byte (int value - int 'a' + int 'A')
    else
        value

let handle ctx =
    ctx.CancellationToken.ThrowIfCancellationRequested()

    for segment in ctx.Input do
        let source = segment.Span

        if source.Length > 0 then
            let destination = ctx.Output.GetSpan(source.Length)

            for index in 0 .. source.Length - 1 do
                destination[index] <- toUpperAscii source[index]

            ctx.Output.Advance(source.Length)

    // Every byte is resolved immediately, including at normal EOF.
    releaseAll
