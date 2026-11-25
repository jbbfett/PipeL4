#if INTERACTIVE
#r "nuget: PipeL4.Plugins, 0.1.0-alpha.2"
#endif

open PipeL4.Plugins

// Delay the final four bytes, emitting earlier input without copying it.
// The runtime retains the suffix and joins it to new input, possibly segmented.
let handle (ctx: ProcessContext) =
    ctx.CancellationToken.ThrowIfCancellationRequested()
    let retained = if ctx.IsCompleted then 0L else min 4L ctx.Input.Length
    emitInput ctx 0L (ctx.Input.Length - retained)
    retainSuffix retained
