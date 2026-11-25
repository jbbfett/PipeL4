#if INTERACTIVE
#r "nuget: PipeL4.Plugins, 0.1.0-alpha.2"
#endif

open PipeL4.Plugins

// Intentionally fails: IsCompleted also belongs to ProcessorInput. F# commits to
// that record before it sees Input. Annotating ctx: ProcessContext resolves it.
let handle ctx =
    if ctx.IsCompleted then
        emitInput ctx 0L ctx.Input.Length
        releaseAll
    else
        retainSuffix ctx.Input.Length
