#if INTERACTIVE
#r "nuget: PipeL4.Plugins, 0.1.0-alpha.2"
#endif

open System
open PipeL4.Plugins

let create () = ref 0L

// Await real diagnostic I/O at normal EOF. There is no background output producer.
let handle (ctx: ProcessContext) (total: int64 ref) = task {
    ctx.CancellationToken.ThrowIfCancellationRequested()
    total.Value <- total.Value + ctx.Input.Length

    if ctx.IsCompleted then
        do! Console.Error.WriteLineAsync("Normal EOF")

    emitInput ctx 0L ctx.Input.Length
    return releaseAll
}

// Explicit asynchronous cleanup runs on completion, failure, or cancellation.
// The console belongs to the application; do not dispose it.
let dispose (total: int64 ref) = task {
    do! Console.Error.WriteLineAsync($"Processed bytes: {total.Value}")
}
