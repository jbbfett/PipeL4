#if INTERACTIVE
#r "nuget: PipeL4.Plugins, 0.1.0-alpha.2"
#endif

open PipeL4.Plugins

let create (ctx: CreateContext) =
    ctx.ConnectionState.GetOrAdd("shared-count", fun () -> ref 0L) |> ignore
    ref 0L

let handle ctx (state: int64 ref) =
    state.Value <- state.Value + ctx.Input.Length
    emitInput ctx 0L ctx.Input.Length
    releaseAll

let dispose (state: int64 ref) =
    state.Value <- 0L
