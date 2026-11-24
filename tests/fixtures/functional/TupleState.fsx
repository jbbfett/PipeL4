#if INTERACTIVE
#r "nuget: PipeL4.Plugins, 0.1.0-alpha.2"
#endif

open System.Threading.Tasks
open PipeL4.Plugins

let create () = (ref 0, ref 0)

let handle ctx (state: int ref * int ref) =
    let first, second = state
    first.Value <- first.Value + 1
    second.Value <- second.Value + 1
    emitInput ctx 0L ctx.Input.Length
    releaseAll

let dispose (state: int ref * int ref) =
    let first, second = state
    first.Value <- 0
    second.Value <- 0
