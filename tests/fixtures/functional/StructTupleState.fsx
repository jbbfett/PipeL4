#if INTERACTIVE
#r "nuget: PipeL4.Plugins, 0.1.0-alpha.2"
#endif

open System.Threading.Tasks
open PipeL4.Plugins

let create () = struct (ref 0, ref 0)

let handle ctx (state: struct (int ref * int ref)) =
    let struct (first, second) = state
    first.Value <- first.Value + 1
    second.Value <- second.Value + 1
    emitInput ctx 0L ctx.Input.Length
    ValueTask<ProcessorResult>(releaseAll)

let dispose (state: struct (int ref * int ref)) = ()
