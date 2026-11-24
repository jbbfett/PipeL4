#if INTERACTIVE
#r "nuget: PipeL4.Plugins, 0.1.0-alpha.2"
#endif

open System.Threading.Tasks
open PipeL4.Plugins

let create (_: unit) = ref 0

let handle ctx (state: int ref) =
    state.Value <- state.Value + 1
    emitInput ctx 0L ctx.Input.Length
    releaseAll
