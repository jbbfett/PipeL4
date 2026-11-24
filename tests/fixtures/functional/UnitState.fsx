#if INTERACTIVE
#r "nuget: PipeL4.Plugins, 0.1.0-alpha.2"
#endif

open System.Threading.Tasks
open PipeL4.Plugins

let create () = ()

let handle ctx (state: unit) =
    emitInput ctx 0L ctx.Input.Length
    releaseAll

let dispose () = ValueTask<unit>(task { return () })
