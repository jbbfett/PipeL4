#if INTERACTIVE
#r "nuget: PipeL4.Plugins, 0.1.0-alpha.2"
#endif

open System.Threading.Tasks
open PipeL4.Plugins

let make (_: unit) = ref 0
let create = make

let implementation ctx (state: int ref) = task {
    state.Value <- state.Value + 1
    emitInput ctx 0L ctx.Input.Length
    return releaseAll
}
let handle = implementation

let cleanup (state: int ref) = ValueTask<unit>(task {
    do! Task.Yield()
    state.Value <- 0
})
let dispose = cleanup
