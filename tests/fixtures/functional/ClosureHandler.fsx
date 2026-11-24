#if INTERACTIVE
#r "nuget: PipeL4.Plugins, 0.1.0-alpha.2"
#endif

open System.Threading.Tasks
open PipeL4.Plugins

let handle =
    let count = ref 0
    fun ctx ->
        count.Value <- count.Value + 1
        emitInput ctx 0L ctx.Input.Length
        releaseAll
