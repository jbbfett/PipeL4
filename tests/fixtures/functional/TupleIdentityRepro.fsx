#if INTERACTIVE
#r "nuget: PipeL4.Plugins, 0.1.0-alpha.2"
#endif

open System.Threading.Tasks
open PipeL4.Plugins

// F# flattens the reference-tuple argument and reconstructs its container inside
// the function. Native F# calls and the host have the same identity observation.
let original = (ref 0, ref 0)
let observesOriginal (state: int ref * int ref) = obj.ReferenceEquals(state, original)
let nativeIdentity = observesOriginal original
let observed = ResizeArray<bool>()
let create () = original

let handle ctx (state: int ref * int ref) =
    observed.Add(obj.ReferenceEquals(state, original))
    let first, second = state
    if not (obj.ReferenceEquals(first, fst original)) || not (obj.ReferenceEquals(second, snd original)) then
        invalidOp "Tuple component identity changed"
    emitInput ctx 0L ctx.Input.Length
    releaseAll

printfn "Native tuple container identity: %b; component references remain unchanged." nativeIdentity
