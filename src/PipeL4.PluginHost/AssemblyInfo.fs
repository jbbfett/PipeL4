namespace PipeL4.PluginHost

open System.Runtime.CompilerServices

[<assembly: InternalsVisibleTo("PipeL4.Tests")>]
[<assembly: InternalsVisibleTo("PipeL4")>]
[<assembly: InternalsVisibleTo("PipeL4.Benchmarks")>]
do ()
