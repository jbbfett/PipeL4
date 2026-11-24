namespace PipeL4.Tests

open System
open PipeL4.Plugins
open Xunit

module PublicApiBoundaryTests =
    [<Fact>]
    let ``processor contracts belong to the public plugin assembly`` () =
        let assembly = typeof<IByteStreamProcessor>.Assembly

        [| typeof<IConnectionState>
           typeof<ProcessorInput>
           typeof<ProcessorResult>
           typeof<IProcessorOutput>
           typeof<IByteStreamProcessor>
           typeof<CreateContext>
           typeof<ProcessContext> |]
        |> Array.iter (fun contract ->
            Assert.Same(assembly, contract.Assembly)
            Assert.Equal("PipeL4.Plugins", contract.Namespace))

        Assert.Equal("PipeL4.Plugins", assembly.GetName().Name)

    [<Fact>]
    let ``public plugin assembly has no runtime or host references`` () =
        let forbidden =
            [| "PipeL4.Core"
               "PipeL4.PluginHost"
               "FSharp.Compiler"
               "FSharp.DependencyManager" |]

        let referencedNames =
            typeof<IByteStreamProcessor>.Assembly.GetReferencedAssemblies()
            |> Array.choose (fun reference -> Option.ofObj reference.Name)

        for reference in referencedNames do
            Assert.False(
                forbidden
                |> Array.exists (fun prefix ->
                    reference.StartsWith(prefix, StringComparison.Ordinal)),
                $"Unexpected public API assembly reference: {reference}"
            )
