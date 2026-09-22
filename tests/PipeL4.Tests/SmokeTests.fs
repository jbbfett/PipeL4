namespace PipeL4.Tests

open FsUnit.Xunit
open Xunit

module SmokeTests =
    [<Fact>]
    let ``test project is discovered and executed`` () =
        2 + 2 |> should equal 4
