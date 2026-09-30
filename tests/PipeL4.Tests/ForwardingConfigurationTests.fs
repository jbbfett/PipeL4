namespace PipeL4.Tests

open System.Net
open FsUnit.Xunit
open PipeL4.Core
open Xunit

module ForwardingConfigurationTests =
    let private validArguments =
        [| "--listen-address"
           "127.0.0.1"
           "--listen-port"
           "8080"
           "--backend-host"
           "127.0.0.2"
           "--backend-port"
           "8081" |]

    let private parse arguments =
        match ForwardingConfiguration.tryParseArguments arguments with
        | Ok configuration -> configuration
        | Error error -> failwithf "Expected valid arguments, but parsing failed: %s" error

    let private expectError description arguments =
        match ForwardingConfiguration.tryParseArguments arguments with
        | Error _ -> ()
        | Ok _ -> failwithf "Expected invalid arguments for %s, but parsing succeeded." description

    let private without optionName =
        validArguments
        |> Array.chunkBySize 2
        |> Array.filter (fun pair -> pair[0] <> optionName)
        |> Array.collect id

    [<Fact>]
    let ``parses an IPv4 forwarding configuration`` () =
        let configuration = parse validArguments

        configuration.ListenEndpoint
        |> should equal (IPEndPoint(IPAddress.Parse "127.0.0.1", 8080))

        configuration.BackendEndpoint
        |> should equal (IPEndPoint(IPAddress.Parse "127.0.0.2", 8081))

    [<Fact>]
    let ``parses IPv6 arguments in any order`` () =
        let arguments =
            [| "--backend-port"
               "443"
               "--listen-port"
               "8443"
               "--backend-host"
               "2001:db8::1"
               "--listen-address"
               "::1" |]

        let configuration = parse arguments

        configuration.ListenEndpoint
        |> should equal (IPEndPoint(IPAddress.IPv6Loopback, 8443))

        configuration.BackendEndpoint
        |> should equal (IPEndPoint(IPAddress.Parse "2001:db8::1", 443))

    [<Fact>]
    let ``accepts the TCP port boundaries`` () =
        let arguments =
            [| "--listen-address"
               "0.0.0.0"
               "--listen-port"
               "1"
               "--backend-host"
               "127.0.0.1"
               "--backend-port"
               "65535" |]

        let configuration = parse arguments

        configuration.ListenEndpoint.Port |> should equal 1
        configuration.BackendEndpoint.Port |> should equal 65535

    [<Fact>]
    let ``rejects every missing required argument`` () =
        [ "--listen-address"; "--listen-port"; "--backend-host"; "--backend-port" ]
        |> List.iter (fun optionName -> without optionName |> expectError optionName)

    [<Fact>]
    let ``rejects unknown duplicate and valueless arguments`` () =
        Array.append validArguments [| "--unknown"; "value" |]
        |> expectError "an unknown argument"

        Array.append validArguments [| "--listen-port"; "9000" |]
        |> expectError "a duplicate argument"

        [| "--listen-address" |] |> expectError "an argument without a value"

    [<Fact>]
    let ``rejects invalid listen and backend IP addresses`` () =
        [| "--listen-address"
           "localhost"
           "--listen-port"
           "8080"
           "--backend-host"
           "127.0.0.1"
           "--backend-port"
           "8081" |]
        |> expectError "a non-IP listen address"

        [| "--listen-address"
           "127.0.0.1"
           "--listen-port"
           "8080"
           "--backend-host"
           "backend.example"
           "--backend-port"
           "8081" |]
        |> expectError "a non-IP backend host"

    [<Fact>]
    let ``rejects invalid TCP ports`` () =
        [ "not-a-port"; "0"; "-1"; "65536" ]
        |> List.iter (fun port ->
            [| "--listen-address"
               "127.0.0.1"
               "--listen-port"
               port
               "--backend-host"
               "127.0.0.2"
               "--backend-port"
               "8081" |]
            |> expectError $"listen port {port}")

        [| "--listen-address"
           "127.0.0.1"
           "--listen-port"
           "8080"
           "--backend-host"
           "127.0.0.2"
           "--backend-port"
           "65536" |]
        |> expectError "an invalid backend port"
