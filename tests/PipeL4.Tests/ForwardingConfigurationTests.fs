namespace PipeL4.Tests

open System
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

        configuration.ClientToServerPluginPaths |> should be Empty
        configuration.ServerToClientPluginPaths |> should be Empty

        configuration.ClientToServerLogicalCapacityBytes |> should equal Int64.MaxValue
        configuration.ServerToClientLogicalCapacityBytes |> should equal Int64.MaxValue
        configuration.ConnectionPhysicalCapacityBytes |> should equal Int64.MaxValue

    [<Fact>]
    let ``parses repeatable directional plugin paths in occurrence order`` () =
        let arguments =
            Array.concat
                [| validArguments
                   [| "--client-to-server-plugin"
                      "./plugins/first.fsx"
                      "--server-to-client-plugin"
                      "./plugins/reverse.fsx"
                      "--client-to-server-plugin"
                      "./plugins/second.fsx"
                      "--client-to-server-plugin"
                      "./plugins/first.fsx" |] |]

        let configuration = parse arguments

        configuration.ClientToServerPluginPaths
        |> Seq.toArray
        |> should
            equal
            [| "./plugins/first.fsx"
               "./plugins/second.fsx"
               "./plugins/first.fsx" |]

        configuration.ServerToClientPluginPaths
        |> Seq.toArray
        |> should equal [| "./plugins/reverse.fsx" |]

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
    let ``accepts capacity boundaries and independent accounting dimensions`` () =
        let arguments =
            Array.append
                validArguments
                [| "--client-to-server-logical-capacity-bytes"
                   "4096"
                   "--server-to-client-logical-capacity-bytes"
                   "9223372036854775807"
                   "--connection-physical-capacity-bytes"
                   "3" |]

        let configuration = parse arguments

        configuration.ClientToServerLogicalCapacityBytes |> should equal 4096L
        configuration.ServerToClientLogicalCapacityBytes |> should equal Int64.MaxValue
        configuration.ConnectionPhysicalCapacityBytes |> should equal 3L

        let independentDimensions =
            Array.append
                validArguments
                [| "--client-to-server-logical-capacity-bytes"
                   "100"
                   "--server-to-client-logical-capacity-bytes"
                   "200"
                   "--connection-physical-capacity-bytes"
                   "2" |]

        let independent = parse independentDimensions
        independent.ClientToServerLogicalCapacityBytes |> should equal 100L
        independent.ServerToClientLogicalCapacityBytes |> should equal 200L
        independent.ConnectionPhysicalCapacityBytes |> should equal 2L

    [<Fact>]
    let ``accepts a logical capacity of exactly one byte`` () =
        let arguments =
            Array.append
                validArguments
                [| "--client-to-server-logical-capacity-bytes"
                   "1" |]

        let configuration = parse arguments

        configuration.ClientToServerLogicalCapacityBytes |> should equal 1L

    [<Fact>]
    let ``accepts an explicit maximum physical capacity`` () =
        let arguments =
            Array.append
                validArguments
                [| "--connection-physical-capacity-bytes"
                   "9223372036854775807" |]

        let configuration = parse arguments

        configuration.ConnectionPhysicalCapacityBytes |> should equal Int64.MaxValue

    [<Fact>]
    let ``rejects invalid capacity values`` () =
        let capacityOptions =
            [| "--client-to-server-logical-capacity-bytes"
               "--server-to-client-logical-capacity-bytes" |]

        let invalidLogicalValues =
            [| "0"
               "-1"
               "not-a-byte"
               "1.0"
               "9223372036854775808" |]

        for optionName in capacityOptions do
            for value in invalidLogicalValues do
                Array.append validArguments [| optionName; value |]
                |> expectError $"logical capacity {value}"

            Array.append validArguments [| optionName |]
            |> expectError "a missing logical capacity"

            Array.append validArguments [| optionName; "1"; optionName; "2" |]
            |> expectError "a duplicate logical capacity"

        let physicalOption = "--connection-physical-capacity-bytes"

        [| "0"
           "1"
           "-1"
           "not-a-byte"
           "1.0"
           "9223372036854775808" |]
        |> Array.iter (fun value ->
            Array.append validArguments [| physicalOption; value |]
            |> expectError $"physical capacity {value}")

        Array.append validArguments [| physicalOption |]
        |> expectError "a missing physical capacity"

        Array.append validArguments [| physicalOption; "2"; physicalOption; "3" |]
        |> expectError "a duplicate physical capacity"

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

        Array.append validArguments [| "--client-to-server-plugin" |]
        |> expectError "a client-to-server plugin without a value"

        Array.append
            validArguments
            [| "--server-to-client-plugin"; "--client-to-server-plugin"; "plugin.fsx" |]
        |> expectError "a server-to-client plugin followed by another option"

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
