namespace PipeL4.Core

open System
open System.Collections.Generic
open System.Net

[<Sealed>]
type ForwardingConfiguration private (listenEndpoint: IPEndPoint, backendEndpoint: IPEndPoint) =
    member _.ListenEndpoint = listenEndpoint
    member _.BackendEndpoint = backendEndpoint

    static member internal Create(listenEndpoint, backendEndpoint) =
        ForwardingConfiguration(listenEndpoint, backendEndpoint)

[<RequireQualifiedAccess>]
module ForwardingConfiguration =
    let private optionNames =
        [ "--listen-address"
          "--listen-port"
          "--backend-host"
          "--backend-port" ]

    let private knownOptions = Set.ofList optionNames

    let private collectArguments (arguments: string array) =
        let values = Dictionary<string, string>(StringComparer.Ordinal)

        let rec collect index =
            if index = arguments.Length then
                Ok values
            else
                let optionName = arguments[index]

                if not (knownOptions.Contains optionName) then
                    Error $"Unknown argument '{optionName}'."
                elif values.ContainsKey optionName then
                    Error $"Argument '{optionName}' was specified more than once."
                elif index + 1 >= arguments.Length || knownOptions.Contains arguments[index + 1] then
                    Error $"Missing value for argument '{optionName}'."
                else
                    values.Add(optionName, arguments[index + 1])
                    collect (index + 2)

        collect 0

    let private findMissingOption (values: Dictionary<string, string>) =
        optionNames |> List.tryFind (values.ContainsKey >> not)

    let private tryParseAddress optionName (value: string): Result<IPAddress, string> =
        match IPAddress.TryParse value with
        | true, address ->
            match Option.ofObj address with
            | Some address -> Ok address
            | None -> Error ("Argument " + optionName + " must be an IPv4 or IPv6 address.")
        | _ -> Error ("Argument " + optionName + " must be an IPv4 or IPv6 address.")

    let private tryParsePort optionName (value: string) =
        match Int32.TryParse value with
        | true, port when port >= 1 && port <= 65535 -> Ok port
        | _ -> Error $"Argument '{optionName}' must be an integer from 1 through 65535."

    let tryParseArguments (arguments: string array) =
        match collectArguments arguments with
        | Error error -> Error error
        | Ok values ->
            match findMissingOption values with
            | Some optionName -> Error $"Missing required argument '{optionName}'."
            | None ->
                match tryParseAddress "--listen-address" values["--listen-address"] with
                | Error error -> Error error
                | Ok listenAddress ->
                    match tryParsePort "--listen-port" values["--listen-port"] with
                    | Error error -> Error error
                    | Ok listenPort ->
                        match tryParseAddress "--backend-host" values["--backend-host"] with
                        | Error error -> Error error
                        | Ok backendAddress ->
                            match tryParsePort "--backend-port" values["--backend-port"] with
                            | Error error -> Error error
                            | Ok backendPort ->
                                ForwardingConfiguration.Create(
                                    IPEndPoint(listenAddress, listenPort),
                                    IPEndPoint(backendAddress, backendPort)
                                )
                                |> Ok
