namespace PipeL4.Core

open System
open System.Collections.Generic
open System.Net

[<Sealed>]
type ForwardingConfiguration private (
    listenEndpoint: IPEndPoint,
    backendEndpoint: IPEndPoint,
    clientToServerPluginPaths: string array,
    serverToClientPluginPaths: string array
) =
    member _.ListenEndpoint = listenEndpoint
    member _.BackendEndpoint = backendEndpoint
    member _.ClientToServerPluginPaths: IReadOnlyList<string> = clientToServerPluginPaths
    member _.ServerToClientPluginPaths: IReadOnlyList<string> = serverToClientPluginPaths

    static member internal Create(
        listenEndpoint,
        backendEndpoint,
        clientToServerPluginPaths,
        serverToClientPluginPaths
    ) =
        ForwardingConfiguration(
            listenEndpoint,
            backendEndpoint,
            clientToServerPluginPaths,
            serverToClientPluginPaths
        )

[<RequireQualifiedAccess>]
module ForwardingConfiguration =
    [<Literal>]
    let private ClientToServerPluginOption = "--client-to-server-plugin"

    [<Literal>]
    let private ServerToClientPluginOption = "--server-to-client-plugin"

    let private requiredOptionNames =
        [ "--listen-address"
          "--listen-port"
          "--backend-host"
          "--backend-port" ]

    let private knownOptions =
        Set.ofList(
            ClientToServerPluginOption
            :: ServerToClientPluginOption
            :: requiredOptionNames
        )

    [<NoEquality; NoComparison>]
    type private CollectedArguments =
        { Values: Dictionary<string, string>
          ClientToServerPluginPaths: ResizeArray<string>
          ServerToClientPluginPaths: ResizeArray<string> }

    let private collectArguments (arguments: string array) =
        let collected =
            { Values = Dictionary<string, string>(StringComparer.Ordinal)
              ClientToServerPluginPaths = ResizeArray()
              ServerToClientPluginPaths = ResizeArray() }

        let rec collect index =
            if index = arguments.Length then
                Ok collected
            else
                let optionName = arguments[index]

                if not (knownOptions.Contains optionName) then
                    Error $"Unknown argument '{optionName}'."
                elif index + 1 >= arguments.Length || knownOptions.Contains arguments[index + 1] then
                    Error $"Missing value for argument '{optionName}'."
                else
                    let value = arguments[index + 1]

                    match optionName with
                    | ClientToServerPluginOption ->
                        collected.ClientToServerPluginPaths.Add value
                        collect (index + 2)
                    | ServerToClientPluginOption ->
                        collected.ServerToClientPluginPaths.Add value
                        collect (index + 2)
                    | _ when collected.Values.ContainsKey optionName ->
                        Error $"Argument '{optionName}' was specified more than once."
                    | _ ->
                        collected.Values.Add(optionName, value)
                        collect (index + 2)

        collect 0

    let private findMissingOption (values: Dictionary<string, string>) =
        requiredOptionNames |> List.tryFind (values.ContainsKey >> not)

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
        | Ok collected ->
            match findMissingOption collected.Values with
            | Some optionName -> Error $"Missing required argument '{optionName}'."
            | None ->
                let values = collected.Values

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
                                    IPEndPoint(backendAddress, backendPort),
                                    collected.ClientToServerPluginPaths.ToArray(),
                                    collected.ServerToClientPluginPaths.ToArray()
                                )
                                |> Ok
