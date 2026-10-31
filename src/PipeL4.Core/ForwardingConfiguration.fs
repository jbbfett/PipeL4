namespace PipeL4.Core

open System
open System.Collections.Generic
open System.Globalization
open System.Net

[<Sealed>]
type ForwardingConfiguration private (
    listenEndpoint: IPEndPoint,
    backendEndpoint: IPEndPoint,
    clientToServerPluginPaths: string array,
    serverToClientPluginPaths: string array,
    clientToServerLogicalCapacityBytes: int64,
    serverToClientLogicalCapacityBytes: int64,
    connectionPhysicalCapacityBytes: int64,
    runtimeDiagnosticsEnabled: bool
) =
    member _.ListenEndpoint = listenEndpoint
    member _.BackendEndpoint = backendEndpoint
    member _.ClientToServerPluginPaths: IReadOnlyList<string> = clientToServerPluginPaths
    member _.ServerToClientPluginPaths: IReadOnlyList<string> = serverToClientPluginPaths
    member _.ClientToServerLogicalCapacityBytes: int64 = clientToServerLogicalCapacityBytes
    member _.ServerToClientLogicalCapacityBytes: int64 = serverToClientLogicalCapacityBytes
    member _.ConnectionPhysicalCapacityBytes: int64 = connectionPhysicalCapacityBytes
    member _.RuntimeDiagnosticsEnabled = runtimeDiagnosticsEnabled

    static member internal Create(
        listenEndpoint,
        backendEndpoint,
        clientToServerPluginPaths,
        serverToClientPluginPaths,
        clientToServerLogicalCapacityBytes,
        serverToClientLogicalCapacityBytes,
        connectionPhysicalCapacityBytes,
        runtimeDiagnosticsEnabled
    ) =
        ForwardingConfiguration(
            listenEndpoint,
            backendEndpoint,
            clientToServerPluginPaths,
            serverToClientPluginPaths,
            clientToServerLogicalCapacityBytes,
            serverToClientLogicalCapacityBytes,
            connectionPhysicalCapacityBytes,
            runtimeDiagnosticsEnabled
        )

    static member internal Create(
        listenEndpoint,
        backendEndpoint,
        clientToServerPluginPaths,
        serverToClientPluginPaths,
        clientToServerLogicalCapacityBytes,
        serverToClientLogicalCapacityBytes,
        connectionPhysicalCapacityBytes
    ) =
        ForwardingConfiguration.Create(
            listenEndpoint,
            backendEndpoint,
            clientToServerPluginPaths,
            serverToClientPluginPaths,
            clientToServerLogicalCapacityBytes,
            serverToClientLogicalCapacityBytes,
            connectionPhysicalCapacityBytes,
            true
        )

[<RequireQualifiedAccess>]
module ForwardingConfiguration =
    [<Literal>]
    let private ClientToServerPluginOption = "--client-to-server-plugin"

    [<Literal>]
    let private ServerToClientPluginOption = "--server-to-client-plugin"

    [<Literal>]
    let private ClientToServerLogicalCapacityOption =
        "--client-to-server-logical-capacity-bytes"

    [<Literal>]
    let private ServerToClientLogicalCapacityOption =
        "--server-to-client-logical-capacity-bytes"

    [<Literal>]
    let private ConnectionPhysicalCapacityOption = "--connection-physical-capacity-bytes"

    [<Literal>]
    let private DisableRuntimeDiagnosticsOption = "--disable-runtime-diagnostics"

    let private requiredOptionNames =
        [ "--listen-address"
          "--listen-port"
          "--backend-host"
          "--backend-port" ]

    let private knownOptions =
        Set.ofList(
            ClientToServerPluginOption
            :: ServerToClientPluginOption
            :: ClientToServerLogicalCapacityOption
            :: ServerToClientLogicalCapacityOption
            :: ConnectionPhysicalCapacityOption
            :: DisableRuntimeDiagnosticsOption
            :: requiredOptionNames
        )

    let private valuelessOptions = Set.singleton DisableRuntimeDiagnosticsOption

    let private isOptionLikeToken (value: string) =
        not (String.IsNullOrEmpty value)
        && value.StartsWith("--", StringComparison.Ordinal)

    [<NoEquality; NoComparison>]
    type private CollectedArguments =
        { Values: Dictionary<string, string>
          ClientToServerPluginPaths: ResizeArray<string>
          ServerToClientPluginPaths: ResizeArray<string>
          mutable DisableRuntimeDiagnostics: bool }

    let private collectArguments (arguments: string array) =
        let collected =
            { Values = Dictionary<string, string>(StringComparer.Ordinal)
              ClientToServerPluginPaths = ResizeArray()
              ServerToClientPluginPaths = ResizeArray()
              DisableRuntimeDiagnostics = false }

        let rec collect index =
            if index = arguments.Length then
                Ok collected
            else
                let optionName = arguments[index]

                if not (knownOptions.Contains optionName) then
                    Error $"Unknown argument '{optionName}'."
                elif valuelessOptions.Contains optionName then
                    if collected.DisableRuntimeDiagnostics then
                        Error $"Argument '{optionName}' was specified more than once."
                    else
                        collected.DisableRuntimeDiagnostics <- true
                        collect (index + 1)
                elif index + 1 >= arguments.Length || knownOptions.Contains arguments[index + 1] then
                    Error $"Missing value for argument '{optionName}'."
                elif isOptionLikeToken arguments[index + 1] then
                    Error
                        $"Unexpected option-like argument '{arguments[index + 1]}' after argument '{optionName}'; a value is required."
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

    let private hasBracketedAddressPort (value: string) =
        let candidate = value.Trim()

        if candidate.StartsWith("[", StringComparison.Ordinal) then
            let closingBracket = candidate.IndexOf(']')

            closingBracket >= 0
            && closingBracket + 1 < candidate.Length
            && candidate[closingBracket + 1] = ':'
        else
            false

    let private tryParseAddress optionName portOptionName (value: string): Result<IPAddress, string> =
        if hasBracketedAddressPort value then
            Error
                $"Argument '{optionName}' accepts an IP address only; supply the port with '{portOptionName}'."
        else
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

    let private tryParseCapacity optionName minimum (value: string) =
        match Int64.TryParse(value, NumberStyles.None, CultureInfo.InvariantCulture) with
        | true, capacity when capacity >= minimum -> Ok capacity
        | true, _ ->
            Error
                $"Argument '{optionName}' must be a decimal byte capacity of at least {minimum}."
        | _ ->
            Error
                $"Argument '{optionName}' must be a decimal byte capacity from {minimum} through {Int64.MaxValue}."

    let private tryParseOptionalCapacity
        (values: Dictionary<string, string>)
        optionName
        minimum
        =
        if values.ContainsKey optionName then
            tryParseCapacity optionName minimum values[optionName]
        else
            Ok Int64.MaxValue

    let tryParseArguments (arguments: string array) =
        match collectArguments arguments with
        | Error error -> Error error
        | Ok collected ->
            match findMissingOption collected.Values with
            | Some optionName -> Error $"Missing required argument '{optionName}'."
            | None ->
                let values = collected.Values

                match
                    tryParseAddress
                        "--listen-address"
                        "--listen-port"
                        values["--listen-address"]
                with
                | Error error -> Error error
                | Ok listenAddress ->
                    match tryParsePort "--listen-port" values["--listen-port"] with
                    | Error error -> Error error
                    | Ok listenPort ->
                        match
                            tryParseAddress
                                "--backend-host"
                                "--backend-port"
                                values["--backend-host"]
                        with
                        | Error error -> Error error
                        | Ok backendAddress ->
                            match tryParsePort "--backend-port" values["--backend-port"] with
                            | Error error -> Error error
                            | Ok backendPort ->
                                match
                                    tryParseOptionalCapacity
                                        values
                                        ClientToServerLogicalCapacityOption
                                        1L
                                with
                                | Error error -> Error error
                                | Ok clientToServerLogicalCapacityBytes ->
                                    match
                                        tryParseOptionalCapacity
                                            values
                                            ServerToClientLogicalCapacityOption
                                            1L
                                    with
                                    | Error error -> Error error
                                    | Ok serverToClientLogicalCapacityBytes ->
                                        match
                                            tryParseOptionalCapacity
                                                values
                                                ConnectionPhysicalCapacityOption
                                                2L
                                        with
                                        | Error error -> Error error
                                        | Ok connectionPhysicalCapacityBytes ->
                                            ForwardingConfiguration.Create(
                                                IPEndPoint(listenAddress, listenPort),
                                                IPEndPoint(backendAddress, backendPort),
                                                collected.ClientToServerPluginPaths.ToArray(),
                                                collected.ServerToClientPluginPaths.ToArray(),
                                                clientToServerLogicalCapacityBytes,
                                                serverToClientLogicalCapacityBytes,
                                                connectionPhysicalCapacityBytes,
                                                not collected.DisableRuntimeDiagnostics
                                            )
                                            |> Ok
