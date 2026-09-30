namespace PipeL4.Core

open System
open System.Net.Sockets
open System.Threading

[<RequireQualifiedAccess>]
module TcpForwarder =
    let private connectBackend
        (configuration: ForwardingConfiguration)
        (client: TcpClient)
        (cancellationToken: CancellationToken)
        =
        task {
            use client = client
            use backend = new TcpClient(configuration.BackendEndpoint.AddressFamily)

            try
                do! backend.ConnectAsync(configuration.BackendEndpoint, cancellationToken)
            with :? SocketException ->
                ()
        }

    let runAsync (configuration: ForwardingConfiguration) (cancellationToken: CancellationToken) =
        task {
            let listener = new TcpListener(configuration.ListenEndpoint)

            try
                listener.Start()

                try
                    while true do
                        let! client = listener.AcceptTcpClientAsync(cancellationToken)
                        do! connectBackend configuration client cancellationToken
                with :? OperationCanceledException when cancellationToken.IsCancellationRequested ->
                    ()
            finally
                listener.Stop()
        }
