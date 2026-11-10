# PipeL4

PipeL4 is an experimental TCP byte-stream proxy with programmable F# processing pipelines.
It targets .NET 10 and requires the .NET 10 runtime (also included with the .NET 10 SDK).

After this prerelease is published to NuGet.org, install it with:

```sh
dotnet tool install --global PipeL4 --version 0.1.0-alpha.1
```

The installed command is `pipel4`. Run `pipel4 --help` for the full CLI reference and
`pipel4 --version` to check the installed version.

## Local TCP example

In one Unix-like terminal, start this Python 3 echo backend, which accepts one connection on
`127.0.0.1:6000`:

```sh
python3 - <<'PY'
import socket

with socket.socket() as listener:
    listener.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
    listener.bind(("127.0.0.1", 6000))
    listener.listen(1)
    connection, _ = listener.accept()
    with connection:
        while True:
            data = connection.recv(65536)
            if not data:
                break
            connection.sendall(data)
PY
```

In a second terminal, start PipeL4:

```sh
pipel4 --listen-address 127.0.0.1 --listen-port 5000 \
  --backend-host 127.0.0.1 --backend-port 6000
```

In a third terminal, use OpenBSD netcat to send a line and close its sending side at standard-input
EOF; other `nc` variants may need a different flag:

```sh
printf 'Hello, PipeL4!\n' | nc -N 127.0.0.1 5000
```

The response is `Hello, PipeL4!`. Stop PipeL4 with Ctrl+C. To process client-to-backend bytes,
restart the one-connection backend, then copy a script embedded in the installed tool and launch
PipeL4 with it:

```sh
pipel4 examples copy AsciiUppercase
pipel4 --listen-address 127.0.0.1 --listen-port 5000 \
  --backend-host 127.0.0.1 --backend-port 6000 \
  --client-to-server-plugin ./AsciiUppercase.fsx
```

Repeat the same `printf ... | nc -N ...` command. The response is `HELLO, PIPEL4!` because the
script uppercases ASCII lowercase bytes before forwarding them. `pipel4 examples list` shows the
other bundled scripts. Relative script paths are resolved from the current working directory.

## Scope

PipeL4 forwards TCP byte streams; it does not implicitly frame HTTP or another application
protocol. The backend is set by the endpoint options. There is no TLS support or production-readiness
guarantee in this experimental prerelease. Processor `.fsx` scripts compile on load and run as
trusted code in the PipeL4 process without a sandbox. If startup fails, check the .NET 10 runtime,
backend endpoint, and script path; plugin read and compilation failures are reported before the
listener starts.

[Bundled dependency notices](THIRD_PARTY_NOTICES.md) are included in this package.
