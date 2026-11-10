# PipeL4 v0.1.0-alpha.1

PipeL4 is an experimental F#/.NET TCP byte-stream proxy. This first public prerelease is for
integration tests, protocol experiments, and runtime research. Its architecture, processor API,
and behavior may change; it carries no production-readiness or performance guarantee.

PipeL4 forwards bytes between a TCP listener and a configured backend. Client-to-backend and
backend-to-client traffic have separate, ordered processor pipelines that can run concurrently.
Trusted F# `.fsx` scripts compile when loaded and can process byte streams through the PipeL4
processor API. The installed tool includes three copyable examples: `AsciiUppercase`,
`BytePatternReplace`, and `PrefixMode` (`pipel4 examples list`).

The package is a .NET Tool targeting .NET 10. The supported release assessment covers Ubuntu
24.04 x64; Windows and macOS have not been verified for this prerelease. PipeL4 supports TCP only,
with IPv4 or IPv6 literal endpoint addresses. It does not provide TLS, implicit HTTP framing,
plugin-controlled routing, or a plugin sandbox. Scripts run as trusted in-process code and have
limited dependency references.

After the package is published to nuget.org, install this exact prerelease version with the .NET
10 SDK. The installed command is `pipel4`:

```sh
dotnet tool install --global PipeL4 --version 0.1.0-alpha.1
pipel4 --help
```

For a first local TCP exchange and processor example, follow the [package README](../src/PipeL4/README.md#local-tcp-example)
or the fuller [repository quick start](../README.md#quick-start). PipeL4 is licensed under
[MIT](../LICENSE). The package includes [third-party notices](../src/PipeL4/THIRD_PARTY_NOTICES.md)
for distributed dependencies; the repository also has a [Paket tooling notice](../THIRD_PARTY_NOTICES.md).
