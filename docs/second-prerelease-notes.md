# PipeL4 v0.1.0-alpha.2 — release notes

PipeL4 is an experimental F#/.NET TCP byte-stream proxy for integration tests, protocol
experiments, and runtime research. This release adds the `PipeL4.Plugins` NuGet library as a
standalone public contract package, separate from the runtime and script host. The `PipeL4` .NET
Tool still installs the `pipel4` command and includes the matching contract assembly for its
script host. Running scripts through the tool needs no separate API package installation.

F# Interactive and editor tooling can resolve the public contracts through a conditional NuGet
reference in a `.fsx` processor. The PipeL4 host compiles that reference out and uses its bundled
assembly. Alpha.2 adds package and FSI validation, including an isolated external F# consumer
build, and extends Distribution validation to retain both package candidates with source-revision
records. These changes follow the alpha.1 tool release; they do not add a new processor API model.

## Install and try a processor

Install the .NET 10 SDK, which includes the runtime required by the tool. Install the exact
matching tool version from NuGet.org:

```sh
dotnet tool install --global PipeL4 --version 0.1.0-alpha.2
pipel4 --version
pipel4 examples copy AsciiUppercase
```

To update an existing global installation, run:

```sh
dotnet tool update --global PipeL4 --version 0.1.0-alpha.2
```

The exact-version package pages are [PipeL4 0.1.0-alpha.2](https://www.nuget.org/packages/PipeL4/0.1.0-alpha.2)
and [`PipeL4.Plugins` 0.1.0-alpha.2](https://www.nuget.org/packages/PipeL4.Plugins/0.1.0-alpha.2).
The tool bundles the public contract assembly for running scripts; the separate API package is for
F# Interactive, Ionide, and .NET/F# projects that reference the processor contracts.

The copied `AsciiUppercase.fsx` is a complete processor using `PipeL4.Plugins.IByteStreamProcessor`.
Edit it as an ordinary F# script. Its authoring reference is:

```fsharp
#if INTERACTIVE
#r "nuget: PipeL4.Plugins, 0.1.0-alpha.2"
#endif
```

This references the matching public API package for F# Interactive and Ionide. An independent
`net10.0` F# project can reference `PipeL4.Plugins` version `0.1.0-alpha.2` to compile against the
same public contracts. For SDKs whose implicit `FSharp.Core` version is older, set
`FSharpCoreImplicitPackageVersion=10.1.401` before launching F# Interactive or VS Code/Ionide. See
the [authoring guide](../README.md#f-script-authoring) for the exact directive rules and editor
workflow.

To run the copied script, start a TCP backend on `127.0.0.1:6000`, then launch the installed tool:

```sh
pipel4 --listen-address 127.0.0.1 --listen-port 5000 \
  --backend-host 127.0.0.1 --backend-port 6000 \
  --client-to-server-plugin ./AsciiUppercase.fsx
```

With the [README echo backend](../README.md#quick-start), sending `Hello, PipeL4!` through
`127.0.0.1:5000` returns `HELLO, PIPEL4!`:

```sh
printf 'Hello, PipeL4!\n' | nc -N 127.0.0.1 5000
```

This uses OpenBSD netcat's `-N` to half-close after input; other `nc` variants may require a
different flag. The tool compiles the script on load. The editor/FSI reference does not trigger a
NuGet restore inside PipeL4.

## Compatibility and limitations

The public processor types and signatures in `ProcessorApi.fs` are identical to those in the
alpha.1 source. Existing `.fsx` processors that use those documented contracts do not need source
changes to run with the alpha.2 tool; this is source compatibility and does not promise binary
compatibility for consumers compiled against an alpha.1 assembly. The conditional `#r` block is
optional for PipeL4 execution; add it when authoring with F# Interactive or an editor. Alpha.1
published only the `PipeL4` tool, so there is no alpha.1 public `PipeL4.Plugins` package to upgrade
from. No compatibility guarantee is made for later prereleases.

PipeL4 targets .NET 10. The release validation targets Ubuntu 24.04 x64; Windows and macOS have
not been verified for this prerelease. It handles TCP byte streams with a configured backend and
separate ordered pipelines in each direction. It does not add HTTP framing, TLS, or plugin routing.
The public processor API in alpha.2 is the existing `IByteStreamProcessor` contract, which
processors implement for the supported stream processing behavior. A function-first convenience
API has been proposed separately and is not included in this release.

Processors use the runtime-controlled `IByteStreamProcessor.ProcessAsync` model. The runtime owns
network and pipeline memory, schedules downstream work, and supplies completion and backpressure.
Scripts are trusted, in-process F# `.fsx` code compiled on load. The host supplies standard
.NET/F# libraries and PipeL4 APIs; it does not permit unrestricted package references, `#load`,
or `#I`. The guarded, versioned `PipeL4.Plugins` NuGet reference is for editor and FSI authoring
only. See the [architecture](architecture.md#plugins) for ownership and dependency rules.

PipeL4 is experimental and carries no production-readiness or performance guarantee. The
[alpha.1 release notes](first-prerelease-notes.md) remain the record for the earlier release.
