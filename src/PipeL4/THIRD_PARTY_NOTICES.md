# Bundled dependency notices

These notices cover third-party binaries distributed in the PipeL4 .NET Tool package under
`tools/net10.0/any/`. PipeL4's own MIT license and copyright are separate and are declared by the
package's MIT license expression and the repository's `LICENSE` file.

## F#

| NuGet package | Version | Distributed binaries |
| --- | --- | --- |
| FSharp.Compiler.Service | 43.12.401 | `FSharp.Compiler.Service.dll`, `FSharp.DependencyManager.Nuget.dll`, and their satellite resource assemblies |
| FSharp.Core | 10.1.401 | `FSharp.Core.dll` and its satellite resource assemblies |

The satellite resource assemblies for each of these three binaries are included in the `cs`, `de`,
`es`, `fr`, `it`, `ja`, `ko`, `pl`, `pt-BR`, `ru`, `tr`, `zh-Hans`, and `zh-Hant` directories.

The [F# MIT license](notices/FSharp.LICENSE.txt) applies to these binaries. Its copyright notice is
"Copyright (c) Microsoft Corporation. All rights reserved." Both package nuspecs also state:
"Contains code from the F# Software Foundation."

Source: the [F# license at the source commit recorded in both package nuspecs](https://github.com/dotnet/dotnet/blob/e34a38d2ae1fc26406a317517196e55c68ff83ab/src/fsharp/License.txt).
The exact `FSharp.Compiler.Service` and `FSharp.Core` NuGet packages contain no separate license or
third-party notice file; they declare the MIT license expression.

## .NET libraries

The following binaries each come from the NuGet package with the same name, version `10.0.12`:

- `Microsoft.Bcl.AsyncInterfaces.dll`
- `Microsoft.Extensions.Configuration.dll`
- `Microsoft.Extensions.Configuration.Abstractions.dll`
- `Microsoft.Extensions.Configuration.Binder.dll`
- `Microsoft.Extensions.DependencyInjection.dll`
- `Microsoft.Extensions.DependencyInjection.Abstractions.dll`
- `Microsoft.Extensions.Logging.dll`
- `Microsoft.Extensions.Logging.Abstractions.dll`
- `Microsoft.Extensions.Logging.Configuration.dll`
- `Microsoft.Extensions.Logging.Console.dll`
- `Microsoft.Extensions.Options.dll`
- `Microsoft.Extensions.Options.ConfigurationExtensions.dll`
- `Microsoft.Extensions.Primitives.dll`

The [version-matched .NET MIT license](notices/DotNet.LICENSE.txt) applies to these binaries. It
identifies ".NET Foundation and Contributors" as the copyright holder; the package nuspecs also
state "© Microsoft Corporation. All rights reserved."

The [complete .NET third-party notices](notices/DotNet.THIRD-PARTY-NOTICES.txt) are copied unchanged
from the `THIRD-PARTY-NOTICES.TXT` entry in the exact `10.0.12` NuGet packages. All thirteen package
copies are byte-identical. The upstream file does not map each notice section to individual shipped
assemblies, so the complete version-specific file is retained rather than selecting an uncertain
subset. Its text matches the .NET `v10.0.12` release file after line-ending normalization. Some
sections may therefore describe components outside this tool's payload.

Sources: [.NET 10.0.12 license](https://github.com/dotnet/runtime/blob/v10.0.12/LICENSE.TXT),
[.NET 10.0.12 third-party notices](https://github.com/dotnet/runtime/blob/v10.0.12/THIRD-PARTY-NOTICES.TXT),
and the `THIRD-PARTY-NOTICES.TXT` entry in each distributed package at version `10.0.12`.

When updating bundled dependencies, compare the new package payload with its originating NuGet
packages and refresh these notices from the matching versions.
