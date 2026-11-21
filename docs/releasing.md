# Release procedures and publication records

## 0.1.0-alpha.1 publication record

`PipeL4` version `0.1.0-alpha.1` has been released as the [NuGet package](https://www.nuget.org/packages/PipeL4/0.1.0-alpha.1)
and [GitHub prerelease](https://github.com/jbbfett/PipeL4/releases/tag/v0.1.0-alpha.1), from source
commit `075a4502101cef4740c1d4496b3785bbc0771bef`, under tag `v0.1.0-alpha.1`. The [release notes](first-prerelease-notes.md)
document this release's scope and installation instructions.

## Lockstep packages

Future releases distribute both `PipeL4` (the .NET tool) and `PipeL4.Plugins` (the public API
library) at the same NuGet version and publish them together. Set the single
`PipeL4PackageVersion` property in [`Directory.Build.props`](../Directory.Build.props); both
distributable projects derive their package `Version` from it. The current candidate is
`0.1.0-alpha.2`, following the published `PipeL4 0.1.0-alpha.1` CLI release. The alpha.1 release
was the tool package; the matching `PipeL4.Plugins` package was not published at that version. This
checkout identifies both alpha.2 packages as local release candidates. Verify their live NuGet
status and publishing account ownership before release. The commands below build and validate both
packages; CI and Distribution validation do not publish them.

## 0.1.0-alpha.2 release procedure

### Package model and compatibility

The release consists of two packages at the same NuGet version:

- `PipeL4` is the `net10.0` .NET Tool. It installs the `pipel4` command and bundles the public
  contract assembly and runtime dependencies required by the tool.
- `PipeL4.Plugins` is a `net10.0` library containing the public F# processor contracts. It contains
  the contract assembly, README, and MIT license, and declares `FSharp.Core` as its only package
  dependency. It does not depend on the tool, runtime, or script host.

Both package versions come from `PipeL4PackageVersion` in
[`Directory.Build.props`](../Directory.Build.props). For this release, the version is
`0.1.0-alpha.2`; the package version is separate from the public contract assembly's CLR version
`1.0.0.0`. Public API consumers need a .NET 10-compatible project and the matching
`PipeL4.Plugins` package version. The tool already carries that assembly, so a script run by
`PipeL4` does not need a separate package install or restore.

For editor and F# Interactive type checking, the repository example guards the package reference
with `INTERACTIVE`:

```fsharp
#if INTERACTIVE
#r "nuget: PipeL4.Plugins, 0.1.0-alpha.2"
#endif
```

The alpha.2 version in this checkout is a local candidate in the documented workflow. The PipeL4
host compiles the guarded reference out and uses its bundled contract assembly. A public consumer
should use the same released version of `PipeL4.Plugins` as the tool. A NuGet package version that
has already been published cannot be replaced.

F# Interactive also resolves an implicit `FSharp.Core` package. The published alpha.2 package
requires `FSharp.Core` `10.1.401`; set `FSharpCoreImplicitPackageVersion=10.1.401` when authoring
against that published package with an older SDK default, as described in the README. The local
candidate built from this checkout declares `10.0.100` and must be checked with the SDK default,
without that override. The setting does not change PipeL4's runtime loading path.

### 1. Local source validation

Run from a clean checkout of the release branch. Record the selected SDK and source revision. The
repository requests SDK `10.0.112` in `global.json`; use the SDK selected by the installed `dotnet`
resolver. These commands assume a Unix-like shell and the configured Microsoft.Testing.Platform
runner:

```sh
set -euo pipefail
dotnet --version
git status --short
git rev-parse HEAD
dotnet tool restore
dotnet clean PipeL4.sln --configuration Release -m:1 -p:PaketDisableCliTest=true -p:BuildInParallel=false
dotnet restore PipeL4.sln -p:PaketDisableCliTest=true -p:BuildInParallel=false
dotnet build PipeL4.sln --configuration Release --no-restore -p:PaketDisableCliTest=true
```

Stop on a failed command. Do not change the test runner configuration to work around an invocation
or environment failure; record and resolve the failure before release review.

### 2. Create and inspect both local packages

Use fresh output directories. Both `dotnet pack` commands must complete without a missing README
warning (`NU5046`), and each directory must contain only its expected package:

```sh
set -euo pipefail
tool_dir=./artifacts/second-prerelease/tool
api_dir=./artifacts/second-prerelease/plugins
test ! -e "$tool_dir"
test ! -e "$api_dir"
mkdir -p "$tool_dir" "$api_dir"
dotnet pack src/PipeL4/PipeL4.fsproj --configuration Release --no-restore \
  -p:PaketDisableCliTest=true -p:BuildInParallel=false --output "$tool_dir"
dotnet pack src/PipeL4.Plugins/PipeL4.Plugins.fsproj --configuration Release --no-restore \
  -p:PaketDisableCliTest=true -p:BuildInParallel=false --output "$api_dir"
tool_package="$tool_dir/PipeL4.0.1.0-alpha.2.nupkg"
api_package="$api_dir/PipeL4.Plugins.0.1.0-alpha.2.nupkg"
test -f "$tool_package"
test -f "$api_package"
test "$(find "$tool_dir" -maxdepth 1 -type f -name '*.nupkg' | wc -l)" -eq 1
test "$(find "$api_dir" -maxdepth 1 -type f -name '*.nupkg' | wc -l)" -eq 1
```

Inspect package entries and nuspecs directly. Check each package ID, version, target, author,
description, tags, project and repository URLs, license metadata, README, and payload. Confirm the
tool nuspec identifies a `DotnetTool` with command `pipel4`; confirm its runtime files include every
asset from `PipeL4.deps.json` and include `PipeL4.Plugins.dll`. Confirm the library's only assembly
is `lib/net10.0/PipeL4.Plugins.dll` and its only dependency is `FSharp.Core`.

```sh
unzip -l "$tool_package"
unzip -p "$tool_package" PipeL4.nuspec
unzip -l "$api_package"
unzip -p "$api_package" PipeL4.Plugins.nuspec
sha256sum "$tool_package" "$api_package"
```

Record each path and checksum with the source revision. These pre-merge packages are review
candidates; final release artifacts must come from the hosted Distribution validation run for the
squash-merged source SHA.

### 3. Verify package consumers

Run the repository validators and consumer checks against those exact files. The public API
validator checks the package metadata and payload, creates a separate F# consumer project, restores
from an isolated local feed containing the candidate and cached `FSharp.Core`, and builds it. The
FSI checks use the same `AsciiUppercase.fsx` example referenced above.

```sh
set -euo pipefail
tool_dir=./artifacts/second-prerelease/tool
api_dir=./artifacts/second-prerelease/plugins
tool_package="$tool_dir/PipeL4.0.1.0-alpha.2.nupkg"
api_package="$api_dir/PipeL4.Plugins.0.1.0-alpha.2.nupkg"
dotnet fsi scripts/validate_package.fsx "$tool_package"
dotnet fsi scripts/test_validate_package.fsx "$tool_package"
dotnet fsi scripts/test_validation_exit_codes.fsx "$tool_dir"
dotnet fsi scripts/validate_public_api_package.fsx "$api_package"
dotnet fsi scripts/test_public_api_consumer.fsx "$api_package"
dotnet fsi scripts/test_fsi_authoring_cache.fsx "$api_package" examples/AsciiUppercase.fsx
dotnet fsi scripts/validate_fsi_authoring.fsx "$api_package" examples/AsciiUppercase.fsx
PIPEL4_PACKAGE_PATH="$tool_package" dotnet test --solution PipeL4.sln \
  --configuration Release --no-restore -p:PaketDisableCliTest=true -p:BuildInParallel=false
```

The package-backed suite includes installed-tool and TCP integration coverage. Also install the
exact local tool package into a disposable path, not an existing global installation, and check its
first-use commands:

```sh
set -euo pipefail
tool_dir=./artifacts/second-prerelease/tool
tool_path="$(mktemp -d)"
dotnet tool install PipeL4 --tool-path "$tool_path" --source "$tool_dir" --version 0.1.0-alpha.2
"$tool_path/pipel4" --help
"$tool_path/pipel4" --version
"$tool_path/pipel4" examples list
```

Stop if a validator, consumer build, FSI check, package-backed test, or installed-tool check fails.
Repeat checks affected by changes to source, package metadata, or package content.

### 3a. Manual VS Code/Ionide authoring gate

Before approving the pre-merge candidate, a reviewer must perform this check in VS Code with the
Ionide F# extension and the .NET 10 SDK installed. Start with the
[README authoring instructions](../README.md#f-script-authoring). Complete the build in step 1
first. Run the following block from the repository root as one fail-fast shell sequence. It packs
a fresh candidate into a new, empty directory, validates that exact file, and keeps it in place
through the manual editor check. The external workspace pins the SDK actually selected in the
repository, with roll-forward disabled. Stage that SDK's default `FSharp.Core` package in the
local feed; if it is absent from the normal package cache, a disposable F# project fetches it
from NuGet.org first:

```sh
set -euo pipefail
repo_root="$(pwd)"
repo_sdk="$(dotnet --version)"
ide_root="$(mktemp -d)"
candidate_dir="$ide_root/candidate"
api_package="$candidate_dir/PipeL4.Plugins.0.1.0-alpha.2.nupkg"
api_feed="$ide_root/feed"
extensions_dir="$HOME/.vscode/extensions"
sdk_fsharp_version="$(dotnet msbuild src/PipeL4.Plugins/PipeL4.Plugins.fsproj -getProperty:FSCorePackageVersion)"
global_packages="$(dotnet nuget locals global-packages --list | sed -n 's/^global-packages: //p')"
fsharp_package="$global_packages/fsharp.core/$sdk_fsharp_version/fsharp.core.$sdk_fsharp_version.nupkg"
mkdir -p "$candidate_dir" "$ide_root/workspace" "$api_feed" "$ide_root/home" "$ide_root/packages" "$ide_root/http-cache" "$ide_root/dotnet-home" "$ide_root/runtime"
dotnet pack src/PipeL4.Plugins/PipeL4.Plugins.fsproj --configuration Release --no-restore \
  -p:PaketDisableCliTest=true -p:BuildInParallel=false --output "$candidate_dir"
test -f "$api_package"
dotnet fsi scripts/validate_fsi_authoring.fsx "$api_package" examples/AsciiUppercase.fsx
cp examples/AsciiUppercase.fsx "$ide_root/workspace/AsciiUppercase.fsx"
cat > "$ide_root/workspace/global.json" <<EOF
{"sdk":{"version":"$repo_sdk","rollForward":"disable"}}
EOF
if ! test -f "$fsharp_package"; then
  dotnet new classlib --language F# --framework net10.0 --output "$ide_root/core-stage" --no-restore
  dotnet restore "$ide_root/core-stage/core-stage.fsproj" --source https://api.nuget.org/v3/index.json
fi
cp "$api_package" "$api_feed/"
cp "$fsharp_package" "$api_feed/"
cat > "$ide_root/workspace/NuGet.Config" <<EOF
<configuration>
  <packageSources>
    <clear />
    <add key="candidate" value="$api_feed" />
  </packageSources>
</configuration>
EOF
(
  cd "$ide_root/workspace"
  unset FSharpCoreImplicitPackageVersion NoWarn
  export RestoreSources="$api_feed" NUGET_PACKAGES="$ide_root/packages" NUGET_HTTP_CACHE_PATH="$ide_root/http-cache"
  export DOTNET_CLI_HOME="$ide_root/dotnet-home" HOME="$ide_root/home" XDG_RUNTIME_DIR="$ide_root/runtime"
  workspace_sdk="$(dotnet --version)"
  printf 'Workspace SDK: %s\n' "$workspace_sdk"
  test "$workspace_sdk" = "$repo_sdk"
  test "$(dotnet msbuild "$repo_root/src/PipeL4.Plugins/PipeL4.Plugins.fsproj" -getProperty:FSCorePackageVersion)" = "$sdk_fsharp_version"
  dotnet fsi --exec ./AsciiUppercase.fsx
  cmp "$api_package" "$ide_root/packages/pipel4.plugins/0.1.0-alpha.2/pipel4.plugins.0.1.0-alpha.2.nupkg"
  code --new-window --user-data-dir "$ide_root/vscode-data" \
    --extensions-dir "$extensions_dir" "$ide_root/workspace"
)
```

The `--extensions-dir` path assumes the normal VS Code extension directory on Linux; adjust it to
the actual directory containing Ionide. Keep this window separate from an existing VS Code process
so the environment reaches Ionide and FSI. FSI's script dependency restore did not honor Package
Source Mapping in this workflow: with an empty candidate feed, it loaded the published alpha.2
package from another configured source. The sole restore source makes that impossible while the
staged `FSharp.Core` package supplies the SDK default. NuGet.org is used only during setup if that
package is not cached. The empty package cache and home prevent an older package or a previous FSI
dependency-manager result from satisfying the check. The workspace-local `global.json` requires
the exact repository-selected SDK; a missing SDK stops the workflow instead of selecting another
feature band. Keep `ide_root` until the manual check is complete. Inspect the `#if INTERACTIVE`
reference in the copied file and confirm that it names `PipeL4.Plugins, 0.1.0-alpha.2`.

In that external workspace, open `AsciiUppercase.fsx`. Wait for Ionide to finish, then confirm
`IByteStreamProcessor` and `ProcessorResult` resolve, hover shows their F# types, and diagnostics
appear for a temporary `let intentionalError: int = "wrong type"` line and clear when it is
removed. Use **F#: Send File to F# Interactive** and confirm successful evaluation. In that FSI
session, evaluate `typeof<IByteStreamProcessor>.Assembly.Location` and record the returned path.
In VS Code's integrated terminal at the workspace root, run `dotnet --version` and
`dotnet fsi --version`; confirm the SDK matches `repo_sdk` and inspect Ionide's F# output for the
SDK/toolchain path it selected. Stop if Ionide reports a different SDK or cannot resolve the script.
Confirm that no PipeL4 source project or project reference is present in the workspace. After FSI
restore, check the selected SDK-default package (currently `10.0.115`), compare the cached package
with the candidate, and compare the DLL loaded by FSI with the DLL inside that candidate:

```sh
test "$sdk_fsharp_version" = 10.0.115 # expected for the currently installed SDK
test -f "$ide_root/packages/fsharp.core/$sdk_fsharp_version/fsharp.core.$sdk_fsharp_version.nupkg"
cmp "$api_package" "$ide_root/packages/pipel4.plugins/0.1.0-alpha.2/pipel4.plugins.0.1.0-alpha.2.nupkg"
test "$fsi_dll" = "$ide_root/packages/pipel4.plugins/0.1.0-alpha.2/lib/net10.0/PipeL4.Plugins.dll"
unzip -p "$api_package" lib/net10.0/PipeL4.Plugins.dll | sha256sum
sha256sum "$fsi_dll"
```

Set `fsi_dll` to the exact path printed by FSI before running the last three commands. If the
installed SDK default changes, record the observed version instead of using the `10.0.115` check.
The two DLL hashes must match. Record the IDE/extension and SDK versions, the
observed type information, diagnostics, FSI result, and matching hashes. A failure or an unperformed
IDE check blocks release approval. Repository FSI validation is separate evidence and does not
complete this manual gate. **Execution pending:** this procedure has not itself been performed in an
IDE as part of the documentation change.

### 4. Independent release audit

Have a reviewer who did not create the candidate inspect the source diff, both local packages,
nuspecs, checksums, validation output, consumer evidence, and the manual IDE result from step 3a.
Confirm both package IDs and versions, the documented dependency boundary, and release scope. The
reviewer should identify the candidate source revision and explicitly approve moving it to `master`.
Stop for an unresolved finding.

### 4a. Hosted pull request validation before merge

Before squash-merging, require both GitHub Actions checks to succeed for the latest pull request
revision:

- `CI / Build and test`
- `Distribution validation / Build and validate distributable packages`

Confirm each run belongs to this pull request and corresponds to its current head and base
revisions. Both workflows use `actions/checkout@v7` without a `ref` override. For a
`pull_request` event, that checks out GitHub's synthetic pull request merge ref; the run's tested
`GITHUB_SHA` can therefore differ from the pull request branch's head SHA. Do not require those
SHAs to be equal. Record both run URLs, the pull request head and base SHAs, the event, and each
run's tested SHA. If the pull request head or base changes after validation, require both checks to
pass again for the updated pull request state and record the new runs before merging.

### 5. Squash merge into `master`

Merge the approved release pull request into `master` using the repository's squash-merge
convention. Do not tag or publish the pre-merge packages. The squash merge creates the source
revision from which the final release artifacts must be built.

### 6. Hosted CI and Distribution validation

After the merge, verify both hosted workflows for the same `master` commit. These runs are separate
from the pre-merge pull request checks in step 4a:

- [CI](../.github/workflows/ci.yml) restores, builds, tests, packs, and validates the `PipeL4` tool
  package. It does not validate the `PipeL4.Plugins` distribution by itself.
- [Distribution validation](../.github/workflows/distribution-validation.yml) packs both packages,
  validates each package and their matching versions, builds the independent public API consumer,
  checks FSI authoring, runs the package-backed solution suite, and uploads both candidates with
  `source-revision.txt` files.

Both workflows run on pushes and pull requests targeting `master` and support manual dispatch.
Neither workflow publishes NuGet packages, creates a Git tag, or publishes a GitHub Release. Save
the run URLs, confirm every required job passed, and download the Distribution artifact for the
merged commit. Do not use an artifact from a pre-merge or unrelated run.

### 7. Verify the final source SHA

Before tagging, check the clean `master` checkout, remote branch, both downloaded
`source-revision.txt` files, and the `repository commit` attributes in both package nuspecs. Each
must identify the same full squash-merge commit SHA. Distribution records the source revision in
the artifact but does not compare it with both nuspec commits, so make that comparison explicitly.
Re-run package validators on the downloaded files and record their SHA-256 checksums. Stop if the
workflow, source revision, nuspec commit, or local `master` SHA differs.

```sh
git status --short
git rev-parse HEAD
git ls-remote origin refs/heads/master
```

Compare these revisions with the hosted run's commit and both artifact revision files. Inspect the
repository commit in `PipeL4.nuspec` and `PipeL4.Plugins.nuspec`; verify both equal that same SHA.

```sh
dotnet fsi scripts/validate_package.fsx /path/to/PipeL4.0.1.0-alpha.2.nupkg
dotnet fsi scripts/validate_public_api_package.fsx /path/to/PipeL4.Plugins.0.1.0-alpha.2.nupkg
sha256sum /path/to/PipeL4.0.1.0-alpha.2.nupkg /path/to/PipeL4.Plugins.0.1.0-alpha.2.nupkg
```

The earlier alpha.1 [tool package](https://www.nuget.org/packages/PipeL4/0.1.0-alpha.1) and tag
are historical and must not be reused for alpha.2.

### 8. Verify publication eligibility before tagging

Before creating or pushing the alpha.2 tag, verify eligibility and exact-version availability for
both package IDs with the intended publishing account:

- Confirm the account or organization is authorized to publish `PipeL4`.
- Confirm it is authorized to publish `PipeL4.Plugins`, including package ID reservation, ownership,
  or transfer constraints. The prior `PipeL4` publication does not establish permission for this
  second ID.
- Confirm `0.1.0-alpha.2` is available for each package and that neither exact version is already
  published, unlisted, pending, or otherwise in conflict.

Use the publisher account's package management and ownership information, and contact NuGet or the
current package owner if reservation or ownership is unclear. A public feed query, search result,
or missing package page alone does not prove publishing authorization or claimability. Record the
evidence for both IDs. Any failed or inconclusive ownership, authorization, or version-availability
check blocks tagging; stop and resolve it before continuing.

Record final release approval only after the hosted checks, final source and artifact provenance,
manual IDE result, and both publication-eligibility checks have passed. This approval applies to
the final squash-merged revision and its downloaded Distribution artifacts, not the pre-merge
candidate.

### 9. Create and verify the Git tag

Only after steps 7 and 8 pass, create an annotated tag at the approved full source SHA and push
only that tag. Confirm that the remote peeled tag target equals the approved commit; never move or
overwrite an existing tag.

```sh
set -euo pipefail
approved_sha="<APPROVED-SOURCE-SHA>"
release_tag=v0.1.0-alpha.2
test -z "$(git status --porcelain)"
test "$(git rev-parse HEAD)" = "$approved_sha"
test "$(git branch --show-current)" = master
test -z "$(git tag --list "$release_tag")"
remote_before="$(git ls-remote --tags origin "refs/tags/${release_tag}" "refs/tags/${release_tag}^{}")"
test -z "$remote_before"
git tag -a "$release_tag" "$approved_sha" -m "PipeL4 0.1.0-alpha.2"
test "$(git rev-parse "refs/tags/${release_tag}^{commit}")" = "$approved_sha"
git push origin "refs/tags/${release_tag}"
remote_after="$(git ls-remote --tags origin "refs/tags/${release_tag}" "refs/tags/${release_tag}^{}")"
remote_commit="$(printf '%s\n' "$remote_after" | awk -v ref="refs/tags/${release_tag}^{}" '$2 == ref {print $1}')"
test "$remote_commit" = "$approved_sha"
```

If a local or remote tag already exists, a push result is ambiguous, or the peeled target differs,
stop and investigate. Do not delete, move, or force-push the tag. Record the verified remote tag
target with the release evidence.

### 10. Manually publish both NuGet packages

Immediately before the first upload, repeat the eligibility check from step 8 for both IDs:
confirm publishing authorization and confirm neither exact version has become published, unlisted,
pending, or conflicting since the tag was pushed. Do not treat a NuGet feed query alone as proof of
authorization. Before each later upload, recheck authorization and exact-version status for that
package, accounting for any earlier package already confirmed published.

Use the two exact downloaded and approved `.nupkg` files from step 7, recheck their checksums, and
submit each through the signed-in NuGet Gallery **Upload Package** flow. Check the displayed package
ID and `0.1.0-alpha.2` version for each submission. No package-publishing workflow is configured
in the checked-in repository. If using a CLI, follow the official
[`dotnet nuget push` instructions](https://learn.microsoft.com/en-us/dotnet/core/tools/dotnet-nuget-push)
with an explicit NuGet source and protected API key; never put a key in source, shell history, or
retained logs.

NuGet submissions are independent, not atomic. After **any** failed, timed-out, or uncertain
submission, stop all further package submissions and do not create the GitHub Release. Inspect that
package's status in the publisher account and on NuGet, including published or unlisted state,
validation messages, pending uploads, and notification email. Establish whether the exact ID and
version were accepted, rejected/not accepted, or remain pending/uncertain. Do not blindly retry the
same package or proceed to the next package.

- **Accepted:** Verify the accepted package is the reviewed artifact and record its status. Do not
  retry it. Resume only after confirming the remaining package is still authorized and its exact
  version is available.
- **Rejected or confirmed not accepted:** Record the reason. Retry only after NuGet confirms there
  is no accepted or pending upload, the same reviewed file and checksum are still approved, and
  publishing authorization remains valid. If resolving the rejection changes package content or
  metadata, stop for a new artifact and release review.
- **Pending or uncertain:** Wait for NuGet to resolve the status or contact NuGet support. Do not
  retry or submit the other package while the result is unknown.

Resume submissions only after the package state is positively established and fresh authorization
and version-availability checks confirm that the next action is safe.

If one package succeeds and the other fails or remains uncertain, keep the successful version
unchanged and apply the same recovery steps to the other package; do not proceed to GitHub Release
publication until both packages are confirmed. A published package cannot be replaced. If a
published artifact needs content or metadata changes, stop and review a new coordinated version
for both packages. A duplicate-upload response alone does not prove that the intended file was
accepted.

### 11. Verify both public packages

Wait for validation and indexing. Verify the public pages for
[`PipeL4 0.1.0-alpha.2`](https://www.nuget.org/packages/PipeL4/0.1.0-alpha.2) and
[`PipeL4.Plugins 0.1.0-alpha.2`](https://www.nuget.org/packages/PipeL4.Plugins/0.1.0-alpha.2)
show the expected IDs, version, author, license, README, target framework, and dependency metadata.
Download each public package and compare its identity, nuspec, and non-signature payload with the
approved artifact. If NuGet has added package-signature metadata, inspect it separately from the
application payload. From a clean .NET 10 environment, install the exact public tool version and
run first-use commands:

```sh
public_tool_path="$(mktemp -d)"
dotnet tool install PipeL4 --tool-path "$public_tool_path" \
  --source https://api.nuget.org/v3/index.json --version 0.1.0-alpha.2
"$public_tool_path/pipel4" --help
"$public_tool_path/pipel4" --version
"$public_tool_path/pipel4" examples list
```

Compile an independent F# consumer against the public `PipeL4.Plugins` `0.1.0-alpha.2` package in
a fresh external .NET 10 environment, restoring only from NuGet.org. Check public FSI authoring
with an empty package and dependency-manager cache, outside the repository:

```sh
public_fsi_root="$(mktemp -d)"
mkdir -p "$public_fsi_root/home" "$public_fsi_root/packages" "$public_fsi_root/http-cache" "$public_fsi_root/dotnet-home"
cp examples/AsciiUppercase.fsx "$public_fsi_root/AsciiUppercase.fsx"
HOME="$public_fsi_root/home" DOTNET_CLI_HOME="$public_fsi_root/dotnet-home" \
  NUGET_PACKAGES="$public_fsi_root/packages" NUGET_HTTP_CACHE_PATH="$public_fsi_root/http-cache" \
  RestoreSources=https://api.nuget.org/v3/index.json FSharpCoreImplicitPackageVersion=10.1.401 \
  dotnet fsi --exec "$public_fsi_root/AsciiUppercase.fsx"
```

Confirm the guarded `#r "nuget: PipeL4.Plugins, 0.1.0-alpha.2"` restored the public package in the
fresh cache. Repeat the TCP echo and processor check with the public tool installation. Do not
continue while either package page, restore, consumer build, FSI check, or tool check is missing or
failing.

### 12. Publish the GitHub prerelease

Only after both packages are publicly available and verified, review the
[alpha.2 release notes](second-prerelease-notes.md) against the published artifacts. Then
manually draft a GitHub Release
from the `v0.1.0-alpha.2` tag. Mark it as a prerelease, link both
NuGet packages, and verify the release page, tag target, notes, and links before publishing.
GitHub Release creation is separate from tagging and NuGet publication. Record the public release
and package URLs.

## Historical alpha.1 release procedure

The gates below preserve the first release's original procedure, values, approvals, and provenance.
Its actions for `0.1.0-alpha.1` are complete. The hard-coded version, package list, paths, and
release notes are historical and must not be reused for alpha.2. Follow the alpha.2 procedure above
for the two-package release.

The [CI workflow](../.github/workflows/ci.yml) runs on pushes to `master`, pull requests targeting
`master`, and manual dispatch. It restores, builds, tests, packs, and validates the `PipeL4` tool
package on Ubuntu 24.04. The separate [distribution validation workflow](../.github/workflows/distribution-validation.yml)
also runs on Ubuntu 24.04, validates both packages, and can upload candidates with
`source-revision.txt`; it does not publish. The earlier
[local readiness report](release-readiness.md) covers a different historical package and cannot
substitute for validation of this final prerelease.

### Gate A — approved source is on master

**Proceed only after PRE-06 has passed and `release/first-prerelease` has been squash-merged into
`master`.** Record the final `master` SHA and check the clean working tree. Review reachable public
history and tracked files for secrets, private files, generated artifacts, and unwanted commits.
Keep old experimental branches and unwanted tags local. A squash merge alone does not prove the
earlier `master` ancestry is suitable for publication. From the intended `master` checkout:

```sh
git status --short
git rev-parse HEAD
git log --oneline --decorate master
git ls-files
git branch --show-current
```

Evidence: PRE-06 approval, merge result, clean status, reviewed commit graph and tracked-file
inventory. Stop if `master` or its reachable history is not approved for public release.

### Gate B — public GitHub repository and hosted CI

The public repository identity is established as
[`jbbfett/PipeL4`](https://github.com/jbbfett/PipeL4), with canonical URL
`https://github.com/jbbfett/PipeL4`. Verify that the configured remote points to this repository
and record the currently published `master` SHA for comparison with the intended source:

```sh
git remote -v
git ls-remote origin refs/heads/master
```

Do not blindly add or replace an existing remote. Do not use `git push --all`, `git push --mirror`,
or a tag push at this gate. Verify the public branch list
and tags contain only intended refs. Confirm `master` is the GitHub default branch (set it in
repository Settings if needed). Inspect the public [README](../README.md), [MIT license](../LICENSE),
[repository notices](../THIRD_PARTY_NOTICES.md), and [package notices](../src/PipeL4/THIRD_PARTY_NOTICES.md).
Run or observe the first hosted **CI** workflow for the published `master` SHA. Check every step,
including package validation, regression checks, and package-backed tests. Record its run URL and
resolved runner image. If CI fails, stop, fix through review, and repeat for the new `master` SHA.

Evidence: approved owner/URL, public `master` history and files, default branch, intended ref
list, and passing hosted CI at the relevant SHA. See [GitHub's import instructions](https://docs.github.com/en/migrations/importing-source-code/using-the-command-line-to-import-source-code/adding-locally-hosted-code-to-github)
and [default-branch instructions](https://docs.github.com/en/repositories/configuring-branches-and-merges-in-your-repository/managing-branches-in-your-repository/changing-the-default-branch).

### Gate C — final metadata, source, artifact, and verified tag

#### Publication prerequisites

Settle the final source before tagging or packing. The project metadata records the known GitHub
identity in `PackageProjectUrl`, `RepositoryUrl`, and `RepositoryType`; package validation requires
those exact values. This identifies the source repository only. NuGet publication identity and
authorization were confirmed for the first release: the nuget.org account or organization had to
confirm it could publish PackageId `PipeL4` and would own the package. A previously observed 404
from its package index did not prove the ID was claimable. Confirm version
`0.1.0-alpha.1`, ToolCommandName `pipel4`, author Michael Krotenko, and MIT license in
[`PipeL4.fsproj`](../src/PipeL4/PipeL4.fsproj). After the metadata change is approved and merged,
rerun hosted CI, then rebuild and revalidate the package from that source revision. Verify that the
[package notices](../src/PipeL4/THIRD_PARTY_NOTICES.md) still cover the distributed F# and Microsoft
assemblies; the [Paket notice](../THIRD_PARTY_NOTICES.md) covers build tooling outside the package.
Refresh the inventory if dependencies or payload change. For the published release, the source SHA
is `075a4502101cef4740c1d4496b3785bbc0771bef`, the artifact passed its release validation, and tag
`v0.1.0-alpha.1` points to that source. The remaining checks below preserve the procedure used to
verify those release inputs.

Freeze the approved `master` SHA after all release-affecting source and documentation edits. Plan
`v0.1.0-alpha.1` for that SHA, but create no tag until source is final and the artifact passes all
checks. Use a clean checkout at that exact commit and the SDK requested by `global.json`
(`10.0.112`; verify the selected SDK with `dotnet --version`). The commands assume a Unix-like
shell and fresh `artifacts/first-prerelease` directory. A CI-downloaded candidate may be used
instead only after matching its recorded SHA, contents, test results, and checksum to this gate.

```sh
set -euo pipefail
git switch master
test -z "$(git status --porcelain)"
approved_sha="$(git rev-parse HEAD)"
git rev-parse HEAD
dotnet --version
dotnet tool restore
dotnet clean src/PipeL4/PipeL4.fsproj --configuration Release -p:PaketDisableCliTest=true -p:BuildInParallel=false
dotnet restore PipeL4.sln -p:PaketDisableCliTest=true -p:BuildInParallel=false
dotnet build PipeL4.sln --configuration Release --no-restore -p:PaketDisableCliTest=true
mkdir -p artifacts/first-prerelease
test -z "$(find artifacts/first-prerelease -maxdepth 1 -type f -name '*.nupkg' -print -quit)"
dotnet pack src/PipeL4/PipeL4.fsproj --configuration Release --no-restore -p:PaketDisableCliTest=true -p:BuildInParallel=false --output ./artifacts/first-prerelease
package_path="./artifacts/first-prerelease/PipeL4.0.1.0-alpha.1.nupkg"
test -f "$package_path"
test "$(find artifacts/first-prerelease -maxdepth 1 -type f -name '*.nupkg' | wc -l)" -eq 1
dotnet fsi scripts/validate_package.fsx "$package_path"
dotnet fsi scripts/test_validate_package.fsx "$package_path"
dotnet fsi scripts/test_validation_exit_codes.fsx "$package_path"
PIPEL4_PACKAGE_PATH="$package_path" dotnet test --solution PipeL4.sln --configuration Release --no-restore -p:PaketDisableCliTest=true -p:BuildInParallel=false
test "$(git rev-parse HEAD)" = "$approved_sha"
test -z "$(git status --porcelain)"
python3 - "$package_path" "$approved_sha" <<'PY'
import re
import sys
import zipfile
import xml.etree.ElementTree as ET

def fail(message):
    raise SystemExit(f"Package source provenance check failed: {message}")

package_path, approved_sha = sys.argv[1:]
full_sha = re.compile(r"(?:[0-9a-f]{40}|[0-9a-f]{64})\Z")
if not full_sha.fullmatch(approved_sha):
    fail("approved_sha is not a full commit SHA")

supported_namespaces = {
    "http://schemas.microsoft.com/packaging/2010/07/nuspec.xsd",
    "http://schemas.microsoft.com/packaging/2011/08/nuspec.xsd",
    "http://schemas.microsoft.com/packaging/2011/10/nuspec.xsd",
    "http://schemas.microsoft.com/packaging/2012/06/nuspec.xsd",
    "http://schemas.microsoft.com/packaging/2013/01/nuspec.xsd",
    "http://schemas.microsoft.com/packaging/2013/05/nuspec.xsd",
}

try:
    with zipfile.ZipFile(package_path) as package:
        nuspec_names = [name for name in package.namelist() if name.lower().endswith(".nuspec")]
        if len(nuspec_names) != 1:
            fail(f"expected one embedded nuspec, found {len(nuspec_names)}")
        root = ET.fromstring(package.read(nuspec_names[0]))
except (OSError, zipfile.BadZipFile, ET.ParseError) as error:
    fail(f"could not read the selected package's embedded nuspec: {error}")

if not root.tag.startswith("{"):
    fail("nuspec package element has no NuGet namespace")
namespace, root_name = root.tag[1:].split("}", 1)
if root_name != "package" or namespace not in supported_namespaces:
    fail("nuspec package element has an unexpected expanded XML name")

def local_name(tag):
    return tag.rsplit("}", 1)[-1]

metadata_nodes = [element for element in root if local_name(element.tag) == "metadata"]
if len(metadata_nodes) != 1 or metadata_nodes[0].tag != f"{{{namespace}}}metadata":
    fail("nuspec metadata is missing, foreign, or ambiguous")
metadata = metadata_nodes[0]
repository_nodes = [element for element in metadata if local_name(element.tag) == "repository"]
if len(repository_nodes) != 1 or repository_nodes[0].tag != f"{{{namespace}}}repository":
    fail("canonical repository metadata is missing, foreign, or ambiguous")
repository = repository_nodes[0]

allowed_attributes = {"url", "type", "branch", "commit"}
if any(name.startswith("{") or name not in allowed_attributes for name in repository.attrib):
    fail("repository metadata has unexpected or namespace-qualified attributes")
if repository.attrib.get("url") != "https://github.com/jbbfett/PipeL4.git":
    fail("repository URL is absent or not canonical")
if repository.attrib.get("type") != "git":
    fail("repository type is absent or not git")
if len(repository) != 0 or (repository.text or "").strip():
    fail("repository metadata contains nested elements or non-whitespace text")

package_commit = repository.attrib.get("commit", "")
if not full_sha.fullmatch(package_commit):
    fail("repository commit is absent or is not a full commit SHA")
if package_commit != approved_sha:
    fail("repository commit does not exactly match approved_sha")

print(f"Verified package repository commit: {package_commit}")
PY
sha256sum "$package_path"
```

If the repository commit assertion fails, stop before recording or approving the checksum, creating
the tag, or authorizing NuGet publication. Investigate source provenance, then rebuild and revalidate
the candidate from the approved revision. Do not edit the nuspec inside the package to make the
assertion pass.

Require exactly one `.nupkg` in the output directory and no `NU5046` missing-README warning in
the pack output. The validator checks identity, version, tool metadata, runtime files, README,
notices, and unwanted content. Inspect the effective `.nuspec`, packaged `README.md`,
`THIRD_PARTY_NOTICES.md`, and `notices/` entries directly with `unzip -l` and `unzip -p`;
compare the nuspec with approved metadata decisions. For this package:

```sh
unzip -l "$package_path"
unzip -p "$package_path" PipeL4.nuspec
unzip -p "$package_path" README.md
unzip -p "$package_path" THIRD_PARTY_NOTICES.md
```

Read the bundled `notices/` license and attribution files listed by `unzip -l`. Record the SHA-256
and retain this exact `.nupkg` in a protected release location with the source SHA and validation results.
The package-backed suite includes isolated installed-tool coverage. Also install this package into
a disposable tool path, run `pipel4 --help`, `pipel4 --version`, and `pipel4 examples list`, then
perform the [local TCP echo and AsciiUppercase first-use smoke](../src/PipeL4/README.md#local-tcp-example)
using that installed command. Confirm version, forwarded echo, and transformed echo; do not let
an existing global `pipel4` satisfy this check. For an isolated Unix-like tool path:

```sh
tool_path="$(mktemp -d)"
dotnet tool install PipeL4 --tool-path "$tool_path" --source ./artifacts/first-prerelease --version 0.1.0-alpha.1
"$tool_path/pipel4" --help
"$tool_path/pipel4" --version
"$tool_path/pipel4" examples list
```

Record the exact package path, SHA-256, source SHA, SDK, validation/test results, nuspec and
notice inspection, isolated installation, and TCP smoke. If any check fails, stop. If source
changes before tagging, repeat Gate B's hosted CI check and all of Gate C; never rebuild or replace
an approved artifact without fresh review.

After the final artifact and recorded checksum are approved against the exact source SHA, create
an annotated tag on that commit and push **only** that tag to the verified `origin`. The commands
below require Git authentication, the approved SHA, and a clean checkout of that commit. Replace
the placeholder before running them. `git ls-remote` reports both the annotated tag object's SHA
and its peeled `^{}` target; compare the **peeled commit**, not the tag object's SHA.

```sh
set -euo pipefail
approved_sha="<APPROVED-SOURCE-SHA>"
release_tag="v0.1.0-alpha.1"
test -z "$(git status --porcelain)"
test "$(git rev-parse HEAD)" = "$approved_sha"
test "$(git branch --show-current)" = master
test -z "$(git tag --list "$release_tag")"
remote_before="$(git ls-remote --tags origin "refs/tags/${release_tag}" "refs/tags/${release_tag}^{}")"
test -z "$remote_before"
git tag -a "$release_tag" "$approved_sha" -m "PipeL4 0.1.0-alpha.1"
test "$(git rev-parse "refs/tags/${release_tag}^{commit}")" = "$approved_sha"
git push origin "refs/tags/${release_tag}"
remote_after="$(git ls-remote --tags origin "refs/tags/${release_tag}" "refs/tags/${release_tag}^{}")"
remote_commit="$(printf '%s\n' "$remote_after" | awk -v ref="refs/tags/${release_tag}^{}" '$2 == ref {print $1}')"
test "$remote_commit" = "$approved_sha"
```

Record the remote tag check with Gate C evidence. Stop on a conflicting local or remote tag, a
failed push, or a remote peeled-commit mismatch. Investigate the remote state after any ambiguous
push result; do not overwrite, delete, move, or force-push a published tag. If source or package
content must change after tagging, stop and explicitly review the release candidate, version,
checksum, and tag plan before any further publication step.

### Gate D — explicit authorization for NuGet publication

Present Gate C evidence, including the verified remote tag target and exact `.nupkg` SHA-256, to
the publication approver. Obtain explicit authorization for irreversible public nuget.org
submission, confirming the intended publishing
account owns or may claim `PipeL4`. Stop without authorization or if the ID/version is unavailable.
A package version already published to nuget.org cannot be overwritten; correction needs a new
version and new release review. See [NuGet's publishing guidance](https://learn.microsoft.com/en-us/nuget/nuget-org/publish-a-package)
and [push semantics](https://learn.microsoft.com/en-us/nuget/api/package-publish-resource).

### Gate E — publish on nuget.org and verify public installation

Sign in to the approved nuget.org publishing account. Use **Upload Package** in the nuget.org UI
to select the exact Gate C file, verify displayed PackageId and version, inspect the preview, and
submit only after rechecking its local SHA-256 against the approved value. This manual path keeps
an API key out of shell history and repository files. If the CLI is used instead, follow the
[official `dotnet nuget push` instructions](https://learn.microsoft.com/en-us/dotnet/core/tools/dotnet-nuget-push)
with explicit `https://api.nuget.org/v3/index.json` source and a secret API key scoped to the
required push permission. Do not put the key in a checked-in file, example, shell history, or
retained log. The SDK requested by `global.json` (`10.0.112`) predates documented `NUGET_API_KEY` environment-variable
support introduced with SDK `10.0.300`; do not assume that shortcut works. No credential or
publishing workflow is configured here.

#### Resolve uncertain upload results

If a CLI push times out, loses its connection, or reports an ambiguous result, **do not retry
immediately**. An apparently successful upload that is missing from public search also needs a
status check, not a retry. Sign in to the intended publishing account and check **Manage packages**
for the exact `PipeL4` / `0.1.0-alpha.1` package under Published or Unlisted Packages; inspect its
package page, validation messages, and nuget.org notification email. Check the exact version's
public package page and availability independently. Search results alone do not establish whether
an upload was accepted, because validation and indexing can leave an accepted package unlisted.

- **Upload confirmed not accepted:** Retry only after nuget.org explicitly confirms the request
  was not accepted and account and exact-version checks show no pending or published upload.
  Recheck the Gate C file's SHA-256 and submit that same approved file under Gate D's authorization.
- **Accepted, validation or indexing pending:** Wait and monitor the account/package status; do
  not submit again. Check [nuget.org service status](https://status.nuget.org/) and contact NuGet
  support if the published status remains unresolved beyond its documented guidance.
- **Successfully published:** Continue with public exact-version installation and verification.
- **Rejected or failed validation:** Record the reason and stop. If resolving it changes package
  content, metadata, or version, require a new release-candidate review of source, artifact,
  checksum, and the already published tag plan before continuing.
- **Status still unknown or a conflicting package/version exists:** Stop and investigate with
  nuget.org support or the package owner. Do not retry, use `--skip-duplicate`, or proceed to Gate F.

nuget.org rejects an existing PackageId/version rather than overwriting it. A duplicate response
is not proof that the reviewed file was accepted; verify ownership and package status before
continuing. See [NuGet's validation and indexing guidance](https://learn.microsoft.com/en-us/nuget/nuget-org/publish-a-package#package-validation-and-indexing)
and [`dotnet nuget push` options](https://learn.microsoft.com/en-us/dotnet/core/tools/dotnet-nuget-push).

Wait for nuget.org validation and indexing. Verify the public package page shows `PipeL4`
`0.1.0-alpha.1`, expected author, license, and README; inspect notices in the downloaded public
package. From a clean environment, install **that exact version** from the explicit public source,
then run CLI and TCP first-use
checks using the installed tool:

```sh
public_tool_path="$(mktemp -d)"
dotnet tool install PipeL4 --tool-path "$public_tool_path" --source https://api.nuget.org/v3/index.json --version 0.1.0-alpha.1
"$public_tool_path/pipel4" --help
"$public_tool_path/pipel4" --version
"$public_tool_path/pipel4" examples list
```

Repeat the [TCP echo and AsciiUppercase example](../src/PipeL4/README.md#local-tcp-example)
with this public installation. Stop if indexing, exact-version installation, CLI output, or TCP
behavior fails. An upload response alone is not a completed release.

### Gate F — publish the GitHub prerelease

After Gate E succeeds, confirm the existing `v0.1.0-alpha.1` remote tag still peels to Gate C's
approved source commit. Tag creation in Gate C and GitHub Release publication here are separate
steps. In GitHub **Releases**, draft a release using the existing tag, copy the reviewed
[release notes](first-prerelease-notes.md), mark **This is a pre-release**, and check the public
NuGet installation command. Resolve the notes' relative repository links against the final public
GitHub URL and inspect their rendered targets before publishing. If attaching a `.nupkg`, compare
its SHA-256 with Gate C's approved checksum immediately before upload. Publish the GitHub Release
only after these checks. GitHub does not publish the NuGet package, and nuget.org does not create
the GitHub Release. Check the published release page, prerelease marker, tag target, release notes,
attachment checksum if present, and links to the indexed NuGet package. Record the public GitHub Release and
NuGet package URLs. Stop and correct any discrepancy before declaring the release complete. See
[GitHub's release procedure](https://docs.github.com/en/repositories/releasing-projects-on-github/managing-releases-in-a-repository).
