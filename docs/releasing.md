# First prerelease: manual publication

This runbook is for the first public `PipeL4` .NET Tool package, version
`0.1.0-alpha.1`, and GitHub tag `v0.1.0-alpha.1`. The [release notes](first-prerelease-notes.md)
are the proposed GitHub Release body. All publication steps below are future actions; CI does not
publish a package, tag, or GitHub Release. Stop at a failed check, resolve it, and repeat checks
affected by the change. Record the evidence at each gate before proceeding.

The [CI workflow](../.github/workflows/ci.yml) runs on pushes to `master`, pull requests targeting
`master`, and manual dispatch. It restores, builds, tests, packs, and validates a local package on
Ubuntu 24.04. The separate [distribution validation workflow](../.github/workflows/distribution-validation.yml)
can upload a validated candidate with `source-revision.txt`; it also does not publish. The earlier
[local readiness report](release-readiness.md) covers a different historical package and cannot
substitute for validation of this final prerelease.

## Gate A — approved source is on master

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

## Gate B — public GitHub repository and hosted CI

Decide the final GitHub owner and repository name. The canonical URL is **not yet established**;
`<GITHUB-OWNER>` and `<GITHUB-REPOSITORY>` below are placeholders. Create a public repository under
that owner with **no** GitHub-generated README, license, `.gitignore`, or other initial commit.
With GitHub access configured and the empty remote confirmed, publish only approved `master`:

```sh
git remote add origin https://github.com/<GITHUB-OWNER>/<GITHUB-REPOSITORY>.git
git remote -v
git push -u origin master
```

If a remote exists, inspect it and use the approved URL; do not blindly add or replace it. Do not
use `git push --all`, `git push --mirror`, or a tag push at this gate. Verify the public branch list
and tags contain only intended refs. Confirm `master` is the GitHub default branch (set it in
repository Settings if needed). Inspect the public [README](../README.md), [MIT license](../LICENSE),
[repository notices](../THIRD_PARTY_NOTICES.md), and [package notices](../src/PipeL4/THIRD_PARTY_NOTICES.md).
Run or observe the first hosted **CI** workflow for the published `master` SHA. Check every step,
including package validation, regression checks, and package-backed tests. Record its run URL and
resolved runner image. If CI fails, stop, fix through review, and repeat for the new `master` SHA.

Evidence: approved owner/URL, public `master` history and files, default branch, intended ref
list, and passing hosted CI at the relevant SHA. See [GitHub's import instructions](https://docs.github.com/en/migrations/importing-source-code/using-the-command-line-to-import-source-code/adding-locally-hosted-code-to-github)
and [default-branch instructions](https://docs.github.com/en/repositories/configuring-branches-and-merges-in-your-repository/managing-branches-in-your-repository/changing-the-default-branch).

## Gate C — final metadata, source, artifact, and verified tag

### Publication prerequisites

Settle the final source before tagging or packing. Verify that the intended nuget.org account or
organization can publish PackageId `PipeL4` and will own the package. A previously observed 404
from its package index does not prove the ID is claimable. Confirm version `0.1.0-alpha.1`,
ToolCommandName `pipel4`, author Michael Krotenko, and MIT license in
[`PipeL4.fsproj`](../src/PipeL4/PipeL4.fsproj). Decide whether to add `PackageProjectUrl`,
`RepositoryUrl`, and `RepositoryType` now that the canonical URL is known. None is currently set.
If repository metadata is added, include both `RepositoryUrl` and `RepositoryType`; the package
validator requires the pair.
If any are added, commit the approved change, rerun hosted CI, then rebuild and revalidate the
package from that new source revision. Verify that [package notices](../src/PipeL4/THIRD_PARTY_NOTICES.md)
still cover the distributed F# and Microsoft assemblies; the [Paket notice](../THIRD_PARTY_NOTICES.md)
covers build tooling outside the package. Refresh the inventory if dependencies or payload change.

Freeze the approved `master` SHA after all release-affecting source and documentation edits. Plan
`v0.1.0-alpha.1` for that SHA, but create no tag until source is final and the artifact passes all
checks. Use a clean checkout at that exact commit and the SDK requested by `global.json`
(`10.0.112`; verify the selected SDK with `dotnet --version`). The commands assume a Unix-like
shell and fresh `artifacts/first-prerelease` directory. A CI-downloaded candidate may be used
instead only after matching its recorded SHA, contents, test results, and checksum to this gate.

```sh
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
sha256sum "$package_path"
```

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

## Gate D — explicit authorization for NuGet publication

Present Gate C evidence, including the verified remote tag target and exact `.nupkg` SHA-256, to
the publication approver. Obtain explicit authorization for irreversible public nuget.org
submission, confirming the intended publishing
account owns or may claim `PipeL4`. Stop without authorization or if the ID/version is unavailable.
A package version already published to nuget.org cannot be overwritten; correction needs a new
version and new release review. See [NuGet's publishing guidance](https://learn.microsoft.com/en-us/nuget/nuget-org/publish-a-package)
and [push semantics](https://learn.microsoft.com/en-us/nuget/api/package-publish-resource).

## Gate E — publish on nuget.org and verify public installation

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

### Resolve uncertain upload results

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

## Gate F — publish the GitHub prerelease

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
