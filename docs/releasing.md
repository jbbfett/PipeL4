# Release procedure

PipeL4 CI builds and validates a local .NET Tool package, then uploads the validated `.nupkg` with
the source revision that produced it. It does not publish packages or create GitHub Releases.

## Create a release candidate

Before starting, confirm the candidate version has maintainer approval and has not already been
published. The metadata and ownership questions listed under [publication
prerequisites](#publication-prerequisites) must be resolved before any publication approval; they do
not prevent local or CI validation of a release candidate.

Update the single `<Version>` value in `src/PipeL4/PipeL4.fsproj`. MSBuild derives the NuGet package
version and assembly informational version from it; do not add a second version constant. Use the
same version for the release tag, with a `v` prefix (for example, `v<package-version>`), and make
sure the tag points at the source revision used for the candidate.

From the repository root in a clean checkout with the .NET 10 SDK installed, run:

```sh
dotnet tool restore
dotnet clean src/PipeL4/PipeL4.fsproj --configuration Release -p:PaketDisableCliTest=true -p:BuildInParallel=false
dotnet restore PipeL4.sln -p:PaketDisableCliTest=true -p:BuildInParallel=false
dotnet build PipeL4.sln --configuration Release --no-restore -p:PaketDisableCliTest=true
mkdir -p artifacts/release-candidate
dotnet pack src/PipeL4/PipeL4.fsproj --configuration Release --no-restore -p:PaketDisableCliTest=true -p:BuildInParallel=false --output ./artifacts/release-candidate
dotnet fsi scripts/test_validate_package.fsx ./artifacts/release-candidate
dotnet fsi scripts/validate_package.fsx ./artifacts/release-candidate
```

The output directory must contain no old `.nupkg` files before packing. The validator requires
exactly one package and fails on mismatched identity/version, invalid tool metadata, a missing
README or runtime file, or unexpected test and temporary content. The `dotnet pack` output must not
contain the missing-package-readme warning `NU5046`.

Run the full suite against the package just built. This includes the existing installed-tool test,
which installs from the package's local directory into an isolated temporary tool path, checks
`--help` and `--version`, compiles `.fsx` processors, and exercises bidirectional TCP forwarding:

```sh
package_path="$(find ./artifacts/release-candidate -maxdepth 1 -type f -name 'PipeL4.*.nupkg' -print -quit)"
test -n "$package_path"
PIPEL4_PACKAGE_PATH="$package_path" dotnet test --solution PipeL4.sln --configuration Release --no-restore -p:PaketDisableCliTest=true -p:BuildInParallel=false
git rev-parse HEAD > ./artifacts/release-candidate/source-revision.txt
```

The environment variable makes the installed-tool test consume that exact candidate rather than
packing a second package. The `.nupkg` filename and embedded nuspec must agree on the version, and
the installed command output must match it. To run this flow in CI, use the **Distribution
validation** workflow's manual trigger. The uploaded artifact contains the validated package and a
`source-revision.txt` file. Its artifact name is `PipeL4-<package-version>-release-candidate-<commit-sha>`;
retrieve it instead of rebuilding the package for publication.

After downloading the artifact and fetching the intended tag, verify the filename, tag version, and
source revision together:

```sh
package_path="$(find ./downloaded-artifact -maxdepth 1 -type f -name 'PipeL4.*.nupkg' -print -quit)"
test -n "$package_path"
package_name="${package_path##*/}"
package_version="${package_name#PipeL4.}"
package_version="${package_version%.nupkg}"
release_tag="v${package_version}"
test "$(git rev-parse "refs/tags/${release_tag}^{commit}")" = "$(cat ./downloaded-artifact/source-revision.txt)"
```

## Publication prerequisites

Before any publication approval, maintainers must:

- Confirm PackageId `PipeL4` is available and controlled by the intended NuGet.org owner. This
  repository cannot verify availability or ownership.
- Establish the correct author attribution. The current package metadata says `PipeL4`, inherited
  from the existing package identity; the repository does not identify the responsible person or
  organization.
- Select and add the project's license only after its terms are confirmed. No license file or
  expression currently exists in the repository.
- Confirm the canonical project and repository URLs. No Git remote or hosted URL is configured in
  this checkout, so neither URL is embedded in the package.
- Confirm the approved package version and release tag refer to the same source revision as the
  downloaded candidate. Check the embedded nuspec version, versioned `.nupkg` filename, and
  installed `pipel4 --version` before publication.
- Obtain explicit publication approval and configure the selected NuGet.org publishing identity.
  NuGet Trusted Publishing with GitHub Actions OIDC is a suitable future option; no credentials,
  OIDC policy, or publishing workflow are configured here.

Once those checks and approval are complete, publish the already validated file from the downloaded
artifact without rebuilding it:

```sh
package_path="$(find ./downloaded-artifact -maxdepth 1 -type f -name 'PipeL4.*.nupkg' -print -quit)"
test -n "$package_path"
dotnet nuget push "$package_path" --source https://api.nuget.org/v3/index.json --api-key "$NUGET_API_KEY"
```

After publication, verify installation from NuGet.org and run `pipel4 --help` and
`pipel4 --version`. For a prerelease, include `--prerelease` in the install command. Do not treat
artifact creation or CI success as publication approval.
