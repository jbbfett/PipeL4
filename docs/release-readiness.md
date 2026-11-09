# Release readiness report

## Final local assessment

**READY FOR PRERELEASE PREPARATION** for the first experimental Linux .NET Tool release. The verified candidate passed local package validation, two complete package-backed suites, isolated global and local installation, and an installed-tool TCP smoke. F1 and F3 are resolved for this candidate. No remaining finding blocks this local technical decision. Public publication requires the separate release work below and explicit approval.

The assessment covers Ubuntu 24.04 x64 primarily, .NET 10, TCP byte-stream forwarding, trusted in-process F# .fsx processors, and .NET Tool distribution. Windows, macOS, TLS, UDP, additional proxy features, plugin sandboxing, new runtime APIs, performance optimization, and Docker publication are outside this assessment. The current [README](../README.md), [architecture](architecture.md), and [backpressure contract](backpressure.md) describe the implemented scope and its limits.

## Candidate and evidence

| Item | Verified local candidate |
| --- | --- |
| Host and date | Ubuntu 24.04.5 LTS x64; 2026-11-08 |
| .NET | SDK 10.0.114; runtime 10.0.14. global.json requests SDK 10.0.112 and Microsoft.Testing.Platform. |
| Package source revision | 4b5e593d5e2c38b7cda9e081ccef791aeccea845 |
| Package | PipeL4.0.1.0-preview.1.nupkg; PackageId PipeL4; version 0.1.0-preview.1; command pipel4; target net10.0 |
| SHA-256 | e93186bc53a35294456e522e52c7fddd90e63a59ddc628d7934038b8818e9b1e |

The hash matches the ignored local package at artifacts/release-candidate/. Its source-revision.txt
records the reported source revision above; this is local provenance, not a hosted CI attestation.
Revision `4b5e593d5e2c38b7cda9e081ccef791aeccea845` is the source revision recorded for this locally
verified pre-publication package. It is historical artifact provenance and is not expected to be
reachable from a master-only public clone. This report may be updated independently of the package;
documentation edits after the recorded source revision are not part of that package. The package
was not rebuilt for the installation checks. The final prerelease package will be rebuilt and
verified separately against its actual source revision.

| Stage | Status and evidence boundary |
| --- | --- |
| RA-01 inventory and initial gate | Completed. Its package and scope inventory established the baseline; its failing full-suite result is historical and was superseded after F1 was fixed. |
| RA-02 release build and pack | Passed as reported. The local pack.log records package creation without NU5046. The retained build.log is an unsuccessful sandbox attempt, **not** evidence of the reported successful build with zero errors and two existing FS3511 warnings. |
| RA-02 package validator and exit-code regression | Passed as reported, including 7/7 exit-code checks. Separate output logs were not retained. The local package metadata and hash remain inspectable. |
| RA-02 package-backed suites | Two passes, 455/455 each, with zero failures or skips. Ignored local full-suite-1.log and full-suite-2.log retain both summaries; their use of the identified package is reported by the verification execution. |
| RA-03.1 isolated global/local installation | Passed as reported: disposable profiles, isolated local feed, global PATH discovery, local manifest resolution, matching version, and operation outside the checkout. No separate installation transcript was retained. |
| RA-03.2 installed-tool TCP first use | Passed as reported: bundled example extraction and .fsx loading; client input Hello, PipeL4! became HELLO, PIPEL4! at the backend, with the reverse response unchanged; client half-close reached backend EOF before its response; both directions completed normally, then SIGINT exited 0 with cleanup. No separate smoke transcript was retained. |

The RA-02 logs are ignored local files under artifacts/release-verification-2026-11-08/, not repository or hosted CI artifacts. No GitHub repository, hosted workflow run, resolved runner image, public-feed installation, package publication, tag, or GitHub Release was verified. The [distribution workflow](../.github/workflows/distribution-validation.yml) defines a local-package validation path but does not publish.

## Findings and accepted limitations

| Finding | Current status and consequence | Blocks local readiness? | Required before public publication? | Next task |
| --- | --- | --- | --- | --- |
| F1 — listener readiness | Resolved for this candidate. The readiness clock now starts after script loading; RA-02 reported regression review, and both subsequent full suites passed. Earlier timeouts are historical. | No | No further F1 work indicated | Retain coverage in the final artifact's verification. |
| F2 — publication prerequisites | Open. Local validation cannot establish PackageId ownership, author, license, canonical URLs, publishing identity, approved version/source, or publication authority. | No | Yes | Resolve [publication prerequisites](releasing.md#publication-prerequisites) and approve the exact release version and source. |
| F3 — global/local installation | Resolved for this Linux candidate by RA-03. Public-feed installation remains untested until publication. | No | No prepublication public-feed test possible | Validate installation from the public feed after approved publication. |
| F4 — hosted CI reproducibility | Deferred. The workflow uses ubuntu-latest; no hosted run or resolved image is verified. | No | Yes for the planned GitHub release path | Establish the repository, run the Linux workflow, and record or pin the resolved Ubuntu image before publication. |
| F5 — packaged README | Open, nonblocking. Its first-use guidance is minimal; the repository README and CLI provide fuller instructions. | No | No | Consider improving package onboarding in a later documentation task. |
| F6 — Docker verification | Open, nonblocking. Docker build/run was not reverified and Docker publication is outside this .NET Tool release. | No | No | Verify only if Docker distribution is pursued. |

The accepted P3 forcedTerminationRequested exit-provenance race can label a child that exits just before Kill as force-terminated. It is test-harness diagnostic debt, does not change the observed passing results, and does not block local readiness or public publication; address it in a focused test-harness task if exit provenance becomes material. Trusted plugins, IP-literal endpoints, .NET 10 dependence, and the documented capacity/progress boundaries are accepted experimental constraints, not release blockers. Historical [benchmark results](../benchmarks/PipeL4.Benchmarks/README.md) were not rerun for this candidate.

## Release handoff

For the planned first prerelease stage, finalize local Git history before the first push; establish the GitHub repository and remote; verify the Linux GitHub Actions workflow and runner image; settle package ownership, author/license/URL metadata, and publishing identity; select and approve the actual version; then rebuild and verify the final artifact against its **exact** source revision. Publication, a tag or GitHub Release, and post-publication public-feed installation require a separate, explicitly approved release step. The existing candidate's local pass does not authorize publication or substitute for verification of a changed final artifact.
