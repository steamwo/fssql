# Phase 6 Acceptance Report

Date: 2026-11-02
Milestone: `v1.0 Stable`
Status: **ACCEPTED — Feature Freeze release gate passed and v1.0 published**

## Acceptance candidate

- Phase 6 acceptance candidate: `fd737c3c6f45f47fe1ba1735d09658e6c1120bc1`
- Branch acceptance workflow run: `41430739607`
- Branch acceptance workflow job: `125228379133`
- Accepted-state main verification run: `41433012771`
- Final self-verifying publication commit: `09c88549a29db652b576cb9dd030fec1fb95be70`
- Final self-verifying publication run: `41435118170`
- Published tag: `v1.0`
- Published release: `v1.0 Stable`
- Runner: Ubuntu 24.04.5 / linux amd64
- Go: 1.22.12
- DuckDB reference: 2.0.0
- Interop matrix: PyArrow 26.0.0, DuckDB 2.0.0, Polars 2.1.0, PySpark 4.3.0

The complete release gate finished with:

```text
Phase 6 Feature Freeze / v1 release gate: PASS
```

The final publication workflow re-ran repository correctness, race detection, vet, architecture freeze, Stable SQL compatibility, forced-spill equality, fault/leak suites, Phase 5 lifecycle/security/config/stability suites, Parquet interoperability, resource-envelope workloads, public/release documentation checks, release performance baselines, and publisher preflight before publishing `v1.0`.

## Deliverables

| Deliverable | Evidence | Status |
| --- | --- | --- |
| P6-D1 Stable Feature Matrix | `artifacts/phase-6/stable-feature-matrix.md` | PASS |
| P6-D2 DuckDB Compatibility Report | `artifacts/phase-6/duckdb-compatibility-report.md` | PASS |
| P6-D3 Compatibility Deviation Registry | `compatibility/deviations.yaml`, machine validated by `test/compat` | PASS |
| P6-D4 Parquet Interop Report | `artifacts/phase-6/parquet-interop-report.md` | PASS |
| P6-D5 Full Failure Matrix | `artifacts/phase-6/failure-report.md` | PASS |
| P6-D6 Performance Baseline | `artifacts/phase-6/benchmark-report.md` | PASS |
| P6-D7 Resource Envelope | `artifacts/phase-6/memory-report.md` | PASS |
| P6-D8 Security Review | `artifacts/phase-6/security-review.md` | PASS |
| P6-D9 Operational Documentation | `docs/operations.md` | PASS |
| P6-D10 Public API Documentation | `docs/public-api.md` | PASS |
| P6-D11 Release Artifacts | `scripts/phase6-release.sh`, `scripts/phase6-publish-v1.sh`, `artifacts/phase-6/release-manifest.md`, published `v1.0` tag/release | PASS |

## Release contract

The release candidate satisfies the frozen AD-041 / release-contract conditions:

- Stable Feature Complete
- SQL Compatibility Verified
- Correctness Verified
- Forced-Spill Verified
- Interop Verified
- Failure Matrix Passed
- Security Review Passed
- Performance Baseline Established
- Resource Envelope Documented
- Operational Documentation Complete

No Stable feature was added during Phase 6. The only engine-adjacent fix made while closing the gate was a release-evidence output-contract fix: the Phase 1 interop runner stdout is JSON-only, with a regression test, so Phase 6 can consume it deterministically.

## Architecture freeze

`go run ./tools/architecture-check .` passed in the acceptance runs and in the final publication run. Phase 6 introduced no ADR change and no bypass of Push Pipeline, own Vector/DataChunk ABI, MemoryManager ownership, unified VFS/I/O, SourceSnapshot, TemporaryStorageManager spill, or Query lifecycle ownership.

## Publication result

The self-verifying publication run `41435118170` completed both `verify` and `publish` jobs successfully. GitHub tag `v1.0` resolves exactly to `09c88549a29db652b576cb9dd030fec1fb95be70`, and the non-draft, non-prerelease GitHub Release is named `v1.0 Stable`.

The one-time GitHub Actions verification/publication workflows were removed after use. Reusable release verification remains available through `scripts/phase6-release.sh` and publication preflight/idempotence through `scripts/phase6-publish-v1.sh`.
