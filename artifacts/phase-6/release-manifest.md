# Phase 6 Release Manifest

Date: 2026-11-02
Release: `v1.0 Stable`
Phase: 6 — Feature Freeze / v1 Release

## Accepted release inputs

- Phase 5 status: ACCEPTED
- Phase 6 release-gate candidate: `fd737c3c6f45f47fe1ba1735d09658e6c1120bc1`
- Branch acceptance workflow run: `41430739607`
- Branch acceptance workflow job: `125228379133`
- Merged-main verification run: `41433012771`
- Release script: `scripts/phase6-release.sh`
- Verification workflow: historical only; removed after acceptance per repository workflow-cleanup rule
- DuckDB reference: 2.0.0

## Required acceptance evidence

- `artifacts/phase-6/acceptance-report.md`
- `artifacts/phase-6/benchmark-report.md`
- `artifacts/phase-6/memory-report.md`
- `artifacts/phase-6/failure-report.md`
- `artifacts/phase-6/known-limitations.md`

## Phase 6 deliverable evidence

- Stable feature matrix: `artifacts/phase-6/stable-feature-matrix.md`
- DuckDB compatibility: `artifacts/phase-6/duckdb-compatibility-report.md`
- deviation registry: `compatibility/deviations.yaml`
- Parquet interop: `artifacts/phase-6/parquet-interop-report.md`
- failure matrix: `artifacts/phase-6/failure-report.md`
- performance baseline: `artifacts/phase-6/benchmark-report.md`
- resource envelope: `artifacts/phase-6/memory-report.md`
- security review: `artifacts/phase-6/security-review.md`
- operations: `docs/operations.md`
- public API: `docs/public-api.md`

## Publication rule

The GitHub tag/release `v1.0` must point at the merged mainline commit containing the accepted Phase 6 state and all evidence above. Do not publish the tag from an unmerged working branch.

No Stable feature may be added between this acceptance and publication. Any release-blocking bug/security/compatibility fix requires the complete Phase 6 gate to pass again before publication.
