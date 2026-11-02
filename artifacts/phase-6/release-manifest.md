# Phase 6 Release Manifest

Date: 2026-11-02
Release: `v1.0 Stable`
Phase: 6 — Feature Freeze / v1 Release
Status: **PUBLISHED**

## Accepted release inputs

- Phase 5 status: ACCEPTED
- Phase 6 status: ACCEPTED
- Phase 6 acceptance candidate: `fd737c3c6f45f47fe1ba1735d09658e6c1120bc1`
- Branch acceptance workflow run: `41430739607`
- Accepted-state main verification run: `41433012771`
- Final self-verifying publication commit: `09c88549a29db652b576cb9dd030fec1fb95be70`
- Final self-verifying publication run: `41435118170`
- Published tag: `v1.0`
- Release title: `v1.0 Stable`
- Release script: `scripts/phase6-release.sh`
- Publication script: `scripts/phase6-publish-v1.sh`
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

## Publication result

`v1.0` was published only after the complete Phase 6 gate passed on main. The tag resolves exactly to `09c88549a29db652b576cb9dd030fec1fb95be70`; publication run `41435118170` completed both verification and publication successfully.

The one-time GitHub Actions verifier/publisher workflows were removed after publication in accordance with the repository workflow-cleanup rule. The reusable shell gates remain in the repository.

No Stable feature may be added to the published `v1.0` contract. Any subsequent release-blocking bug, security, compatibility, or regression fix must follow the Feature Freeze rules and re-run the complete release gate before a new release is published.
