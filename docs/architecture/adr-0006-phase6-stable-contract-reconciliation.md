# ADR-0006 — Phase 6 Stable Contract / Release Gate Reconciliation

Status: **ACCEPTED**

Date: 2026-11-01
Accepted: 2026-11-02

## Context

Phase 6 is the Feature Freeze / v1 Release phase. The controlling frozen facts are:

- AD-038: every feature is classified STABLE / EXPERIMENTAL / DEFERRED, and DuckDB compatibility applies to Stable.
- `architecture/feature-contract.md`: v1 Stable includes the SQL frontend surface, table functions, catalog-facing statements and introspection surface.
- AD-041 and `architecture/release-contract.md`: v1 release requires `Stable Feature Complete` and `SQL Compatibility Verified` before release.
- `phases/phase-6-release.md`: Phase 6 does not add new Stable features; it only handles bug, security, compatibility, regression, operations, docs and tests.

The current `main` repository contains the lower-level execution/storage/format/export/server/resource/security implementations developed in Phases 0–5. However, the repository tree does not contain several modules required by the frozen Stable contract and module-boundary design:

```text
internal/sql/lexer
internal/sql/parser
internal/sql/ast
internal/sql/binder
internal/function
internal/catalog
internal/planner
internal/optimizer
internal/session
pkg/gosql
```

The repository also currently has no release-level SQL compatibility framework/evidence matching the testing contract:

```text
SQLLogic suite
DuckDB differential SQL suite
compatibility/deviations.yaml
```

`cmd/phase2-compat` directly exercises aggregate/distinct/sort operator semantics. It is useful lower-level compatibility evidence, but it is not an end-to-end SQL parser/binder/planner/execution compatibility verifier.

Therefore Phase 6 cannot truthfully produce P6-D1 Stable Feature Matrix or P6-D2/P6-D3 SQL compatibility evidence with `PASS`, and cannot satisfy the frozen v1 Release Gate, without first resolving the mismatch between the frozen Stable contract and the implementation previously accepted as Phase 5.

## Problem

There are three materially different ways to resolve the mismatch, and choosing between them changes either phase semantics or the public Stable contract. The implementation agent must not silently choose one.

## Decision

**Option A is accepted.**

The frozen v1 Stable Feature Contract remains unchanged. The project returns to pre-Feature-Freeze remediation and reopens Phase 5 until every already-declared Stable v1 entry needed for the release gate is implemented and verified.

While remediation is active:

```yaml
current_phase: 5
phase_5: IN_PROGRESS
phase_6: LOCKED
```

The remediation is not authorization to add any new Stable feature. It is limited to completing the already-frozen v1 contract and the verification infrastructure required by AD-036/AD-041. Phase 6 may be entered again only after the remediated Phase 5 passes its full acceptance gate and is marked `ACCEPTED` again.

## Option A — Preserve the frozen v1 Stable contract and reopen pre-release implementation

Treat the previous Phase 5 `ACCEPTED` state as insufficient for v1 release completeness, return the project to pre-Feature-Freeze remediation, and implement the missing already-declared Stable contract before re-entering Phase 6.

Properties:

- Does **not** add any new Stable feature entry.
- Does **not** narrow the frozen v1 public contract.
- Preserves AD-038 / AD-041 meaning and the normal meaning of Feature Freeze.
- Requires implementation work for the missing SQL frontend, binder/planner/catalog/public API wiring, SQLLogic/DuckDB differential verification and compatibility deviation registry.
- Requires project-state correction before Phase 6 can be accepted.

## Option B — Narrow the v1 Stable feature contract to the implementation that exists

Rejected for this release. It would reduce the already-frozen public contract and require reclassification of existing Stable entries.

## Option C — Allow Phase 6 to implement missing Stable contract entries as release blockers

Rejected for this release. It would turn Feature Freeze into a major implementation phase and weaken AD-041 semantics.

## Required remediation work

The remediation must be dependency-ordered and remain within the existing frozen architecture:

1. SQL lexer/parser/AST using AD-004.
2. FunctionCatalog and type/binder integration using AD-012/AD-020/AD-025/AD-026.
3. MemoryCatalog + SQLiteCatalog using AD-010.
4. Logical/physical planning and optimizer contracts using AD-003/AD-013/AD-024.
5. Session/statement/query wiring preserving AD-014/AD-035.
6. Stable SQL statements/table functions from `feature-contract.md`, wired only through existing VFS/format/export/security/resource boundaries.
7. Native Chunk Go API and database/sql compatibility boundary from AD-022; external interchange remains outside core ABI.
8. Source introspection must reuse the actual binder path per AD-042/I-11.
9. SQLLogic and DuckDB differential harness per AD-036/testing contract.
10. `compatibility/deviations.yaml`, with every mismatch classified as bug / intentional deviation / unsupported feature.
11. Re-run the complete Phase 5 gates and acceptance before unlocking Phase 6 again.

No new Stable SQL syntax or capability outside `feature-contract.md` is authorized by this remediation.

## Acceptance impact

Until remediation is complete:

```text
Phase 6 remains LOCKED.
P6-D1/P6-D2/P6-D3 remain blocked.
v1.0 Stable MUST NOT be published.
```

No Phase 6 acceptance report may claim `Stable Feature Complete` or `SQL Compatibility Verified` solely from lower-level operator tests.

## Architecture invariants

The accepted decision changes no execution invariant. Remediation must continue to preserve I-01 through I-12, especially:

- I-03 Push Pipeline
- I-04 Managed Memory
- I-05 No Silent Range Fallback
- I-07 Query Snapshot
- I-08 Spill Ownership
- I-09 Semantic Centralization
- I-10 Lifecycle
- I-11 Introspection Consistency
- I-12 No Hidden Semantic Downgrade
