# ADR-0006 — Phase 6 Stable Contract / Release Gate Reconciliation

Status: **PROPOSED**

Date: 2026-11-01

## Context

Phase 6 is the Feature Freeze / v1 Release phase. The controlling frozen facts are:

- AD-038: every feature is classified STABLE / EXPERIMENTAL / DEFERRED, and DuckDB compatibility applies to Stable.
- `architecture/feature-contract.md`: v1 Stable includes the SQL frontend surface, table functions, catalog-facing statements and introspection surface.
- AD-041 and `architecture/release-contract.md`: v1 release requires `Stable Feature Complete` and `SQL Compatibility Verified` before release.
- `phases/phase-6-release.md`: Phase 6 does not add new Stable features; it only handles bug, security, compatibility, regression, operations, docs and tests.

The current `main` repository does contain the lower-level execution/storage/format/export/server/resource/security implementations developed in Phases 0–5. However, the repository tree does not contain several modules required by the frozen Stable contract and module-boundary design:

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

Therefore Phase 6 cannot truthfully produce P6-D1 Stable Feature Matrix or P6-D2/P6-D3 SQL compatibility evidence with `PASS`, and cannot satisfy the frozen v1 Release Gate, without first resolving the mismatch between the frozen Stable contract and the implementation currently accepted as Phase 5.

## Problem

There are three materially different ways to resolve the mismatch, and choosing between them changes either phase semantics or the public Stable contract.

The implementation agent must not silently choose one.

## Option A — Preserve the frozen v1 Stable contract and reopen pre-release implementation

Treat Phase 5 `ACCEPTED` as insufficient for v1 release completeness, return the project to pre-Feature-Freeze remediation, and implement the missing already-declared Stable contract before re-entering Phase 6.

Properties:

- Does **not** add any new Stable feature entry.
- Does **not** narrow the frozen v1 public contract.
- Preserves AD-038 / AD-041 meaning and the normal meaning of Feature Freeze.
- Requires substantial implementation work: SQL frontend, binder/planner/catalog/public API wiring, SQLLogic/DuckDB differential verification and compatibility deviation registry.
- Requires project-state correction before Phase 6 can be accepted.

Recommended state while remediation is active:

```yaml
current_phase: 5
phase_5: IN_PROGRESS
phase_6: LOCKED
```

After all frozen Stable entries are implemented and Phase 5 is re-accepted, Phase 6 may start again as a true release-only phase.

## Option B — Narrow the v1 Stable feature contract to the implementation that exists

Change `architecture/feature-contract.md` and the corresponding canonical architecture facts so that v1 Stable only promises the currently implemented lower-level/public surfaces.

Properties:

- Makes Phase 6 smaller and immediately closer to releasable.
- Explicitly reduces the product contract.
- Requires an accepted ADR because AD-038/AD-041 and the frozen Feature Contract are changed.
- Existing documentation and compatibility expectations must be rewritten to match the reduced v1 scope.

This option must not be achieved by merely listing missing features as “known limitations”; a feature marked Stable cannot be silently absent.

## Option C — Allow Phase 6 to implement missing Stable contract entries as release blockers

Interpret “no new Stable feature” as “no new Stable **contract entry**”, while allowing Phase 6 to implement previously declared-but-missing Stable entries under compatibility/release-blocker work.

Properties:

- Keeps the frozen feature list unchanged.
- Avoids reopening Phase 5.
- Substantially changes the intended meaning of Feature Freeze and turns Phase 6 into a major feature-completion phase.
- Requires this ADR to explicitly authorize that interpretation before coding begins.

## Recommendation

**Recommend Option A.**

Reasoning:

1. It preserves the already-frozen public v1 contract instead of reducing scope after the fact.
2. It preserves AD-041 and `release-contract.md` semantics: Feature Freeze begins only after Stable Feature Complete.
3. It avoids using Phase 6 release verification to mask missing product implementation.
4. It keeps release artifacts trustworthy: P6-D1/D2/D3 can be evidence of a complete system rather than a list of unimplemented Stable promises.

## Required work if Option A is accepted

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

## Required work if Option B is accepted

Before Phase 6 implementation continues:

- define the exact reduced v1 Stable matrix;
- update the frozen canonical decision registry and Feature Contract through this ADR;
- update public/release documentation and compatibility scope;
- ensure every removed Stable entry is explicitly reclassified EXPERIMENTAL or DEFERRED rather than disappearing silently.

## Required work if Option C is accepted

Before coding continues:

- amend Phase 6/release semantics to state that implementation of already-declared Stable entries is allowed as a release blocker;
- keep the Stable feature list unchanged;
- require the same SQLLogic/DuckDB/deviation evidence as Option A before v1.0.

## Acceptance impact

Until this ADR is resolved:

```text
Phase 6 release implementation is BLOCKED at P6-D1/P6-D2/P6-D3.
v1.0 Stable MUST NOT be published.
```

No Phase 6 acceptance report may claim `Stable Feature Complete` or `SQL Compatibility Verified` solely from lower-level operator tests.

## Architecture invariants

This ADR proposal changes no invariant by itself. Any accepted resolution must continue to preserve I-01 through I-12, especially:

- I-03 Push Pipeline
- I-04 Managed Memory
- I-05 No Silent Range Fallback
- I-07 Query Snapshot
- I-08 Spill Ownership
- I-09 Semantic Centralization
- I-10 Lifecycle
- I-11 Introspection Consistency
- I-12 No Hidden Semantic Downgrade
