# Phase 6 DuckDB Compatibility Report

Date: 2026-11-02
Candidate commit: `fd737c3c6f45f47fe1ba1735d09658e6c1120bc1`
DuckDB reference: 2.0.0
Workflow run: `41430739607`

## Release verification

The Phase 6 gate pins DuckDB 2.0.0 and fails if the installed reference version differs.

The following passed on the release candidate:

```text
FSSQL_REQUIRE_DUCKDB=1 go test -count=1 ./test/compat
FSSQL_REQUIRE_DUCKDB=1 go test -race -count=1 ./test/compat
```

The compatibility package validates SQLLogic expected results, live DuckDB differential behavior, and the machine-readable deviation registry together. A frozen Stable SQL mismatch cannot be waived by classifying the behavior as an unsupported feature.

## Stable SQL result

- unresolved Stable DuckDB mismatches: 0
- unclassified Stable mismatches: 0
- known Stable semantic bugs accepted for release: 0

Phase 5 remediation regressions for BOOLEAN fallback composition, BIGINT comparison execution, INTEGER→BIGINT widening, and zero-physical-column filter cardinality remain covered by the repository tests and compatibility gate.

## Registered adapter deviations

`compatibility/deviations.yaml` contains three entries, all with `stable_sql_semantics: false`:

1. `DBSQL-001` — database/sql parameter binding is outside the frozen v1 API boundary and non-empty argument lists are explicitly rejected.
2. `DBSQL-002` — LIST/STRUCT remain lossless through native Chunk API and are not coerced into `database/sql` `driver.Value`.
3. `DBSQL-003` — DECIMAL/HUGEINT cross the `database/sql` adapter as exact base-10 text because the standard driver value model has no Int128/Decimal type.

These entries document compatibility-adapter boundaries and are not used to waive Stable SQL semantics.

## Result

Stable v1 SQL compatibility against DuckDB 2.0.0 is verified for the release candidate.
