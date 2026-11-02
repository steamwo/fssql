# Phase 6 Stable Feature Matrix

Date: 2026-11-02
Reference contract: `docs/architecture/feature-contract.md`
Candidate commit: `fd737c3c6f45f47fe1ba1735d09658e6c1120bc1`

## Stable SQL

| Stable capability | Release evidence | Status |
| --- | --- | --- |
| SELECT / FROM / WHERE | SQLLogic + DuckDB differential + repository tests | PASS |
| GROUP BY / HAVING | planner/runtime aggregate tests + compatibility gate | PASS |
| ORDER BY / LIMIT / OFFSET | planner/runtime sort/limit + compatibility gate | PASS |
| DISTINCT | runtime DISTINCT + forced-spill equality | PASS |
| INNER / LEFT / RIGHT / FULL / CROSS / SEMI / ANTI JOIN | planner/runtime join + forced-spill equality | PASS |
| COUNT / SUM / AVG / MIN / MAX | aggregate execution + forced-spill equality | PASS |
| non-recursive CTE | binder/planner tests + compatibility gate | PASS |
| limited non-correlated subquery | scalar subquery tests + repository gate | PASS |
| CREATE EXTERNAL TABLE | Stable statement grammar/binder + real TableFunction bind path | PASS |
| CREATE VIEW | Stable statement grammar/binder + view planning resolver | PASS |
| CREATE SECRET | Stable grammar/binder + scoped Secret ACL/security tests | PASS |
| COPY TO | Stable statement path + Export/VFS commit/recovery tests | PASS |
| DESCRIBE | bind-only result path + shared schema resolution | PASS |
| information_schema.columns | session implementation/tests + repository gate | PASS |

## Stable table functions

| Function | Status |
| --- | --- |
| read_parquet | PASS |
| read_csv | PASS |
| read_json | PASS |
| read_ndjson | PASS |
| glob | PASS |

All are exercised through the real source-resolution / bind / runtime paths; Phase 6 did not add another inference or execution path.

## Stable introspection

| Capability | Status |
| --- | --- |
| DESCRIBE table/query | PASS |
| Go InspectSource | PASS |
| Server `POST /v1/sources/inspect` | PASS |
| parquet_schema | PASS |
| parquet_file_metadata | PASS |

Source introspection continues to reuse FileSetPlanner / TableFunction.Bind / SchemaResolver behavior rather than a separate inference implementation.

## Out of Stable v1

Experimental and Deferred capabilities remain outside the v1 Stable compatibility promise. Phase 6 did not promote gRPC, recursive CTE, window functions, correlated-decorrelation expansion, Iceberg/Delta, distributed execution, remote spill, or other future features.

## Result

The frozen v1 Stable Feature Contract is complete on the accepted release candidate, with no Stable feature added during Feature Freeze.
