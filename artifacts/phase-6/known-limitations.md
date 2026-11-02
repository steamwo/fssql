# Phase 6 Known Limitations

Date: 2026-11-02
Release target: `v1.0 Stable`

## Stable SQL

No unresolved Stable SQL mismatch against the pinned DuckDB 2.0.0 release reference is accepted for v1.0.

## database/sql compatibility boundary

The machine-readable registry `compatibility/deviations.yaml` records three non-Stable-SQL adapter limitations:

- parameter binding through `database/sql` is not part of the frozen v1 API contract and non-empty argument lists are explicitly rejected;
- LIST/STRUCT values require the native Chunk API for lossless access;
- DECIMAL/HUGEINT cross `database/sql` as exact decimal text because `driver.Value` has no standard Int128/Decimal representation.

These do not waive Stable SQL semantics.

## Feature-scope limitations

The following remain outside Stable v1 by the frozen feature contract and are not release blockers:

- window functions
- recursive CTE
- full correlated-subquery decorrelation
- UPDATE / DELETE / MERGE
- native persistent table storage
- Iceberg / Delta
- ORC / Avro
- PostgreSQL wire
- gRPC as a Stable v1 server transport
- remote spill
- distributed execution
- binary extension ABI

Experimental capabilities remain Experimental and are not included in the Stable compatibility promise.

## Performance / resource interpretation

Phase 6 benchmark and resource numbers are machine-local baselines from the recorded CI runner. They are regression evidence, not portable absolute SLOs. Engine memory accounting remains distinct from process RSS.

## Release-process note

The Phase 6 branch is accepted only after its final acceptance commit re-passes the release workflow. The `v1.0` GitHub release/tag is published from the merged mainline commit, not from an unmerged feature branch.
