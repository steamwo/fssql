# Phase 1 — S3 + Parquet Main Path

## Goal

建立：
```text
S3
→ FileSetPlanner
→ Parquet Metadata
→ Pruning
→ Range Planning
→ Async I/O
→ Direct Vector Decode
→ Pipeline
```

## Required Implementation

- S3 VFS
- RemoteClientManager
- FileSetPlanner / Glob / Hive partitions
- ObjectIdentity / SourceSnapshot
- AsyncIOManager
- RangePlanner / Coalescer / ReadAhead
- Controlled Parquet Reader
- Internal Parquet metadata
- RowGroup pruning
- projection pushdown
- direct-to-Vector decode
- nested LIST/STRUCT reconstruction
- MetadataCache
- Source Introspection
- DESCRIBE
- EXPLAIN / EXPLAIN ANALYZE baseline

## Deliverables

- P1-D1 S3 Parquet CLI query
- P1-D2 Projection pushdown evidence
- P1-D3 RowGroup pruning evidence
- P1-D4 FileSetPlanner demo
- P1-D5 SourceSnapshot demo
- P1-D6 Source Introspection
- P1-D7 Nested Parquet corpus
- P1-D8 DuckDB/PyArrow/Polars/Spark interop corpus
- P1-D9 EXPLAIN metrics
- P1-D10 S3 fault-injection suite

## Mandatory Behavior

Footer：
```text
last 8 bytes
→ metadata length
→ exact metadata range
```

Tail prefetch only optimization.

Pruning：
```text
ALWAYS_FALSE
MAYBE_TRUE
ALWAYS_TRUE
```
Only ALWAYS_FALSE can skip.

## Profile Evidence

至少：
- matched files
- projected columns
- pushed filters
- rowgroups total/pruned/scanned
- planned bytes
- transferred bytes
- useful bytes

## Gate

Correctness：
- nested
- DECIMAL
- timestamps
- dictionary
- multi-file
- hive partition
- union_by_name

Memory：
- no whole ColumnChunk materialization
- incremental Page decode
- ReadAhead memory-governed

Failure：
- 503
- timeout
- short read
- cancel
- object changed
- credential failure

## Acceptance

`artifacts/phase-1/acceptance-report.md`

milestone：
```text
v0.1-parquet
```
