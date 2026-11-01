# Phase 1 Acceptance Report

Acceptance Decision: **BLOCKED — ADR-0001 ONLY**

Phase 1 is intentionally not marked ACCEPTED. All executable Phase 1 implementation, repository-wide verification, failure testing, and four-engine interoperability work is complete. The sole remaining blocker is the frozen-architecture decision required for DECIMAL(19..38): ADR-0001 remains PROPOSED, so the Decimal128 Own Vector representation cannot be implemented until explicitly approved or otherwise resolved.

## Scope

Phase 1 establishes:

`S3 -> FileSetPlanner -> Parquet Metadata -> Pruning -> Range Planning -> Async I/O -> Direct Own Vector Decode -> Scan Pipeline`.

No Phase 2 join/aggregation/spill functionality is included.

## Deliverables

- **P1-D1 S3 Parquet CLI query — PASS**: `cmd/phase1-parquet` exposes `describe`, `explain`, `analyze`, and `scan` over the real S3 VFS, SourceBinder, and RowGroupScanner paths; repository-wide tests pass.
- **P1-D2 Projection pushdown evidence — PASS**: projection planning selects only required ColumnChunks; scanner regression verifies unprojected ranges are not read and profile metrics preserve useful/transferred bytes after scanner close.
- **P1-D3 RowGroup pruning evidence — PASS**: tri-state `ALWAYS_FALSE / MAYBE_TRUE / ALWAYS_TRUE`; only ALWAYS_FALSE skips I/O; regression covers prune-before-I/O.
- **P1-D4 FileSetPlanner demo — PASS**: literal/glob/recursive glob, prefix pruning, paginated list, Hive partitions, file limits, stable ordering/fingerprint.
- **P1-D5 SourceSnapshot demo — PASS**: immutable ScanSet snapshots carry ObjectIdentity and DatasetFingerprint; stable identities are used for conditional reads.
- **P1-D6 Source Introspection — PASS**: `InspectSource` and `DescribeSource` reuse the same SourceBinder; schema equality and metadata-cache reuse are regression tested.
- **P1-D7 Nested Parquet corpus — PASS**: deterministic corpus generation covers nested, dictionary, Decimal64/Decimal128 source files, timestamp, Hive, multi-file and union-by-name cases. The corpus is materialized by the interoperability gate.
- **P1-D8 DuckDB/PyArrow/Polars/Spark interop corpus — PASS**: GitHub Actions run `41256775324`, job `124701834272`, successfully ran the canonical corpus and a 4-producer × 4-reader matrix. Exact results and engine versions are recorded in `artifacts/phase-1/interop-output.json`.
- **P1-D9 EXPLAIN metrics — PASS**: generic ScanProfile and Parquet aggregation report matched files, projected columns, pushed filters, rowgroups, planned/transferred/useful bytes, requests/pages/rows; metrics remain observable after resource cleanup.
- **P1-D10 S3 fault-injection suite — PASS**: `test/fault/phase1_s3_fault_test.go` covers 503 retry, timeout, cancel, short read, object changed, and credential failure with managed-memory cleanup assertions; the suite passes under repository-wide ordinary and race runs.

## Correctness Gate

- nested LIST — PASS.
- STRUCT — PASS for the Phase 1 non-repeated STRUCT contract with primitive direct children.
- LIST<STRUCT> — PASS for the Phase 1 standard three-level LIST contract with non-repeated STRUCT elements and primitive direct children.
- DECIMAL(1..18) — PASS for INT32/INT64/BYTE_ARRAY/FIXED_LEN_BYTE_ARRAY using checked Decimal64 storage.
- DECIMAL(19..38) — **BLOCKED by ADR-0001** because frozen logical types map these widths to PhysicalInt128 but Own Vector has no approved canonical Int128 ABI.
- timestamps — PASS for millis/micros/nanos and legacy INT96 decode paths.
- dictionary — PASS.
- multi-file / Hive partition / union_by_name — PASS.

Overall Correctness Gate: **BLOCKED only by DECIMAL(19..38) / ADR-0001**.

## Memory Gate

- no whole ColumnChunk materialization: PASS; ColumnCursor reads page header/body ranges incrementally.
- compressed/decode/read-ahead buffers are MemoryManager/BufferManager governed: PASS.
- LIST/STRUCT reconstruction uses Own Vector children and managed vector buffers: PASS.
- success/failure cleanup tests assert managed reservations return to zero: PASS.
- race detector over the full repository: PASS.

Overall Memory Gate: **PASS for implemented Phase 1 paths**.

## Failure Gate

Required scenarios all execute in the Phase 1 fault suite:

- 503 retry — PASS
- timeout — PASS
- short read — PASS
- cancellation — PASS
- object changed / precondition failure — PASS
- credential failure — PASS

Overall Failure Gate: **PASS**.

## Interoperability Gate

GitHub Actions run `41256775324` used Python 3.12.15, Java 17.0.21+9, PyArrow 26.0.0, DuckDB 2.0.0, Polars 2.1.0, and PySpark 4.3.0.

Canonical corpus row counts matched across all four readers:

- `decimal128.parquet`: 2 rows in every reader;
- `flat_dictionary.parquet`: 4 rows in every reader;
- `nested.parquet`: 4 rows in every reader;
- Hive corpus: 4 rows in every reader.

The producer grid also passed all 16 combinations: each of PyArrow, DuckDB, Polars, and Spark produced a 3-row Parquet dataset, and all four readers observed exactly 3 rows for every producer.

Overall Interoperability Gate: **PASS**.

## Repository-wide Validation

GitHub Actions run `41256775324`, verify job `124701500839`:

- `go test ./...` — PASS
- `go run ./tools/architecture-check .` — PASS
- `go test -race ./...` — PASS

The earlier no-runner infrastructure condition is resolved and is no longer an acceptance blocker.

## Architecture Invariants

- I-01 preserved: Parquet decodes directly to Own Vector/DataChunk; no Arrow core ABI.
- I-04 preserved: remote/decode/read-ahead buffers are managed.
- I-05 preserved: range reads require exact-range semantics; no silent whole-file fallback.
- I-06 preserved: VFS remains format-agnostic.
- I-07 preserved: SourceSnapshot/ScanSet is immutable after planning.
- I-10 preserved by explicit buffer/cursor/scanner cleanup paths and failure assertions; metrics are frozen before cleanup where post-close observability is required.
- I-11 preserved: introspection and query binding share SourceBinder/SchemaResolver.
- I-12 preserved: unsupported Decimal128 and ambiguous nested layouts return explicit errors rather than approximate/coerced results.

ADR-0001 is PROPOSED and has not been treated as approved.

## Remaining Acceptance Blocker

1. Explicitly approve or otherwise resolve ADR-0001.
2. If approved as proposed, implement the specified `types.Int128` / PhysicalInt128 Own Vector representation, vector accessors/serialization/copy semantics, Parquet Decimal128 decode and boundary tests.
3. Re-run the Phase 1 ordinary/race/architecture/interop gates with the Decimal128 implementation.
4. Only after those checks pass: change this decision to ACCEPTED, update machine state, and unlock Phase 2.

Milestone `v0.1-parquet` must not be declared complete while this report is BLOCKED.
