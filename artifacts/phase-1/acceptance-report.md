# Phase 1 Acceptance Report

Acceptance Decision: **BLOCKED**

Phase 1 is intentionally not marked ACCEPTED. The implementation is substantially complete, but three acceptance blockers remain: ADR-0001 / DECIMAL128, four-engine interoperability execution, and a working full-repository CI runner for unit/race/architecture gates.

## Scope

Phase 1 establishes:

`S3 -> FileSetPlanner -> Parquet Metadata -> Pruning -> Range Planning -> Async I/O -> Direct Own Vector Decode -> Scan Pipeline`.

No Phase 2 join/aggregation/spill functionality is included.

## Deliverables

- **P1-D1 S3 Parquet CLI query — CODE COMPLETE / VERIFICATION PENDING**: `cmd/phase1-parquet` provides `describe`, `explain`, `analyze`, and `scan` modes over the real S3 VFS, SourceBinder, and RowGroupScanner paths.
- **P1-D2 Projection pushdown evidence — CODE COMPLETE**: projection planning selects only required ColumnChunks; scanner tests assert unprojected ranges are not read; useful/transferred bytes are separated.
- **P1-D3 RowGroup pruning evidence — CODE COMPLETE**: tri-state `ALWAYS_FALSE / MAYBE_TRUE / ALWAYS_TRUE`; only ALWAYS_FALSE skips I/O; scanner regression covers prune-before-I/O.
- **P1-D4 FileSetPlanner demo — CODE COMPLETE**: literal/glob/recursive glob, prefix pruning, paginated list, Hive partitions, file limits, stable ordering/fingerprint.
- **P1-D5 SourceSnapshot demo — CODE COMPLETE**: immutable ScanSet snapshots carry ObjectIdentity and DatasetFingerprint; stable identities are used for conditional reads.
- **P1-D6 Source Introspection — CODE COMPLETE**: `InspectSource` and `DescribeSource` reuse the same SourceBinder; schema equality and metadata-cache reuse are regression tested.
- **P1-D7 Nested Parquet corpus — DEFINITION COMPLETE / MATERIALIZATION BLOCKED**: standard LIST, STRUCT, LIST<STRUCT>, dictionary, Decimal64, timestamp, Hive, multi-file and union-by-name cases are represented by tests plus `test/corpus/phase1/manifest.json` and deterministic `generate.py`. Binary corpus generation is blocked in the current environment by missing PyArrow.
- **P1-D8 DuckDB/PyArrow/Polars/Spark interop corpus — BLOCKED BY ENVIRONMENT**: reproducible runner exists at `test/interop/phase1/run.py`; the current VM has none of the four required Python engines installed, so no interoperability PASS is claimed.
- **P1-D9 EXPLAIN metrics — CODE COMPLETE / VERIFICATION PENDING**: generic ScanProfile and Parquet aggregation report matched files, projected columns, pushed filters, rowgroups, planned/transferred/useful bytes, requests/pages/rows; CLI `analyze` executes the scan and emits runtime evidence.
- **P1-D10 S3 fault-injection suite — CODE COMPLETE / FULL-RUN PENDING**: `test/fault/phase1_s3_fault_test.go` covers 503 retry, timeout, cancel, short read, object changed, and credential failure with managed-memory cleanup assertions.

## Correctness Gate

- nested LIST — implemented and regression tested in focused tests.
- STRUCT — implemented for non-repeated STRUCT with primitive children.
- LIST<STRUCT> — implemented for standard three-level LIST with non-repeated STRUCT element and primitive children.
- DECIMAL(1..18) — implemented for INT32/INT64/BYTE_ARRAY/FIXED_LEN_BYTE_ARRAY into Decimal64 storage.
- DECIMAL(19..38) — **BLOCKED by ADR-0001** because frozen logical types map these widths to PhysicalInt128 but Own Vector currently has no canonical Int128 ABI.
- timestamps — millis/micros/nanos and legacy INT96 value paths implemented; focused tests added.
- dictionary — implemented and regression tested.
- multi-file / Hive partition / union_by_name — planner and schema-resolution paths implemented and regression tested.

Overall Correctness Gate: **BLOCKED** pending Decimal128 decision/implementation and full repository verification.

## Memory Gate

- no whole ColumnChunk materialization: implemented; ColumnCursor reads page header/body ranges incrementally.
- compressed/decode/read-ahead buffers are MemoryManager/BufferManager governed.
- LIST/STRUCT reconstruction uses Own Vector children and managed vector buffers.
- S3 fault tests assert managed reservations return to zero on failure.

Overall Memory Gate: **PENDING FULL-REPOSITORY TEST/RACE EXECUTION**.

## Failure Gate

Required scenarios are represented in the Phase 1 fault suite:

- 503 retry
- timeout
- short read
- cancellation
- object changed / precondition failure
- credential failure

Overall Failure Gate: **PENDING FULL-REPOSITORY EXECUTION**.

## Architecture Invariants

- I-01 preserved: Parquet decodes directly to Own Vector/DataChunk; no Arrow core ABI.
- I-04 preserved: large remote/decode/read-ahead buffers are managed.
- I-05 preserved: range reads require 206 exact-range semantics; no silent whole-file fallback.
- I-06 preserved: VFS remains format-agnostic.
- I-07 preserved: SourceSnapshot/ScanSet is immutable after planning.
- I-10 preserved by explicit buffer/cursor/scanner cleanup paths and failure assertions.
- I-11 preserved: introspection and query binding share SourceBinder/SchemaResolver.
- I-12 preserved: unsupported Decimal128 and ambiguous nested layouts return explicit errors rather than approximate/coerced results.

ADR-0001 is PROPOSED and has not been treated as approved.

## Validation Status

Focused implementation units and several isolated/race checks were run during development. However the repository-wide Phase 1 workflow cannot currently execute: GitHub Actions creates a job with no runner and `steps=[]`, so checkout/tests never start and no job log exists. The connected environment also cannot clone the private repository directly.

The four-engine interop runner was syntax checked and then executed as a dependency probe; it exits 2 and explicitly reports PyArrow, DuckDB, Polars, and PySpark missing. This is recorded as BLOCKED, not PASS.

## Remaining Acceptance Blockers

1. Approve or otherwise resolve ADR-0001, then implement and verify DECIMAL(19..38) according to the approved Own Vector representation.
2. Run `test/corpus/phase1/generate.py` and `test/interop/phase1/run.py` in an environment with PyArrow, DuckDB, Polars, and PySpark, recording successful evidence.
3. Run repository-wide `go test ./...`, `go test -race ./...`, and `go run ./tools/architecture-check .` on a functioning runner; fix any surfaced defects.
4. Re-run S3 fault suite and Phase 1 CLI/corpus tests in the full repository environment.
5. Only after all blockers pass: change this decision to ACCEPTED, update machine state, and unlock Phase 2.

Milestone `v0.1-parquet` must not be declared complete while this report is BLOCKED.
