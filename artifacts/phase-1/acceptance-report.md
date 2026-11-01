# Phase 1 Acceptance Report

Acceptance Decision: **ACCEPTED**

Phase 1 S3 + Parquet is accepted. ADR-0001 was explicitly approved on 2026-11-01 and its Decimal128 Own Vector representation is implemented and verified. All Phase 1 correctness, memory, failure, benchmark, interoperability, race, and architecture gates pass.

## Scope

Phase 1 establishes:

`S3 -> FileSetPlanner -> Parquet Metadata -> Pruning -> Range Planning -> Async I/O -> Direct Own Vector Decode -> Scan Pipeline`.

No Phase 2 join/aggregation/spill functionality is included in the Phase 1 implementation.

## Deliverables

- **P1-D1 S3 Parquet CLI query — PASS**: `cmd/phase1-parquet` exposes `describe`, `explain`, `analyze`, and `scan` over the real S3 VFS, SourceBinder, and RowGroupScanner paths.
- **P1-D2 Projection pushdown evidence — PASS**: projection planning selects only required ColumnChunks; scanner regression verifies unprojected ranges are not read and profile metrics remain available after scanner close.
- **P1-D3 RowGroup pruning evidence — PASS**: tri-state `ALWAYS_FALSE / MAYBE_TRUE / ALWAYS_TRUE`; only ALWAYS_FALSE skips I/O; regression covers prune-before-I/O.
- **P1-D4 FileSetPlanner demo — PASS**: literal/glob/recursive glob, prefix pruning, paginated list, Hive partitions, file limits, stable ordering/fingerprint.
- **P1-D5 SourceSnapshot demo — PASS**: immutable ScanSet snapshots carry ObjectIdentity and DatasetFingerprint; stable identities are used for conditional reads.
- **P1-D6 Source Introspection — PASS**: `InspectSource` and `DescribeSource` reuse the same SourceBinder; schema equality and metadata-cache reuse are regression tested.
- **P1-D7 Nested Parquet corpus — PASS**: deterministic corpus generation covers nested, dictionary, Decimal64/Decimal128 source files, timestamp, Hive, multi-file and union-by-name cases.
- **P1-D8 DuckDB/PyArrow/Polars/Spark interop corpus — PASS**: final GitHub Actions run `41259459358`, interop job `124709838587`, successfully ran the pinned canonical corpus and 4-producer × 4-reader matrix.
- **P1-D9 EXPLAIN metrics — PASS**: generic ScanProfile and Parquet aggregation report matched files, projected columns, pushed filters, rowgroups, planned/transferred/useful bytes, requests/pages/rows; metrics remain observable after resource cleanup.
- **P1-D10 S3 fault-injection suite — PASS**: `test/fault/phase1_s3_fault_test.go` covers 503 retry, timeout, cancel, short read, object changed, and credential failure with managed-memory cleanup assertions.

## Correctness Gate

- nested LIST — PASS.
- STRUCT — PASS for the Phase 1 non-repeated STRUCT contract with primitive direct children.
- LIST<STRUCT> — PASS for the Phase 1 standard three-level LIST contract with non-repeated STRUCT elements and primitive direct children.
- DECIMAL(1..18) — PASS for INT32/INT64/BYTE_ARRAY/FIXED_LEN_BYTE_ARRAY using checked Decimal64 storage.
- DECIMAL(19..38) — PASS using the ADR-0001 canonical `types.Int128` representation and 16-byte Own Vector PhysicalInt128 layout. BYTE_ARRAY/FIXED_LEN_BYTE_ARRAY decode uses big-endian two's-complement sign extension, declared-precision checks, explicit overflow rejection, dictionary copy, type-aware vector serialization, and controlled RowGroupScanner integration.
- timestamps — PASS for millis/micros/nanos and legacy INT96 decode paths; millis-to-micros overflow is explicitly rejected.
- dictionary — PASS, including Decimal64/Decimal128 copy paths.
- multi-file / Hive partition / union_by_name — PASS.

Overall Correctness Gate: **PASS**.

## Decimal128 Verification

ADR-0001 is **ACCEPTED** and implemented as specified:

- `types.Int128` uses low unsigned 64-bit and high signed 64-bit limbs with two's-complement semantics;
- Own Vector stores exactly 16 bytes/value, low limb then high limb, both little-endian;
- `SetInt128` / `Int128` support Flat, Constant, and Dictionary vector access;
- vector serialization preserves DECIMAL width/scale and distinguishes 8-byte Decimal64 from 16-byte Decimal128 payloads;
- Parquet controlled decode supports high-precision DECIMAL through BYTE_ARRAY and FIXED_LEN_BYTE_ARRAY, including positive/negative values and signed-128/declared-precision overflow checks;
- invalid high-precision INT32/INT64 physical combinations fail explicitly rather than truncating;
- a RowGroupScanner-level regression exercises exact range read -> DataPage -> ColumnCursor -> Decimal128 Own Vector decode and verifies metrics and managed-memory cleanup.

## Memory Gate

- no whole ColumnChunk materialization: PASS; ColumnCursor reads page header/body ranges incrementally.
- compressed/decode/read-ahead buffers are MemoryManager/BufferManager governed: PASS.
- Decimal128 remains fixed-width managed Own Vector storage: PASS.
- LIST/STRUCT reconstruction uses Own Vector children and managed vector buffers: PASS.
- success/failure/Decimal128 tests assert managed reservations return to zero: PASS.
- race detector over the full repository: PASS.

Overall Memory Gate: **PASS**.

## Failure Gate

Required scenarios all execute in the Phase 1 fault suite:

- 503 retry — PASS
- timeout — PASS
- short read — PASS
- cancellation — PASS
- object changed / precondition failure — PASS
- credential failure — PASS

Decimal precision/representation overflow and timestamp conversion overflow also fail explicitly without approximation.

Overall Failure Gate: **PASS**.

## Benchmark Gate

Final GitHub Actions run `41259459358`, verify job `124709511245`, executed the controlled 1024-row INT32 Parquet scanner benchmark three times with `-benchmem` after ordinary/race/architecture verification.

Observed final acceptance baseline on Go 1.22.12 / AMD EPYC 9V74:

- 41,342 ns/op, 99.08 MB/s, 40,088 B/op, 1,069 allocs/op;
- 41,497 ns/op, 98.71 MB/s, 40,088 B/op, 1,069 allocs/op;
- 41,206 ns/op, 99.40 MB/s, 40,088 B/op, 1,069 allocs/op.

Mean baseline: approximately 41,348 ns/op and 99.06 MB/s. No numeric Phase 1 performance SLO is invented; this is reproducible regression evidence.

Overall Benchmark Evidence: **PASS**.

## Interoperability Gate

Final GitHub Actions run `41259459358`, interop job `124709838587`, used the pinned Phase 1 matrix: Python 3.12, Java 17, PyArrow 26.0.0, DuckDB 2.0.0, Polars 2.1.0, and PySpark 4.3.0.

The canonical Decimal128/dictionary/nested/Hive corpus and all 16 producer-reader combinations pass. Exact corpus row-count evidence is retained in `artifacts/phase-1/interop-output.json`.

Overall Interoperability Gate: **PASS**.

## Repository-wide Validation

Final Phase 1 run `41259459358`, verify job `124709511245`:

- `go test ./...` — PASS
- `go run ./tools/architecture-check .` — PASS
- `go test -race ./...` — PASS
- controlled Parquet benchmark baseline — PASS

Final interop job `124709838587` — PASS.

Corresponding Phase 0 compatibility run `41259459399`, verify job `124709511595`:

- `go test ./...` — PASS
- `go run ./tools/architecture-check .` — PASS
- `go test -race ./...` — PASS

## Architecture Invariants

- I-01 preserved and completed for PhysicalInt128: Parquet decodes directly to Own Vector/DataChunk; no Arrow core ABI.
- I-04 preserved: Decimal128 uses the existing managed fixed-width Vector buffer; remote/decode/read-ahead buffers remain managed.
- I-05 preserved: range reads require exact-range semantics; no silent whole-file fallback.
- I-06 preserved: VFS remains format-agnostic.
- I-07 preserved: SourceSnapshot/ScanSet is immutable after planning.
- I-10 preserved by explicit buffer/cursor/scanner cleanup paths; Decimal128 tests assert managed scope usage returns to zero.
- I-11 preserved: introspection and query binding share SourceBinder/SchemaResolver.
- I-12 preserved: invalid Decimal physical representations, precision overflow, signed-128 overflow, timestamp conversion overflow, and ambiguous nested layouts return explicit errors rather than approximate/coerced results.

ADR-0001 is ACCEPTED. No other frozen architecture decision was changed.

## Acceptance Result

All Phase 1 deliverables and gates pass. Phase 1 is **ACCEPTED** and milestone `v0.1-parquet` acceptance criteria are satisfied. The machine phase graph may advance to Phase 2; this acceptance transition itself does not implement Phase 2 functionality.
