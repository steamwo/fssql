# Phase 1 Failure Report

Status: **PASS**

## Required Failure Matrix

| Failure | Expected behavior | Phase 1 evidence |
| --- | --- | --- |
| S3 503 | bounded retry, then success/final error | `test/fault/phase1_s3_fault_test.go` retries 503 and verifies eventual success |
| timeout | request fails, managed buffer released | phase-level timeout test checks non-success and scope usage returns to zero |
| short read | explicit `vfs.ErrShortRead`, no partial-success result | phase-level short-read test verifies error and cleanup |
| cancel | context cancellation propagates, no reservation leak | phase-level cancel test expects `context.Canceled` and zero scope usage |
| object changed | conditional read/precondition failure becomes `vfs.ErrObjectChanged` | phase-level 412 test verifies explicit object-change error and cleanup |
| credential failure | explicit `vfs.ErrCredential` | phase-level 403 test verifies credential classification and cleanup |

## Additional Failure Behavior

- S3 range reads reject whole-object `200 OK` responses where a `206 Partial Content` exact range is required.
- Footer discovery rejects invalid magic/metadata lengths rather than falling back to whole-file reads.
- SourceSnapshot carries stable object identity into conditional range reads.
- repeated listing continuation tokens are rejected to avoid infinite listing loops.
- FileSetPlanner enforces a matched-file upper bound.
- Async I/O workers/queues are bounded and cancellation/Close paths drain active request accounting.
- MetadataCache bypasses unstable identities to avoid stale-schema reuse.
- unsupported/ambiguous nested Parquet layouts return explicit errors.
- Decimal128 rejects signed-128 overflow, declared-precision overflow, and incompatible high-precision INT32/INT64 physical encodings; no truncation, string coercion, or approximate result is produced.
- timestamp millis-to-micros conversion rejects int64 overflow.

## Execution Evidence

Final GitHub Actions run `41259459358`, verify job `124709511245`, completed successfully:

- `go test ./...` — PASS, including `test/fault/...`, S3 driver tests, Parquet Decimal128 tests, and RowGroupScanner integration.
- `go run ./tools/architecture-check .` — PASS.
- `go test -race ./...` — PASS.
- controlled Parquet benchmark — PASS.

The dependent pinned interop job `124709838587` also completed successfully. Corresponding Phase 0 compatibility run `41259459399`, verify job `124709511595`, passed ordinary/race/architecture verification.

## Gate Result

All required Phase 1 fault scenarios execute with explicit error classification and managed-resource cleanup. Decimal/timestamp boundary failures are explicit and non-degrading. No silent range fallback or partial-success path is accepted.

Overall Failure Gate: **PASS**.
