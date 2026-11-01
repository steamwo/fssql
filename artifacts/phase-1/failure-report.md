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
- DECIMAL widths requiring the unresolved Decimal128 Own Vector representation return an explicit architecture-requirement error; no truncation/coercion is attempted.

## Execution Evidence

GitHub Actions run `41256775324`, verify job `124701500839` completed successfully:

- `go test ./...` — PASS, including `test/fault/...`, S3 driver tests, and Parquet tests.
- `go run ./tools/architecture-check .` — PASS.
- `go test -race ./...` — PASS.

The earlier GitHub runner provisioning problem is resolved and no longer affects the Failure Gate.

## Gate Result

All required Phase 1 fault scenarios execute with explicit error classification and managed-resource cleanup. No silent range fallback or partial-success path is accepted.

Overall Failure Gate: **PASS**.

Phase 1 remains BLOCKED only by ADR-0001 / DECIMAL(19..38), not by failure-path verification.
