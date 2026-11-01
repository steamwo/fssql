# Phase 1 Failure Report

Status: **CODE COMPLETE / FULL-RUN PENDING**

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
- Async I/O workers/queues are bounded and cancellation/Close paths are designed to drain active request accounting.
- MetadataCache bypasses unstable identities to avoid stale-schema reuse.
- unsupported/ambiguous nested Parquet layouts return explicit errors.
- DECIMAL widths requiring an undefined Decimal128 Own Vector representation return an explicit architecture-requirement error; no truncation/coercion is attempted.

## Execution Status

The fault suite is committed but the latest full-repository execution has not run because the GitHub Actions job receives no runner and terminates before executing steps. The connected VM cannot clone the private repository to run the complete module independently.

Individual S3/failure behaviors were developed against focused tests during implementation, but this report does not promote those focused checks to a repository-wide PASS.

## Acceptance Requirement

Before Phase 1 acceptance, run:

```text
go test ./test/fault/... ./internal/storage/remote/s3/... ./internal/format/parquet/...
go test -race ./...
```

followed by the full repository gate. All required failures must leave managed reservations and active asynchronous requests at zero. Until that run exists, the Failure Gate remains pending.
