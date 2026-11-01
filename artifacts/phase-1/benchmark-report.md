# Phase 1 Benchmark Report

Status: **BLOCKED — repository-wide benchmark/verification runner unavailable**

## Purpose

Phase 1 performance evidence is expected to demonstrate that remote Parquet work is reduced by projection/pruning and that the controlled reader does not introduce whole-file/whole-ColumnChunk work.

## Implemented Evidence

- Projection planning selects only requested ColumnChunks.
- RowGroup pruning skips only `ALWAYS_FALSE` groups.
- Footer reads use the mandatory exact sequence: last 8 bytes, then exact metadata range.
- RowGroup scanner reports planned, transferred, and useful bytes separately.
- `EXPLAIN ANALYZE` CLI aggregates matched files, projection, rowgroup accounting, bytes, requests, pages, and rows.
- RangePlanner supports sorting/coalescing without changing logical sub-range identity.
- RemoteClientManager bounds client reuse and avoids client creation per request.

## Measurement Status

No repository-wide benchmark numbers are claimed in this report. The current GitHub Actions environment creates the Phase 1 job but assigns no runner; the job terminates before checkout with no executable steps or logs. The connected VM cannot clone the private repository directly, so it cannot provide a trustworthy full-module replacement run.

Focused development tests were used while implementing individual units, but they are not a substitute for the Phase 1 repository-wide benchmark baseline.

## Required Before Acceptance

Run a working checkout with at least:

```text
go test ./...
go test -race ./...
go run ./tools/architecture-check .
```

and collect representative Parquet scans that record:

- full scan versus projection scan transferred/useful bytes;
- unpruned versus rowgroup-pruned transferred/useful bytes;
- request/page counts;
- rows produced;
- managed-memory peak for the same workload.

Benchmark evidence must use a fixed corpus/object identity and record environment/tool versions. Until that evidence exists, the Phase 1 performance gate remains pending rather than passed.
