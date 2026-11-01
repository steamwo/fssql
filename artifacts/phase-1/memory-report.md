# Phase 1 Memory Report

Status: **PASS FOR IMPLEMENTED PHASE 1 PATHS**

## Memory Ownership

Phase 1 remote/Parquet buffers follow the frozen MemoryManager/BufferManager path:

- S3 exact-range result buffers are allocated through BufferManager and charged to the query scope.
- Parquet compressed page bodies use the Parquet compressed-buffer tag.
- decompressed page workspaces use the Parquet decode tag.
- read-ahead buffers use the read-ahead tag.
- Own Vector payloads and string heaps remain managed by the existing Vector/BufferManager implementation.

## Working-Set Boundaries

The controlled reader does not materialize a full ColumnChunk. `ColumnCursor` probes a page header, fetches the exact page body, decodes incrementally into Own Vector output, and releases page-local buffers before advancing. LIST/STRUCT/LIST<STRUCT> reconstruction composes Own Vector children rather than converting rows through `interface{}` or Arrow.

File discovery is paginated and bounded. RemoteClientManager is bounded. Async I/O workers/queues are bounded. MetadataCache is bounded LRU and caches only stable ObjectIdentity entries.

## Cleanup Evidence

Tests assert scope usage returns to zero after:

- exact S3 range success;
- timeout;
- cancellation;
- short read;
- object-changed/precondition failure;
- credential failure;
- ColumnCursor close;
- scalar RowGroupScanner close;
- LIST reconstruction;
- STRUCT reconstruction;
- LIST<STRUCT> reconstruction.

`RowGroupScanner.Close` freezes final scan/profile counters before releasing readers, so observability is retained without extending buffer lifetimes.

## Repository-wide Validation

GitHub Actions run `41256775324`, verify job `124701500839`:

- `go test ./...` — PASS
- `go run ./tools/architecture-check .` — PASS
- `go test -race ./...` — PASS

The Phase 1 fault suite is included in the ordinary/race repository run and validates failure-path reservation cleanup.

## Gate Result

The implemented Phase 1 memory model satisfies the frozen requirements used by this phase:

- engine-managed large buffers;
- incremental page decode rather than whole-ColumnChunk materialization;
- bounded discovery/client/async/cache structures;
- terminal cleanup returning managed reservations to zero in tested success/failure paths.

A standalone throughput/peak-memory benchmark is tracked separately in `benchmark-report.md`; no unsupported numeric peak is invented here.

Overall Memory Gate: **PASS** for implemented Phase 1 paths. Phase 1 as a whole remains BLOCKED only because DECIMAL(19..38) requires ADR-0001 and its subsequent implementation/verification.
