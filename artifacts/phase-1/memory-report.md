# Phase 1 Memory Report

Status: **PASS**

## Memory Ownership

Phase 1 remote/Parquet buffers follow the frozen MemoryManager/BufferManager path:

- S3 exact-range result buffers are allocated through BufferManager and charged to the query scope.
- Parquet compressed page bodies use the Parquet compressed-buffer tag.
- decompressed page workspaces use the Parquet decode tag.
- read-ahead buffers use the read-ahead tag.
- Own Vector payloads and string heaps remain managed by the existing Vector/BufferManager implementation.
- Decimal128 uses the fixed-width PhysicalInt128 Own Vector payload: exactly 16 managed bytes/value. No per-value `big.Int` or unmanaged heap object is introduced.

## Working-Set Boundaries

The controlled reader does not materialize a full ColumnChunk. `ColumnCursor` probes a page header, fetches the exact page body, decodes incrementally into Own Vector output, and releases page-local buffers before advancing. LIST/STRUCT/LIST<STRUCT> reconstruction composes Own Vector children rather than converting rows through `interface{}` or Arrow.

Decimal128 decode follows the same incremental page path. Big-endian Parquet two's-complement bytes are converted directly into the fixed-width Int128 Vector representation; dictionary copy and serialization remain fixed-width/vector-managed.

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
- LIST<STRUCT> reconstruction;
- Int128 Flat/Constant/Dictionary Vector tests;
- Decimal64/Decimal128 serialization round-trip;
- Decimal128 dictionary-copy tests;
- Decimal128 page decode and precision-failure tests;
- controlled RowGroupScanner Decimal128 decode.

`RowGroupScanner.Close` freezes final scan/profile counters before releasing readers, so observability is retained without extending buffer lifetimes.

## Repository-wide Validation

Final GitHub Actions run `41259459358`, verify job `124709511245`:

- `go test ./...` — PASS
- `go run ./tools/architecture-check .` — PASS
- `go test -race ./...` — PASS
- controlled Parquet benchmark — PASS

The Phase 1 fault suite is included in the ordinary/race repository run and validates failure-path reservation cleanup. Corresponding Phase 0 compatibility run `41259459399`, verify job `124709511595`, also passes ordinary/race/architecture verification.

## Gate Result

The Phase 1 memory model satisfies the frozen requirements:

- engine-managed large buffers;
- incremental page decode rather than whole-ColumnChunk materialization;
- fixed-width managed Decimal128 Own Vector storage;
- bounded discovery/client/async/cache structures;
- terminal cleanup returning managed reservations to zero in tested success/failure paths.

A standalone throughput baseline is tracked separately in `benchmark-report.md`; no unsupported numeric peak-memory claim is invented here.

Overall Memory Gate: **PASS**.
