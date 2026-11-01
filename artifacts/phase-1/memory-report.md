# Phase 1 Memory Report

Status: **IMPLEMENTED / FULL-GATE EXECUTION PENDING**

## Memory Ownership

Phase 1 remote/Parquet buffers follow the frozen MemoryManager/BufferManager path:

- S3 exact-range result buffers are allocated through BufferManager and charged to the query scope.
- Parquet compressed page bodies use the Parquet compressed-buffer tag.
- decompressed page workspaces use the Parquet decode tag.
- read-ahead buffers use the read-ahead tag.
- Own Vector payloads and string heaps remain managed by the existing Vector/BufferManager implementation.

## Working-Set Boundaries

The controlled reader does not intentionally materialize a full ColumnChunk. `ColumnCursor` probes a page header, fetches the exact page body, decodes into an output Vector, and releases page-local buffers before advancing. LIST/STRUCT/LIST<STRUCT> reconstruction composes Own Vector children rather than converting rows through `interface{}` or Arrow.

File discovery is paginated and bounded. RemoteClientManager is bounded. Async I/O workers/queues are bounded. MetadataCache is bounded LRU and caches only stable ObjectIdentity entries.

## Cleanup Evidence in Tests

Focused tests assert scope usage returns to zero after:

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

## Known Pending Evidence

The latest Phase 1 branch cannot yet receive a trustworthy repository-wide memory/race run because the GitHub Actions job is not assigned a runner and never executes checkout/test steps. The connected VM cannot clone the private repository as a replacement.

No numeric managed-memory peak is claimed for the final Phase 1 corpus until a working full checkout executes the corpus and records the peak under a fixed memory limit.

## Acceptance Requirement

Before Phase 1 can be ACCEPTED, run the complete corpus with:

- full repository unit tests;
- race detector;
- memory limits low enough to exercise reservation failure paths;
- at least one S3/Parquet scan large enough to span multiple pages and row groups;
- verification that terminal query/scope usage returns to zero.

The memory architecture is implemented, but the final Memory Gate remains pending execution evidence.
