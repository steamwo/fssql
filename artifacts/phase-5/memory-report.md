# Phase 5 Memory Report

Date: 2026-11-02
Status: **PASS — remediated acceptance**

## Acceptance evidence

Acceptance candidate `114788a4ef6097008f4a8437ced71a821f7af260`, workflow run `41424206444`, completed the full repository ordinary and race suites plus the Phase 5 public API, compatibility, cancellation and stability gates.

Phase 5 memory acceptance is based on managed ownership and bounded-resource assertions. The acceptance gate does not sample process RSS or emit numeric Go heap/GC telemetry; no such values are fabricated here.

## Managed ownership exercised by the remediated Stable path

- Native/public query results still originate as Managed `DataChunk`/Vector ownership; the public API wraps rather than replaces the engine result lifecycle.
- ResultCursor has a fixed chunk capacity and bounds queued + consumer-held ownership together.
- Public native chunk consumers release borrowed engine ownership explicitly; `database/sql` rows release chunks while advancing/closing.
- Stable ORDER BY, DISTINCT, GROUP/aggregate and JOIN production composition uses query-scoped TemporaryStorageManager spill ownership.
- Stable COPY uses existing Export/VFS writer ownership, staging manifests and reconciliation rather than a second unmanaged write lifecycle.
- Resource Scheduler admission memory remains explicit Query Lease accounting.
- FTP/SFTP use bounded session pools; S3 multipart/single-object publish remains explicit lifecycle ownership.

## Terminal ownership assertions

A terminal Query may not hide live result, spill, admission or remote-write ownership. The accepted lifecycle requires the applicable counters/resources to be drained before successful terminal publication:

```text
queued result chunks = 0
consumer-held result chunks = 0
Query MemoryScope reservations = 0
query TemporaryStorage ownership = 0
admission Lease = 0
remote connection/session leases = 0
multipart/staging ownership = 0
```

The re-acceptance reran the full race suite and the dedicated P5-D8 cancellation gate. The latter passed for Query/server lifecycle, async I/O cancellation, temporary-storage ownership, SFTP leases and S3 multipart cleanup.

## Long-running stability evidence

`phase5-stability.sh` passed on the accepted candidate. The repeated server/query lifecycle gate continues to assert zero MemoryManager usage, zero Resource Scheduler usage, zero retained QueryRegistry/runtime ownership and correct cancellation cleanup. FTP/WebDAV/SFTP/S3 repeated lifecycle tests and the architecture boundary also passed.

The acceptance log reported:

```text
P5-D8 cancellation leak gate: PASS
P5-D15 long-running stability gate: PASS
```

## Bounded structures

- ResultCursor: fixed `CursorCapacityChunks`; queued + consumer-held chunks share one capacity budget.
- ResultCursor consumer ownership: fixed arrays, not an unbounded map/list.
- QueryService registry/runtime retention: bounded by `MaxRetainedQueries`.
- Query execution goroutines: admitted only under finite Resource Scheduler concurrency.
- Temporary storage: engine-level manager with query-scoped quota/cleanup ownership.
- FTP/SFTP sessions and endpoint clients: bounded/scoped pools.
- HTTP control request bodies and public SQL text: explicit maximum size.

## Result

Phase 5 satisfies its managed-memory/ownership gate after ADR-0006 remediation. Public API and Stable SQL composition do not introduce a second memory, spill, result or COPY lifecycle.

Process-level RSS, Go heap live/allocated and GC numeric baselines remain release/operations measurements because the acceptance command does not collect them.
