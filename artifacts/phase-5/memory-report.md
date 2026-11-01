# Phase 5 Memory Report

Date: 2026-11-01
Status: **PASS**

## Acceptance evidence

Workflow run `41318791929` completed the complete repository ordinary and race suites plus the Phase 5 cancellation/stability gates.

Phase 5 memory acceptance is based on managed ownership and bounded-resource assertions. The acceptance gate did not sample process RSS or emit numeric Go heap/GC telemetry; no such values are fabricated in this report.

## Managed ownership introduced or exercised in Phase 5

- Query result DataChunks remain allocated through the Query `MemoryScope` / Managed Buffer path.
- ResultCursor has a fixed chunk capacity and now bounds the combined queued + consumer-held ownership.
- Resource Scheduler admission memory is represented by explicit Query Lease accounting.
- FTP/SFTP use bounded session pools; endpoint clients are scoped and versioned rather than retained without bound.
- Async I/O, temporary storage and S3 multipart ownership remain covered by their existing lifecycle managers.

## Terminal ownership assertions

The final accepted ResultCursor lifecycle requires:

```text
queued result chunks = 0
consumer-held result chunks = 0
Query MemoryScope reservations = 0
admission Lease = 0
```

before a successful/failed/canceled Query terminal state can be published.

The Phase 5 D8 cancellation gate additionally verifies lifecycle cleanup across:

```text
active Query/server ownership
async I/O cancellation
temporary-storage ownership
SFTP session leases
S3 multipart cleanup
```

and repeats the race-sensitive packages under the race detector.

## Long-running stability evidence

`TestPhase5LongRunningStability` executes 256 Query lifecycles per run, mixing successful and canceled queries. Every 32-query batch asserts:

```text
MemoryManager.Used() = 0
Resource Scheduler GlobalUsed = zero
QueryRegistry retained queries = 0
server runtime ownership = 0
```

The acceptance script runs the stability test repeatedly in ordinary mode and once under the race detector, then repeats FTP/WebDAV/SFTP/S3 lifecycle packages. The final acceptance run reported:

```text
P5-D15 long-running stability gate: PASS
```

## Bounded structures

- ResultCursor: fixed `CursorCapacityChunks`; queued + consumer-held chunks share the same capacity budget.
- ResultCursor consumer ownership: fixed arrays, not an unbounded map/list.
- QueryService registry/runtime retention: bounded by `MaxRetainedQueries`.
- Query execution goroutines: allowed only when Resource Scheduler concurrency has a finite bound.
- FTP/SFTP sessions: bounded pools.
- HTTP control request bodies: explicit maximum size.

## Result

Phase 5 satisfies its managed-memory/ownership gate. No Query terminal path is permitted to hide a consumer-held Managed DataChunk, admission Lease, async request, temp ownership, connection lease or multipart ownership.

Process-level RSS, Go heap live/allocated and GC numeric baselines are not recorded by the Phase 5 acceptance command and remain an operations/release measurement item rather than an invented acceptance result.
