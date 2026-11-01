# ADR-0003 — Stable Export Segment Identity for File-Level Resume

Status: PROPOSED

## Context

Phase 4 requires all of the following at the same time:

- `ExportCoordinator + Manifest + WriterPool + Committer`;
- file-level resume;
- incomplete file redo;
- `Kill / Restart / Resume`;
- no duplicate completed files;
- no missing files;
- SingleFile / MultiFile / Partitioned export;
- bounded writer count and managed memory.

The current execution contract exposes query output as concurrent `DataChunk` values:

```text
Source.Next(ctx) -> DataChunk
Pipeline workers
-> Sink.Consume(ctx, DataChunk)
```

`DataChunk` carries no stable output segment identity. Scheduler `Task` also has no stable task/segment identifier. Under parallel execution, chunk arrival order is therefore not a durable property of the query result.

The Phase 4 routing implementation intentionally requires a caller-supplied stable `segmentID` for MultiFile/Partitioned output rather than manufacturing one from chunk arrival order. The current execution layer has no contract capable of supplying such an ID for an arbitrary query, especially after joins, aggregates, sorts, or other operators that can change chunk boundaries and worker scheduling.

A naive implementation such as:

```text
first chunk  -> part-000001
second chunk -> part-000002
...
```

is not restart-safe. After a process restart, a different parallel schedule can assign different rows to the same ordinal file. Preserving the previous `COMPLETE part-000001` and skipping the new `part-000001` would then cause duplicate or missing rows, violating the Phase 4 Failure Gate.

This is an architecture-level gap. It cannot be repaired inside `DatasetWriter` without defining a durable mapping from logical query output rows to export files.

## Proposed Decision

Introduce an export-specific **Stable Export Segment Plan**. It is separate from generic Pipeline task identity and is persisted in the Export Manifest before output files become COMPLETE.

### 1. Persisted SegmentPlan

A resumable MultiFile/Partitioned export records at least:

```text
SegmentStrategy = ROW_HASH_V1
HashVersion
BucketCount
PartitionColumns
SchemaFingerprint
```

The SegmentPlan is part of the export job identity. Resume MUST reject a manifest if the query/schema/segment-plan identity differs from the restarted job.

SingleFile does not require row-hash segmentation because its only file is either COMPLETE or redone in full.

### 2. Stable row-to-segment mapping

For MultiFile:

```text
segment = HashCanonicalRow(row, HashVersion) % BucketCount
```

For Partitioned:

```text
partition = CanonicalPartitionTuple(row)
segment   = HashCanonicalRow(row, HashVersion) % BucketCount
file id   = partition + segment
```

The mapping MUST depend only on bound logical values and the persisted SegmentPlan, never on:

- worker ID;
- goroutine ID;
- chunk arrival order;
- runtime chunk boundaries;
- retry attempt number.

Hash collisions are harmless because the hash is used only for deterministic bucket routing, not for row deduplication.

The row hash MUST reuse the shared core hash semantics governed by AD-033 and preserve the existing property:

```text
Equals(a,b) => Hash(a)==Hash(b)
```

Nested LIST/STRUCT support must use canonical logical-value hashing rather than format-specific bytes.

### 3. Resume semantics

On restart:

```text
load Manifest
-> validate QueryFingerprint
-> validate SourceSnapshot
-> validate SegmentPlan
-> rerun query against the same SourceSnapshot
-> route every row using the same stable segment mapping
```

For a `COMPLETE` segment file, routed rows are discarded from the new attempt because that complete segment already contains the full logical bucket.

For `PLANNED` / `WRITING` / `FAILED` segment files, the segment is rebuilt from the rerun and the old incomplete remote object is discarded/replaced according to the destination capability contract.

This preserves the required v1 resume granularity:

```text
completed file -> preserve
incomplete file -> redo
```

without relying on deterministic execution scheduling.

### 4. Bounded local spool for remote streaming destinations

A stable segment can receive rows at arbitrary times during the query. S3/SFTP streaming writers cannot in general be closed and later reopened for append, and Partitioned export may have unbounded partition cardinality.

Therefore resumable MultiFile/Partitioned export requires a **bounded local export spool** before final remote file encoding/upload:

```text
Query DataChunk
-> stable segment router
-> bounded local segment spool
-> bounded WriterPool
-> Format Writer
-> Compression
-> VFS remote staging file
```

The local spool is query-temporary data governed by `TemporaryStorageManager` quotas and lifecycle. It is **not** remote export staging and must remain separate from the remote dataset commit domain, preserving AD-023.

Only a bounded number of segment writers/finalizers may be active simultaneously. Segment payloads and spool pages must follow MemoryManager / TemporaryStorageManager accounting and Query Lifecycle cleanup rules.

A process restart may discard the local spool; incomplete segments are rebuilt from the same SourceSnapshot. Remote COMPLETE segment files remain authoritative through the Manifest.

### 5. Dataset commit remains unchanged

This proposal does not change AD-009 dataset commit semantics:

```text
remote staging complete files
-> conditional final manifest / success marker
-> COMMITTED dataset
```

S3 still MUST NOT emulate atomic directory rename.

The final manifest becomes visible only after every planned segment is COMPLETE.

## Alternatives

### A. Use runtime chunk ordinal as file identity

Rejected.

Parallel scheduling and operator chunk boundaries are not stable across restart. This can preserve a filename while changing its row membership, directly violating the Phase 4 no-duplicate/no-missing requirement.

### B. Add worker/task ID to Pipeline Sink and use one file per worker

Rejected as insufficient.

A stable worker identifier does not make row-to-worker assignment stable after joins, aggregates, work stealing, retry, or different worker counts. It also couples export recovery semantics to Scheduler implementation details, conflicting with the generic Task boundary.

### C. Require `ORDER BY` and segment by row ordinal

Rejected as the general Phase 4 contract.

It would silently narrow COPY/Export to ordered queries, add a potentially global sort solely for recovery, and SQL `ORDER BY` with ties still does not guarantee a total order unless extra tie-break semantics are introduced. That would be a separate SQL semantic change.

### D. Keep one remote writer open per partition/segment

Rejected.

Partition cardinality can be unbounded and would violate WriterPool/resource limits. SFTP session count, file handles, remote requests, and writer buffers must remain bounded.

### E. Reopen closed remote files and append

Rejected as a generic contract.

`StreamingWrite` does not imply append or random-write capability. S3 multipart and SFTP streaming semantics differ, and introducing hidden whole-file rewrite/append fallback would violate the VFS capability model.

## Consequences

Positive:

- Makes completed-file resume independent of query scheduling and chunk boundaries.
- Provides a proof path for `no duplicate completed files` and `no missing files` after Kill/Restart/Resume.
- Preserves Protocol × Compression × Format orthogonality.
- Keeps writer/session count bounded even with high partition cardinality.
- Does not require deterministic Scheduler execution.

Negative:

- Adds a persisted SegmentPlan to the Manifest contract for resumable MultiFile/Partitioned jobs.
- Adds a local temporary-spool stage before final remote encoding/upload for these layouts.
- Requires stable canonical row hashing for every Phase 4 exportable logical type, including LIST/STRUCT.
- Adds local temp I/O and a second pass over incomplete segment payloads before remote finalization.
- Requires new failure/leak tests for spool cleanup, crash recovery, completed-segment skipping, and writer finalization.

## Compatibility Impact

No Stable SQL result semantics are changed. The proposal affects internal export planning and persisted export-job metadata.

Existing SingleFile behavior remains valid: an incomplete single file is redone in full.

MultiFile/Partitioned manifests created before acceptance of this ADR must not be treated as resumable unless they contain a compatible SegmentPlan. Because Phase 4 is not yet ACCEPTED/released, the exact Manifest version migration can be chosen during implementation after approval.

## Required Tests After Approval

At minimum:

1. Same logical rows with different chunk boundaries produce identical segment membership.
2. Same logical rows with different worker schedules produce identical segment membership.
3. Kill after a subset of segment files become COMPLETE, restart, and resume:
   - COMPLETE files are not rewritten;
   - incomplete files are redone;
   - final logical multiset equals a clean export;
   - no duplicate completed files;
   - no missing rows/files.
4. High-cardinality partition export respects WriterPool, memory, temp quota, and file-handle limits.
5. Cancel/failure leaves:
   - active tasks = 0;
   - query reservations = 0;
   - query temp ownership = 0;
   - client/session leases = 0;
   - incomplete multipart = 0.
6. Property tests verify stable hash routing across FLAT / CONSTANT / DICTIONARY vector representations.
7. Performance regression benchmark covers stable routing + spool + finalization.

## Affected Frozen Decisions

Approval would clarify/extend implementation requirements under these existing frozen decisions; this ADR does not modify them by itself:

- AD-003 Execution Model — keeps generic Pipeline DAG unchanged; stable segmentation remains export-specific.
- AD-009 COPY / Export — supplies the missing durable file identity required by file-level resume.
- AD-014 Query Lifecycle — local spool ownership remains inside QueryContext until files are finalized or aborted.
- AD-021 Consistency Domains — Statement SourceSnapshot and Export Manifest Commit remain separate domains.
- AD-023 Temporary Storage — local spool uses query temporary storage and remains distinct from remote export staging.
- AD-031 Go Runtime / Buffer Ownership — segment buffers remain managed/bounded.
- AD-033 Hash / Row Infrastructure — canonical row hashing is reused rather than adding export-specific hash semantics.
- I-04 Managed Memory and I-10 Lifecycle Protocol remain mandatory.

## Approval

Not approved. Phase 4 P4-D6 implementation is blocked at this point.

No Pipeline ABI, Scheduler ABI, Manifest SegmentPlan, or local spool behavior is authorized until the project owner accepts this ADR (or selects another explicitly documented alternative).
