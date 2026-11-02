# ADR-0008 — Stable SQL COPY Single-File Commit Semantics

Status: **PROPOSED**

Date: 2026-11-02

## Context

ADR-0007 / AD-045 freezes Stable v1 SQL COPY as:

```sql
COPY <qualified_name> TO <string_literal> [ ( <copy_option> [, ...] ) ]
COPY ( <select_statement> ) TO <string_literal> [ ( <copy_option> [, ...] ) ]
```

The destination string is the user-visible output URI/path. AD-025 requires Stable SQL to default to DuckDB-compatible semantics. Current DuckDB behavior for ordinary single-file `COPY ... TO` is:

- the destination itself is the output file;
- an existing destination is overwritten;
- the default implementation uses a temporary-file strategy when needed so an interrupted write does not corrupt the previous file.

The frozen Phase 4 Export architecture is stronger and more dataset-oriented:

```text
ExportCoordinator
→ Manifest
→ WriterPool
→ complete files
→ Committer
→ final manifest / success marker
```

ADR-0003 states that dataset commit remains:

```text
remote staging complete files
→ conditional final manifest / success marker
→ COMMITTED dataset
```

However, neither AD-009, ADR-0003, ADR-0004, Phase 4 nor AD-045 defines how that final marker maps to a Stable **single-file SQL COPY** destination.

The current implementation exposes this gap:

- `BoundCopyTo.Destination` is one user path/URI;
- `export.DatasetWriter` SingleFile mode needs a dataset root plus relative file path;
- `export.Committer` requires a separately supplied `FinalManifestURI`;
- no frozen rule defines a sidecar name or hidden metadata namespace;
- inventing `<destination>.manifest.json`, `_SUCCESS`, or another sibling object would add externally observable output not declared by Stable SQL;
- interpreting the SQL destination as a dataset directory would violate the frozen statement shape and DuckDB-compatible single-file expectation.

This is a public compatibility and crash-consistency decision, not an implementation detail.

## Problem

Stable SQL COPY cannot be wired to the existing Phase 4 commit machinery without choosing one of the following externally visible semantics.

## Option A — Single-file target object is the commit point; dataset marker remains for multi-file exports

**Recommended.**

Clarify AD-009 so ordinary Stable SQL COPY in v1 uses a dedicated SingleFile commit path while preserving all existing Phase 4 components and failure guarantees.

### A.1 User-visible semantics

For ordinary v1 SQL COPY:

```sql
COPY (...) TO 'scheme://host/path/result.parquet'
```

`scheme://host/path/result.parquet` is the only user-visible committed data object.

- no required sibling manifest/success-marker is created;
- existing destination content is replaced only after the new output has completed successfully;
- failed/canceled COPY must not replace a previously complete destination;
- no append or multi-file behavior is implied by this rule;
- existing multi-file/partitioned Export APIs retain their final-manifest/success-marker dataset commit semantics unchanged.

This matches the compatibility target for ordinary DuckDB single-file COPY while keeping the project-specific resume/lifecycle guarantees.

### A.2 Export lifecycle remains explicit

Stable SQL COPY still uses:

```text
QueryContext SourceSnapshot
→ ExportCoordinator
→ Manifest
→ WriterPool / FileWriterFactory
→ SingleFile writer
→ SingleFileCommitter
→ COMMITTED
```

The Manifest remains the export job/recovery state source. The change is only the final visibility/commit object for SingleFile SQL COPY.

`SingleFileCommitter` is not a second export state machine. It must drive the same Manifest `WRITING → COMMITTING → COMMITTED` states and remain owned by the Query/Export lifecycle.

### A.3 Atomic publish capability

Stable SQL COPY must never emulate atomic replacement by exposing an incomplete destination.

VFS gains a protocol-neutral atomic-publish contract for a **single object**. An implementation may satisfy it by one of these driver-internal strategies:

1. write to a hidden/staging object and atomically rename/replace when the protocol truly supports atomic rename;
2. use a protocol operation whose successful finalize makes the complete object visible atomically (for example, object-store multipart completion semantics);
3. another driver-specific mechanism that gives the same contract.

Protocol tokens and staging names remain inside the VFS adapter boundary.

The generic capability is conceptually:

```text
AtomicPublishWrite
```

and means:

```text
OpenWrite(final URI)
→ incomplete bytes are not observable as the final object
→ Close success atomically publishes the complete new object
→ Close/Abort failure leaves the previous complete final object unchanged
```

The concrete API name may follow existing VFS naming conventions, but the semantic contract above is frozen.

If a destination driver cannot provide this contract, Stable single-file SQL COPY must fail **before data output begins**. It may not silently fall back to direct in-place streaming.

### A.4 Existing destination behavior

Ordinary Stable v1 SQL COPY overwrites an existing destination on successful atomic publish, matching DuckDB single-file behavior.

The overwrite is replacement, not append. The previous complete object remains valid until the new publish succeeds.

No `OVERWRITE`, `APPEND`, `USE_TMP_FILE`, or multi-file overwrite option becomes Stable merely because the implementation internally uses staging.

### A.5 Crash/restart behavior

SingleFile remains consistent with ADR-0003:

- no SegmentPlan is required;
- incomplete output is redone as one file;
- QueryFingerprint and SourceSnapshot must match before resume;
- hidden protocol ownership must be recovered according to ADR-0004 when relevant;
- if a crash occurs after remote publish succeeds but before local Manifest state reaches COMMITTED, resume must reconcile the final object identity before deciding whether to redo or mark COMMITTED;
- reconciliation metadata is control/recovery state and does not become a public SQL sidecar contract.

### A.6 Multi-file and partitioned Export unchanged

This decision does **not** remove or weaken the existing dataset commit model for:

- MultiFile Export;
- Partitioned Export;
- any resumable dataset API whose public result is a dataset rather than one SQL target file.

Those continue to commit through conditional final manifest/success marker.

## Option B — Keep final manifest as a visible SQL COPY sidecar

Define a stable sibling such as:

```text
result.parquet
result.parquet.fssql-manifest.json
```

and treat the sidecar as the commit point.

Advantages:

- minimal change to existing `export.Committer`;
- preserves the Phase 4 dataset commit mechanism literally.

Rejected as recommendation because the extra durable object is externally observable and differs from ordinary DuckDB single-file COPY behavior. It would become a permanent Stable storage-side ABI, including naming/collision/security semantics.

## Option C — Interpret SQL destination as a dataset root

Treat:

```sql
COPY (...) TO 's3://bucket/output'
```

as a dataset directory/prefix containing generated data file(s) plus final manifest.

Rejected as recommendation because AD-045 freezes ordinary COPY TO as a destination path/URI and DuckDB-compatible single-file behavior is the default target. This option silently changes the meaning of the user's destination.

## Recommendation

Accept **Option A**.

It is the only option that simultaneously:

- preserves the Stable SQL destination as the actual output file;
- avoids inventing a public sidecar namespace;
- matches DuckDB's successful-replacement behavior for ordinary single-file COPY;
- preserves QueryContext/Manifest/WriterPool/ExportCoordinator ownership and crash recovery;
- keeps the existing final-manifest dataset commit model for MultiFile/Partitioned exports.

## Implementation order after acceptance

1. Add the protocol-neutral VFS atomic-publish write capability/contract and driver capability tests.
2. Implement atomic publish in the existing writable adapters that can prove the contract; unsupported drivers fail capability gating.
3. Add `SingleFileCommitter` over the existing Manifest lifecycle; do not create a second state machine.
4. Add SQL COPY plan mapping from `BoundCopyTo` to Export schema/format/compression/single-file routing.
5. Wire the QueryContext SourceSnapshot into ExportCoordinator and refuse COPY if it is not frozen.
6. Stream native DataChunks through existing FileWriterFactory/DatasetWriter.
7. Add existing-destination replacement, cancel, write failure, hard-kill/reconcile, leak and no-partial-final tests.
8. Keep MultiFile/Partitioned Export commit tests unchanged.

## Acceptance impact

Until this ADR is accepted or replaced:

```text
ADR-0006 remediation #6 COPY execution is BLOCKED at final single-file commit semantics.
DESCRIBE and other unrelated remediation work may continue.
Phase 5 remains IN_PROGRESS.
Phase 6 remains LOCKED.
```

## Architecture invariants

If Option A is accepted:

- I-04 remains: writer/staging buffers stay MemoryManager-owned;
- I-05 remains: no whole-file read fallback is introduced;
- I-06 remains: atomic-publish capability is protocol-neutral and driver-implemented;
- I-07 remains: COPY is tied to the immutable Statement SourceSnapshot;
- I-08 remains: spill is unrelated and still uses TemporaryStorageManager;
- I-10 is strengthened: hidden staging/multipart ownership must be closed/recovered before terminal success;
- I-12 is strengthened: drivers without atomic single-file publish fail explicitly rather than silently using unsafe in-place writes.

This ADR would amend the SingleFile interpretation of AD-009 / ADR-0003 only. MultiFile/Partitioned dataset commit semantics remain frozen as they are today.
