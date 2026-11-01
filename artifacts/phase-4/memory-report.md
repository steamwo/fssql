# Phase 4 Memory Report

Date: 2026-11-01
Status: **PASS**

## Acceptance evidence

Workflow run `41302222162` completed both:

```text
go test ./...
go test -race ./...
```

The race run passed for `internal/export`, VFS, S3, SFTP, Phase 4 interop and the complete repository test set.

## Managed ownership introduced in Phase 4

- `memory.TagExportBuffer`: format output, compression buffers and remote upload buffers.
- `memory.TagMetadata`: Manifest serialization, commit reconciliation and S3 multipart recovery metadata.
- Parquet page/row-group buffers are reserved through MemoryManager/BufferManager.
- WriterPool reserves per-writer ownership before invoking the writer factory.

## Ownership assertions covered by tests

Success, cancellation and injected failure paths assert terminal ownership including:

```text
MemoryScope.Used() = 0
WriterPool.Active() = 0
SFTP leased sessions = 0
S3 incomplete multipart (normal terminal path) = 0
S3 orphan multipart after crash recovery = 0
```

Covered paths include writer allocation/Close/Abort, Parquet allocation/short-write, VFS capability rejection, S3 multipart retry/complete/abort, hard-kill orphan recovery, SFTP session cleanup, Manifest durable Save/Load failure, durable DatasetWriter failure, Committer reconciliation, and Kill/Restart/Resume.

## Bounded structures

- S3 multipart payload buffer: fixed managed part buffer (8 MiB).
- S3 recovery ListMultipartUploads page: max 100 uploads with a 512 KiB Metadata reservation before XML decode.
- Local Manifest size: bounded by `DefaultMaxManifestBytes` (64 MiB).
- WriterPool capacity is explicit and independent of partition count.
- DataChunk route selections are bounded by the engine chunk size.
- zstd export raw-block buffer is fixed at 128 KiB and its frame window matches that bound.

## Result

Phase 4 satisfies the managed-memory and ownership gate. No Phase 4 terminal path is permitted to retain a writer lease, managed reservation, SFTP lease, or recoverable S3 multipart ownership.
