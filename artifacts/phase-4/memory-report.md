# Phase 4 Memory Report

Date: 2026-11-01
Status: **PRELIMINARY — repository-wide gate pending**

## Managed ownership introduced in Phase 4

- `memory.TagExportBuffer`: format output, compression buffers and remote upload buffers.
- `memory.TagMetadata`: Manifest serialization, commit reconciliation and S3 multipart recovery metadata.
- Parquet page/row-group buffers are reserved through MemoryManager/BufferManager.
- WriterPool reserves per-writer ownership before invoking the writer factory.

## Development test observations

Targeted ordinary/race development tests asserted the following after success, cancellation and injected failure paths:

```text
MemoryScope.Used() = 0
WriterPool.Active() = 0
SFTP leased sessions = 0
S3 incomplete multipart (normal terminal path) = 0
S3 orphan multipart after crash recovery = 0
```

Covered paths include:

- format writer constructor allocation failure;
- CSV/JSON/Compression writer Close/Abort;
- Parquet page-buffer allocation and short-write failure;
- VFS write capability rejection;
- S3 multipart success, retry, Complete failure and Abort;
- S3 hard-kill orphan discovery/Abort recovery;
- SFTP write failure/Abort and session lease cleanup;
- LocalManifestStore Save/Load failure;
- durable DatasetWriter state-persistence failure;
- Committer write/close/reconciliation failure;
- Kill/Restart/Resume.

## Hard limits / bounded structures

- S3 multipart payload buffer: fixed managed part buffer (8 MiB).
- S3 recovery ListMultipartUploads page: max 100 uploads with a 512 KiB Metadata reservation before XML decode.
- Local Manifest size: bounded by `DefaultMaxManifestBytes` (64 MiB).
- WriterPool capacity is explicit and independent of partition count.
- DataChunk route selections are bounded by the engine chunk size.

## Final evidence required

Final acceptance must run ordinary and race tests from a complete same-commit worktree. This report must remain preliminary until `./scripts/acceptance.sh 4` passes.
