# Phase 4 Known Limitations

Date: 2026-11-01
Status: **ACCEPTED**

## Product / contract limitations

1. **Resumable MultiFile/Partitioned export requires a stable Export SegmentPlan.**
   Runtime worker IDs, goroutine IDs and DataChunk arrival order are not valid file identities. Generic Pipeline/Scheduler ABI is intentionally unchanged.

2. **SFTP does not advertise ConditionalWrite or AtomicRename.**
   The implementation does not fabricate dataset-level atomic commit semantics for SFTP. Incomplete ordinary files are redone by truncation/rewrite; hidden multipart recovery is not applicable.

3. **S3 dataset commit is object-marker based, not directory rename based.**
   Complete staging files become visible as a committed dataset through a conditional final Manifest object. Atomic directory rename is never emulated.

4. **Parquet whole-file gzip/zstd wrapping is rejected.**
   Phase 4 permits CSV/JSON/NDJSON outer identity/gzip/zstd and Parquet identity only. Parquet page compression remains a distinct format-internal concern.

5. **Crash recovery requires a job-exclusive remote staging scope.**
   ADR-0004 cleanup must never run against a shared bucket or dataset root. S3 recovery is scoped to the exact job prefix.

6. **Crash-resumable S3 credentials require multipart discovery/abort permission.**
   Missing permission is a hard Resume failure, not a warning or silent fallback.

7. **Phase 4 resumability is file-level, not byte/checkpoint-level.**
   A file that was not durably COMPLETE is redone as a whole file after restart.

## Interoperability evidence boundary

The acceptance gate verified the fssql Parquet writer with DuckDB 2.0.0 and exact row values. PyArrow was not installed in the acceptance runner and remains optional supplementary evidence rather than a Phase 4 acceptance dependency.

## Performance note

Partition routing remains materially more allocation-heavy than WriterPool control-path operations. The accepted Phase 4 baseline records this for later regression work; no Phase 5 optimization has been pulled into Phase 4.
