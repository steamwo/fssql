# Phase 4 Known Limitations

Date: 2026-11-01
Status: **PRELIMINARY**

## Product / contract limitations

1. **Resumable MultiFile/Partitioned export requires a stable Export SegmentPlan.**
   Runtime worker IDs, goroutine IDs and DataChunk arrival order are not valid file identities. Generic Pipeline/Scheduler ABI is intentionally unchanged.

2. **SFTP does not advertise ConditionalWrite or AtomicRename.**
   The implementation does not fabricate dataset-level atomic commit semantics for SFTP. Incomplete ordinary files are redone by truncation/rewrite; hidden multipart recovery is not applicable.

3. **S3 dataset commit is object-marker based, not directory rename based.**
   Complete staging files become visible as a committed dataset through a conditional final Manifest object. Atomic directory rename is never emulated.

4. **Parquet whole-file gzip/zstd wrapping is rejected.**
   Parquet writer boundaries remain independent of the outer CompressionWriter. The Phase 4 format/compression matrix permits CSV/JSON/NDJSON outer gzip/zstd and Parquet identity only.

5. **Crash recovery requires job-exclusive remote staging scope.**
   ADR-0004 cleanup must never run against a shared bucket/dataset root. S3 recovery is scoped to the exact job prefix.

6. **Crash-resumable S3 credentials need multipart discovery/abort permission.**
   Missing permission is a hard Resume failure, not a warning or silent fallback.

## Current validation-environment limitations

These are not product contracts, but they currently block Phase 4 acceptance evidence:

- The execution container cannot clone the private GitHub repository through its local network path, so the entire `phase4-export` branch has not yet been executed as a single local worktree.
- DuckDB Python module and DuckDB CLI are not installed in the current container. The repository includes a DuckDB Parquet writer interop gate, but the gate currently reports BLOCKED rather than PASS.
- PyArrow is not installed; it is optional Phase 4 writer interop evidence and is not the DuckDB acceptance target.

No workflow file was added to work around these environment limitations.
