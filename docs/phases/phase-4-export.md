# Phase 4 — COPY / Export

## Goal

形成：
```text
Remote Source
→ Query
→ DataChunk
→ Format Writer
→ Compression
→ Remote Destination
```

## Required Implementation

- ExportCoordinator
- Manifest
- WriterPool
- Committer
- Parquet Writer
- CSV Writer
- JSON/NDJSON Writer
- CompressionWriter
- SingleFile / MultiFile / Partitioned
- Retry / Resume / Commit

## Manifest

至少：
- ManifestVersion
- JobID
- QueryFingerprint
- SourceSnapshot
- Destination
- Format
- Compression
- Consistency
- FileStates
- ExportState

## Resume

v1 粒度：
```text
completed file
```

Incomplete file：
```text
redo
```

## Deliverables

- P4-D1 Format conversion matrix
- P4-D2 S3↔SFTP protocol conversion
- P4-D3 Parquet writer interop
- P4-D4 Partitioned export
- P4-D5 Manifest
- P4-D6 Resume
- P4-D7 SourceSnapshot validation
- P4-D8 Dataset commit
- P4-D9 Writer limits
- P4-D10 Export failure matrix

## Dataset Commit

S3：
```text
staging
→ complete files
→ final manifest / success marker
```

不得伪造 directory rename 原子性。

## Failure Gate

必须完成：
```text
Kill / Restart / Resume
```

并保证：
- no duplicate completed files
- no missing files
- no invalid committed dataset
- no leaked multipart

## Acceptance

`artifacts/phase-4/acceptance-report.md`

milestone：
```text
v0.4-export
```
