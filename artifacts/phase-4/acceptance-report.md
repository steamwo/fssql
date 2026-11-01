# Phase 4 Acceptance Report

Date: 2026-11-01
Milestone: `v0.4-export`
Status: **ACCEPTED**

## Acceptance evidence

Phase 4 was verified by the temporary repository-level GitHub Actions gate for PR #7.

- Candidate branch commit: `492966406e37965370619e9d1fc6bb6178b18e73`
- PR merge test ref: `34607b7a9656b2c02827649a595a3e31339fb816`
- Workflow run: `41302222162`
- Runner: Ubuntu 24.04, Go 1.22.12, Python 3.12.3
- DuckDB reader: 2.0.0

The following complete gate returned success:

```text
go test ./...
go test -race ./...
go vet ./...
go run ./tools/architecture-check .
go test -count=1 ./internal/export
go test -count=1 ./internal/storage/vfs
go test -count=1 ./internal/storage/remote/s3
go test -count=1 ./internal/storage/remote/sftp
go test -count=1 ./test/interop/phase4 -run 'Test(S3ToSFTP|SFTPToS3)ProtocolConversion$'
python3 ./test/interop/phase4/run.py
go test -run '^$' -bench 'Benchmark(WriterPoolAcquireRelease|PartitionRouting2048x64|ParquetWriterInteger1024)$' -benchmem -benchtime=100ms ./internal/export
```

`architecture-check: OK` and all commands above completed with exit status 0.

## Scope

Phase 4 implements the frozen export path:

```text
Remote Source
→ Query / SourceSnapshot
→ Own DataChunk
→ Format Writer
→ Compression
→ VFS Remote Destination
```

No Phase 5 capability is included.

## Architecture decisions

- AD-009 COPY / Export remains the controlling frozen decision.
- ADR-0003 is ACCEPTED: resumable MultiFile/Partitioned export uses a stable Export SegmentPlan; generic DataChunk/Pipeline/Scheduler ABI is unchanged.
- ADR-0004 is ACCEPTED: crash-resumable remote export performs protocol-neutral VFS incomplete-write recovery before redoing incomplete files.
- S3 does not advertise or emulate AtomicRename.
- Protocol × Compression × Format remains orthogonal.

## Deliverables

| Deliverable | Evidence | Status |
| --- | --- | --- |
| P4-D1 Format conversion matrix | `format_matrix_test.go`, CSV/JSON/NDJSON + identity/gzip/zstd matrix, Parquet outer-compression rejection | PASS |
| P4-D2 S3↔SFTP protocol conversion | real S3/SFTP VFS drivers → CSV decode → Own DataChunk → export writer, both directions | PASS |
| P4-D3 Parquet writer interop | fssql writer fixture read by DuckDB 2.0.0 with exact rows `(1,alpha),(2,beta),(3,NULL)` | PASS |
| P4-D4 Partitioned export | stable routing, hive-style partitions, DICTIONARY row selection, bounded writer lifecycle | PASS |
| P4-D5 Manifest | versioned Manifest, file/export state machine, durable LocalManifestStore | PASS |
| P4-D6 Resume | SegmentPlan identity, durable state, Kill/Restart/Resume, commit reconciliation | PASS |
| P4-D7 SourceSnapshot validation | frozen QueryContext snapshot checked before writing/resume | PASS |
| P4-D8 Dataset commit | conditional final manifest, COMMITTING reconciliation, no fake S3 rename | PASS |
| P4-D9 Writer limits | bounded WriterPool + managed per-writer reservation | PASS |
| P4-D10 Export failure matrix | cancellation, memory/credential/write/commit failures and hard-kill multipart recovery | PASS |

## Result

Phase 4 acceptance requirements are satisfied. Phase 5 may be unlocked only after the accepted Phase 4 changes and acceptance evidence are merged to `main`.
