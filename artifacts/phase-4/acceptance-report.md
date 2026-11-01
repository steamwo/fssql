# Phase 4 Acceptance Report

Date: 2026-11-01
Milestone: `v0.4-export`
Status: **BLOCKED — evidence incomplete; Phase 4 remains IN_PROGRESS**

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

| Deliverable | Implementation / evidence | Current status |
| --- | --- | --- |
| P4-D1 Format conversion matrix | `internal/export/format_matrix_test.go`, CSV/JSON/NDJSON writers, CompressionWriter, Parquet whole-file compression rejection | IMPLEMENTED; full package gate pending |
| P4-D2 S3↔SFTP protocol conversion | `test/interop/phase4/protocol_conversion_test.go` exercises VFS read → CSV decode → Own DataChunk → export writer in both directions | IMPLEMENTED; full-repo execution pending |
| P4-D3 Parquet writer interop | memory-governed Parquet writer, fixture generator, `test/interop/phase4/run.py` DuckDB reader gate | BLOCKED in current execution environment: DuckDB module/CLI unavailable |
| P4-D4 Partitioned export | stable routing, DICTIONARY row selection, bounded writer lifecycle | IMPLEMENTED; targeted tests/harness passed |
| P4-D5 Manifest | versioned Manifest, file/export state machine, durable LocalManifestStore | IMPLEMENTED; targeted tests/harness passed |
| P4-D6 Resume | SegmentPlan identity, durable start/file/commit states, Kill/Restart/Resume, commit reconciliation | IMPLEMENTED; targeted tests/harness passed |
| P4-D7 SourceSnapshot validation | QueryContext frozen source snapshot bound to ExportCoordinator and Resume identity | IMPLEMENTED; targeted tests/harness passed |
| P4-D8 Dataset commit | ConditionalWrite final manifest; COMMITTING reconciliation; no fake directory rename | IMPLEMENTED; targeted tests/harness passed |
| P4-D9 Writer limits | bounded WriterPool, per-writer ExportBuffer reservation, terminal lifecycle | IMPLEMENTED; targeted tests/harness passed |
| P4-D10 Export failure matrix | cancellation, memory failure, credential failure, write/close failure, commit ambiguity, hard-kill multipart recovery | IMPLEMENTED; final full-repo failure gate pending |

## Test evidence completed during implementation

The following independently reconstructed production-equivalent subgraphs were executed locally with both ordinary and race tests while each task was developed:

- Manifest/SegmentPlan/Resume state machine;
- durable ManifestStore and durable transition helpers;
- Committer and COMMITTING reconciliation;
- Kill/Restart/Resume path;
- VFS incomplete-write recovery capability gate;
- S3 multipart discovery/abort recovery including pagination, retry, credential failure, cancellation and memory gate;
- WriterPool and routing benchmarks/targeted lifecycle tests.

These results are not substituted for the repository-wide acceptance gate.

## Required final gate

Phase 4 MUST NOT be marked ACCEPTED until all of the following run from a complete repository worktree at the same commit:

```text
go test ./...
go test -race ./...
go vet ./...
go run ./tools/architecture-check .
./scripts/acceptance.sh 4
```

`./scripts/acceptance.sh 4` additionally requires DuckDB to read the Parquet writer fixture. Missing DuckDB returns BLOCKED rather than skip-success.

## Current blockers

1. The current execution container cannot clone the private repository through its network path, so the entire branch has not yet been executed as one local worktree.
2. DuckDB Python module and DuckDB CLI are unavailable in the current container; the P4-D3 external writer interop gate therefore cannot be recorded as PASS here.

Until those are resolved, `docs/machine/project-state.yaml` must remain `phase_4: IN_PROGRESS` and Phase 5 must remain LOCKED.
