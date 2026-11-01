# Phase 4 Failure Report

Date: 2026-11-01
Status: **PASS**

Acceptance workflow run: `41302222162`

The complete repository ordinary and race suites, architecture check, targeted Phase 4 package tests, protocol conversion tests and DuckDB interop all completed successfully.

## Failure matrix

| Failure | Required result | Evidence |
| --- | --- | --- |
| Format writer allocation failure | fail before/abort remote file; managed memory zero | `TestFileWriterFactoryConstructorFailureAbortsVFS` and writer allocation tests |
| Writer terminal misuse | no double release; terminal remains terminal | WriterPool lifecycle tests |
| SFTP write failure | incomplete file removed; session lease released | SFTP write tests |
| S3 UploadPart transient 5xx | retry replayable payload or explicit failure | S3 multipart retry tests |
| S3 Complete failure | upload remains abortable; no hidden ownership leak | Complete failure tests |
| Conditional final-manifest conflict | never overwrite another committed dataset | ConditionalWrite + Committer reconciliation tests |
| Commit response ambiguity | stay `COMMITTING`; reconcile exact final manifest | Committer recovery tests |
| Manifest durable Save failure | process state cannot advance beyond durable state | durable Manifest/Coordinator/DatasetWriter/Committer tests |
| Resume identity mismatch | fail before remote cleanup/data rewrite | Resume identity/recovery tests |
| Kill with COMPLETE + WRITING files | COMPLETE skipped; incomplete redone; no duplicate/missing files | `resume_kill_restart_test.go` |
| Kill after CreateMultipartUpload | discover and abort orphan after restart | S3 incomplete-write recovery tests |
| Multi-page orphan set | paginate all uploads; isolate neighboring job prefix | S3 pagination/scope/idempotence tests |
| Recovery 5xx | bounded retry; failure blocks Resume | S3 recovery retry tests |
| Recovery credential failure | explicit credential error; Resume blocked | S3 recovery credential tests |
| Recovery cancellation | stop recovery; do not enter writer phase | S3/VFS cancellation tests |
| Partial recovery report | `ErrIncompleteWriteRecoveryPartial`; Resume blocked | VFS partial-report test |
| Recovery memory failure | fail before remote list request; reservation zero | S3 recovery memory-gate test |

## Hard-restart ordering

```text
Load durable Manifest
→ validate QueryFingerprint / SourceSnapshot / SegmentPlan
→ RecoverIncompleteWrites(job-exclusive staging scope) when advertised
→ PrepareResume
→ durable Save
→ redo incomplete files
→ conditional dataset commit
```

No failure path downgrades incomplete remote ownership into a warning.

## Gate result

```text
go test ./...                 PASS
go test -race ./...           PASS
go vet ./...                  PASS
go run ./tools/architecture-check .   PASS (architecture-check: OK)
./scripts/acceptance.sh 4     PASS
```

Phase 4 failure acceptance is satisfied.
