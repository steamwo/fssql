# Phase 4 Failure Report

Date: 2026-11-01
Status: **PRELIMINARY — final failure gate pending**

## Failure matrix

| Failure | Required result | Test / evidence |
| --- | --- | --- |
| Format writer allocation failure | fail before/abort remote file; managed memory returns to zero | `TestFileWriterFactoryConstructorFailureAbortsVFS`, writer-specific allocation tests |
| Writer Close/Abort terminal misuse | no double ownership release; writer remains terminal | WriterPool lifecycle tests |
| SFTP write failure | incomplete file removed on Abort; session lease released | `internal/storage/remote/sftp/write_test.go` |
| S3 UploadPart transient 5xx | retry replayable payload; eventual success or explicit failure | `TestS3MultipartRetryAndAbort` |
| S3 CompleteMultipartUpload failure | live upload remains abortable; no hidden local ownership leak | `TestS3CompleteFailureRemainsAbortable` |
| Conditional final manifest conflict | never overwrite another committed dataset | S3 ConditionalWrite tests + Committer conflict/reconciliation tests |
| Commit response ambiguity | remain `COMMITTING`; reconcile exact final manifest before declaring committed | `committer_recovery_test.go`, durable committer tests |
| Manifest durable Save failure | in-memory state does not advance past durable state; no remote operation when precondition Save fails | Manifest durable / Coordinator / DatasetWriter / Committer durable tests |
| Query/Source/SegmentPlan mismatch on Resume | fail before any remote cleanup or data rewrite | `resume_test.go`, `resume_recovery_test.go` |
| Hard kill with a COMPLETE and a WRITING file | COMPLETE file not reopened; incomplete file redone; final file set has no duplicates/missing files | `resume_kill_restart_test.go` |
| Hard kill after S3 CreateMultipartUpload | discover orphan through job-exclusive scope and Abort it after restart | `TestS3RecoveryCapabilityAndOrphanAfterProcessStateLoss` |
| >1 page orphan multipart set | paginate every upload; do not touch neighboring job prefix | `TestS3RecoveryPaginationScopeIsolationAndIdempotence` |
| Multipart recovery 5xx | retry via existing S3 retry policy; failure blocks Resume | S3 recovery retry test |
| Multipart recovery credential failure | explicit credential error; Resume blocked | S3 recovery credential test |
| Multipart recovery cancellation | stop recovery and do not enter new writer phase | S3/VFS recovery cancellation tests |
| Partial recovery report with nil error | reject as `ErrIncompleteWriteRecoveryPartial`; Resume blocked | `TestRecoverIncompleteWritesRejectsRootInvalidAndPartialReport` |
| Recovery memory reservation failure | fail before ListMultipartUploads request; reservation remains zero | S3 recovery memory-gate test |

## Phase 4 hard-restart invariant

The accepted recovery ordering is:

```text
Load durable Manifest
→ validate QueryFingerprint / SourceSnapshot / SegmentPlan
→ RecoverIncompleteWrites(job-exclusive staging scope) when advertised
→ PrepareResume
→ durable Save
→ redo incomplete files
→ conditional dataset commit
```

No failure path is permitted to downgrade incomplete remote ownership into a warning.

## Final gate

The matrix is not acceptance evidence until the complete same-commit repository runs:

```text
go test ./...
go test -race ./...
./scripts/acceptance.sh 4
```
