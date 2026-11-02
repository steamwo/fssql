# Phase 5 Failure Report

Date: 2026-11-02
Status: **PASS — remediated acceptance**

Acceptance candidate: `114788a4ef6097008f4a8437ced71a821f7af260`
Workflow run: `41424206444`

The final complete repository ordinary/race/vet/architecture gate plus Stable SQL compatibility, public composition, lifecycle/security/config/stability gates all completed successfully.

## Failure matrix

| Failure / pressure case | Required result | Evidence |
| --- | --- | --- |
| Query cancellation while active | unblock execution/results; terminal ownership zero | P5-D8 query/server lifecycle + race gate |
| Async I/O cancellation | request stops; no async ownership leak | P5-D8 async I/O cancellation tests |
| Temp failure/cancel | ownership released through TemporaryStorageManager | P5-D8 temp lifecycle tests |
| SFTP cancel/failure | bounded session lease returned | P5-D8/P5-D10 SFTP tests |
| S3 multipart failure/cancel | complete or abort; no hidden multipart ownership | P5-D8 S3 multipart cleanup tests |
| Single-object S3 publish failure | incomplete final object is not exposed; prior complete object remains valid | S3 `AtomicPublishWrite` tests + final package rerun |
| COPY interruption/restart | manifest/reconciliation owns staged state; no silent partial final publication | session/export/VFS acceptance rerun |
| SSRF / loopback / private/link-local / metadata endpoint | reject before unsafe network access | P5-D10 NetworkPolicy tests |
| Redirect to forbidden endpoint | revalidate and reject | P5-D10 redirect security tests |
| Forbidden port | reject | P5-D10 NetworkPolicy tests |
| Unauthorized secret scope | reject without credential disclosure | P5-D10 Secret ACL tests |
| Wrong SFTP host key | fail closed, no insecure retry | P5-D10 SFTP security tests |
| Plain FTP without opt-in | reject; FTPS/security default preserved | P5-D10 + P5-D14 tests |
| Credential rotation | new version isolated from stale endpoint client state | P5-D11 credential-version tests |
| Invalid/future config | explicit failure; no silent downgrade | P5-D14 config compatibility gate |
| Slow result consumer | bounded queued + consumer-held chunks; producer backpressure | P5-D7 ResultCursor tests |
| Consumer disconnect / result abandonment | cancel/release result ownership | public/server result lifecycle tests |
| Stable SQL semantic mismatch | fail unless explicitly classified; bug/Stable-unsupported still block acceptance | SQLLogic + live DuckDB differential + deviations registry |
| Shutdown with running/queued/slow Query | stop admission, cancel/drain, release ownership | P5-D13 graceful shutdown tests |
| Shutdown with uncooperative Query | deadline failure reports remaining ownership; never false-success | P5-D13 timeout test |
| Unsupported source inspector | explicit unsupported error; no second inference fallback | P5-D5 introspection registry tests |

## Remediation defects found and fixed

ADR-0006 revalidation found real compatibility defects before final acceptance:

- BOOLEAN composition could fall outside the typed fallback path.
- BIGINT comparison execution was missing for aggregate/HAVING paths.
- Binder-inserted INTEGER→BIGINT widening was not executable.
- A zero-physical-column relation could lose filter cardinality during materialization.

Each fix has a regression test and was rechecked under the race detector. The live DuckDB 2.0.0 corpus passes after these fixes.

The first full re-acceptance run also caught a stale protocol capability-matrix expectation: the S3 driver correctly advertised the remediation's `AtomicPublishWrite`, while the matrix still expected the pre-remediation bitset. The test was corrected to include `AtomicPublishWrite` while continuing to reject `AtomicRename`.

## Gate result

```text
go test ./...                                      PASS
go test -race ./...                                PASS
go vet ./...                                       PASS
go run ./tools/architecture-check .                PASS
Stable SQLLogic + DuckDB 2.0.0 differential       PASS
P5-D8 cancellation leak gate                       PASS
P5-D10/P5-D11 security gate                        PASS
P5-D14 config compatibility gate                   PASS
P5-D15 long-running stability gate                 PASS
public/COPY/VFS/S3 final package reruns             PASS
```

Phase 5 failure acceptance is satisfied. No failure path may silently weaken security, downgrade a VFS capability contract, publish incomplete COPY output, waive an unknown Stable SQL mismatch, or publish Query terminal state while owned resources remain live.
