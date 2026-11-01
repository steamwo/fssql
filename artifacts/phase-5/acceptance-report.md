# Phase 5 Acceptance Report

Date: 2026-11-01
Milestone: `v1.0-rc`
Status: **ACCEPTED**

## Acceptance evidence

Phase 5 was verified by the repository-level temporary GitHub Actions gate after ADR-0005/AD-043 and the ResultCursor I-10 ownership fix were merged to `main`.

- Accepted architecture baseline: `4834815d0c1c4a3c774e9a08f426b8905b5ddb16` (ADR-0005 / AD-043)
- Result lifecycle fix on `main`: `76557469c2164a6e1768b85ceb5f27d95c2c1472`
- Acceptance candidate commit: `505c89d91814464036238a403adc232753d01e19`
- Workflow run: `41318791929`
- Runner: Ubuntu 24.04.5 / linux amd64
- Go: 1.22.12
- CPU: AMD EPYC 7763 64-Core Processor

The following Phase 5 gate completed successfully:

```text
go test ./...
go test -race ./...
go vet ./...
go run ./tools/architecture-check .
bash ./scripts/phase5-cancellation-leak.sh
bash ./scripts/phase5-security.sh
bash ./scripts/phase5-config-compatibility.sh
bash ./scripts/phase5-stability.sh

go test -count=1 \
  ./internal/server \
  ./internal/resource \
  ./internal/security \
  ./internal/settings \
  ./internal/storage/remote \
  ./internal/storage/remote/ftp \
  ./internal/storage/remote/webdav \
  ./internal/storage/remote/sftp \
  ./internal/storage/remote/s3

go test -run '^$' -bench 'Benchmark' -benchmem -benchtime=100ms \
  ./internal/server \
  ./internal/profile \
  ./internal/storage/remote/ftp \
  ./internal/storage/remote/webdav
```

`architecture-check: OK`; ordinary tests, complete repository race tests, vet, targeted lifecycle/security/config/stability gates and final benchmark commands all returned exit status 0.

## Architecture resolution

ADR-0005 is ACCEPTED and registered as AD-043 FROZEN:

- Phase 5 / v1 requires the shared transport-neutral QueryService core plus the HTTP server adapter.
- gRPC is not a Phase 5 acceptance requirement; any future Stable gRPC wire contract requires a separate ADR.
- QueryService / QueryRegistry / QueryHandle / QueryContext / ResultCursor remain the only server lifecycle core.
- transport SDK types do not enter Core / Execution / Query / Resource ABI.

The final gate also exposed and then verified the fix for a real I-10 ownership race: a Query may not become terminal after a result leaves the ResultCursor ring while the consumer still owns the Managed DataChunk. The accepted implementation now requires `ReleaseChunk` and waits for consumer ownership to reach zero before terminal publication.

## Deliverables

| Deliverable | Evidence | Status |
| --- | --- | --- |
| P5-D1 FTP/FTPS | bounded session pool, FTPS default, explicit plain-FTP policy, lifecycle/failure tests | PASS |
| P5-D2 WebDAV | capability-aware HTTP/WebDAV driver, range/redirect/security/lifecycle tests | PASS |
| P5-D3 Endpoint capability matrix | endpoint/runtime capability probing without silent range fallback | PASS |
| P5-D4 Server query API | transport-neutral QueryService + HTTP submit/status/cancel/forget/results API | PASS |
| P5-D5 Source inspect API | `POST /v1/sources/inspect`, shared Inspector registry, Bind-backed CSV/Parquet adapters | PASS |
| P5-D6 Query state machine | SUBMITTED→QUEUED→PLANNING→RUNNING→FINISHING→terminal | PASS |
| P5-D7 slow-client backpressure | fixed-capacity ResultCursor; queued + consumer-held ownership bounded | PASS |
| P5-D8 cancellation leak | query/async/temp/SFTP/S3 lifecycle gate + race tests | PASS |
| P5-D9 ResourceGroup fairness | bounded admission and ResourceGroup fairness tests | PASS |
| P5-D10 security suite | NetworkPolicy, SSRF/loopback/link-local/metadata/redirect/port/secret/protocol tests | PASS |
| P5-D11 credential rotation | scoped secret versioning and endpoint client credential-version isolation | PASS |
| P5-D12 observability | worker-local counters + Prometheus/OpenTelemetry adapters | PASS |
| P5-D13 graceful shutdown | stop admission, cancel/drain Query ownership, timeout reports remaining ownership | PASS |
| P5-D14 config compatibility | versioned explicit migrations, typed Registry, secure defaults, normal/race gate | PASS |
| P5-D15 long-running stability | repeated Query lifecycle + race + FTP/WebDAV/SFTP/S3 repeated ownership tests | PASS |

## Result

Phase 5 Correctness, Performance, Memory/Ownership and Failure gates are satisfied for the documented Phase 5 scope. Phase 6 implementation remains out of scope for this acceptance change.
