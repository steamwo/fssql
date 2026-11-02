# Phase 5 Acceptance Report

Date: 2026-11-02
Milestone: `v1.0-rc`
Status: **ACCEPTED — ADR-0006 remediation revalidated**

## Acceptance evidence

Phase 5 was reopened by ADR-0006 because the frozen Stable v1 contract was not yet fully represented by the implementation and release evidence. The remediation is now complete and was revalidated with the repository-level Phase 5 acceptance script.

- Acceptance candidate commit: `114788a4ef6097008f4a8437ced71a821f7af260`
- Workflow run: `41424206444`
- Workflow job: `125206760915`
- Runner: Ubuntu 24.04.5 / linux amd64
- Go: 1.22.12
- CPU: AMD EPYC 7763 64-Core Processor
- DuckDB reference: 2.0.0

The complete Phase 5 gate returned exit status 0:

```text
go test ./...
go test -race ./...
go vet ./...
go run ./tools/architecture-check .

# Stable public composition / COPY / atomic publish
go test -count=1 \
  ./pkg/gosql \
  ./internal/session \
  ./internal/export \
  ./internal/storage/vfs \
  ./internal/storage/remote/s3
go test -race -count=1 ./pkg/gosql

# SQLLogic + DuckDB differential + deviation registry
FSSQL_REQUIRE_DUCKDB=1 go test -count=1 ./test/compat
FSSQL_REQUIRE_DUCKDB=1 go test -race -count=1 ./test/compat

bash ./scripts/phase5-cancellation-leak.sh
bash ./scripts/phase5-security.sh
bash ./scripts/phase5-config-compatibility.sh
bash ./scripts/phase5-stability.sh

# Final server/resource/security/remote rerun and Phase 5 benchmarks
```

`architecture-check: OK`; complete repository ordinary/race/vet, Stable SQL compatibility, lifecycle/security/config/stability, final protocol reruns and benchmark commands all passed.

## ADR-0006 remediation closure

The re-acceptance closes the Stable-contract gaps identified before Phase 6:

- Stable SQL now has one production Parser → Binder → Planner → Optimizer → Execution path.
- `pkg/gosql` exposes the native Go API and `database/sql` compatibility boundary without leaking `internal/*` ABI.
- Stable `COPY TO` uses the existing Export/VFS ownership model, including single-file commit/reconciliation.
- S3 advertises and implements `AtomicPublishWrite`; it still does not claim `AtomicRename`.
- SQLLogic expected-result coverage and live DuckDB 2.0.0 differential verification are release gates.
- `compatibility/deviations.yaml` is machine validated; a Stable corpus mismatch must be classified, stale classifications fail, known bugs fail acceptance, and `unsupported_feature` cannot waive a frozen Stable SQL case.
- Current Stable SQL corpus has no unresolved or unclassified DuckDB mismatch.

Compatibility testing exposed and permanently regressed several implementation defects during remediation: BOOLEAN fallback composition, BIGINT comparison execution, INTEGER→BIGINT widening, and zero-physical-column filter cardinality. The first full re-acceptance run also exposed a stale S3 static-capability expectation after `AtomicPublishWrite` was added; the protocol matrix was corrected without weakening the driver boundary.

## Phase 5 deliverables

| Deliverable | Evidence | Status |
| --- | --- | --- |
| P5-D1 FTP/FTPS | bounded session pool, FTPS default, explicit plain-FTP policy, lifecycle/failure tests | PASS |
| P5-D2 WebDAV | capability-aware HTTP/WebDAV driver, range/redirect/security/lifecycle tests | PASS |
| P5-D3 Endpoint capability matrix | endpoint/runtime capability probing; S3 `AtomicPublishWrite`; no silent range fallback | PASS |
| P5-D4 Server query API | transport-neutral QueryService + HTTP submit/status/cancel/forget/results API | PASS |
| P5-D5 Source inspect API | shared Inspector/Bind schema-resolution path | PASS |
| P5-D6 Query state machine | SUBMITTED→QUEUED→PLANNING→RUNNING→FINISHING→terminal | PASS |
| P5-D7 slow-client backpressure | fixed-capacity ResultCursor; queued + consumer-held ownership bounded | PASS |
| P5-D8 cancellation leak | query/async/temp/SFTP/S3 lifecycle gate + race tests | PASS |
| P5-D9 ResourceGroup fairness | bounded admission and ResourceGroup fairness tests | PASS |
| P5-D10 security suite | NetworkPolicy, SSRF/loopback/link-local/metadata/redirect/port/secret/protocol tests | PASS |
| P5-D11 credential rotation | scoped secret versioning and endpoint client credential-version isolation | PASS |
| P5-D12 observability | worker-local counters + Prometheus/OpenTelemetry adapters | PASS |
| P5-D13 graceful shutdown | stop admission, cancel/drain Query ownership, timeout reports remaining ownership | PASS |
| P5-D14 config compatibility | versioned explicit migrations, typed Registry, secure defaults, ordinary/race gate | PASS |
| P5-D15 long-running stability | repeated Query lifecycle + race + FTP/WebDAV/SFTP/S3 repeated ownership tests | PASS |

## Public/compatibility boundary

The registry records three `database/sql` boundary differences with `stable_sql_semantics: false`: parameter binding is outside the frozen v1 API contract, LIST/STRUCT remain native-Chunk-only for lossless access, and DECIMAL/HUGEINT cross `database/sql` as exact text. None is used to waive Stable SQL semantics.

## Result

Phase 5 Correctness, Performance, Memory/Ownership and Failure gates are satisfied on the remediated Stable v1 implementation. Phase 5 is ACCEPTED again. The project may enter Phase 6 Feature Freeze; Phase 6 must not introduce new Stable features.
