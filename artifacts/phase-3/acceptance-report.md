# Phase 3 Acceptance Report

Date: 2026-11-01
Phase: 3 — CSV / JSON / NDJSON + Compression + SFTP
Milestone: `v0.3-remote-scan`
Status: **ACCEPTED**

## Deliverables

| Deliverable | Status | Evidence |
|---|---|---|
| P3-D1 CSV compliance corpus | PASS | quote-aware FSM; escaped quotes; CRLF; quoted newline; malformed/width/allocation tests |
| P3-D2 parallel CSV verification | PASS | shared lexical boundary summaries; `parallel == sequential`; explicit sequential fallback when capability/safe split is unavailable |
| P3-D3 CSV schema inspection | PASS | bounded inference embedded in `SourceBinding`; Inspect/Describe project the same binding schema |
| P3-D4 NDJSON parallel verification | PASS | record-boundary range planning; `parallel == sequential`; managed range reads |
| P3-D5 JSON typed decoder | PASS | structural tokenizer; ProjectedPathSet; typed direct-to-Vector decode; native LIST/STRUCT; inference; malformed/allocation tests |
| P3-D6 compression capability demo | PASS | codec abstraction; gzip; sequential zstd baseline; VFS × Compression × Format capability composition; compressed logical result equality |
| P3-D7 SFTP query demo | PASS | SFTP v3 wire session + VFS driver + CSV Binder/Source query path |
| P3-D8 SFTP pool verification | PASS | endpoint-keyed bounded pool; cancel wait; reuse; poison/discard; parallel capability tied to pool capacity |
| P3-D9 host-key security test | PASS | OpenSSH strict known_hosts; wrong host key is a hard failure; no insecure fallback/retry |
| P3-D10 weak-network test | PASS | reset retry; short read; cancel; failed dial rollback; lease/memory cleanup |

## Architecture

- CSV parser and boundary recognizer share one lexical state machine; no heuristic secondary parser was introduced.
- Internal output remains the engine Vector/DataChunk ABI.
- CSV/JSON/compression/SFTP large payloads use MemoryManager/BufferManager or explicit Reservation.
- VFS, Compression, and Format remain orthogonal; gzip and the Phase 3 zstd baseline advertise sequential/non-splittable capability.
- SFTP uses a bounded session pool instead of the HTTP ClientManager. Session identity is scoped by endpoint, credential scope/version and security profile.
- SFTP host-key verification is strict and cannot silently fall back to insecure behavior.
- Source introspection consumes the real CSV binding result rather than running independent schema inference.
- No FROZEN Architecture Decision or Architecture Invariant was changed.

## Full-repository Gate

The phase-ending source tree was materialized locally from the current `phase3-text-sftp` Git tree and the complete Phase 0–3 package/test graph was executed.

```text
go test ./...                         PASS
go test -race ./...                   PASS
go vet ./...                          PASS
go run ./tools/architecture-check .   PASS (architecture-check: OK)
./scripts/acceptance.sh 3              PASS
```

The Phase 3 acceptance script repeated the full-repository test/race/vet/architecture gate and additionally passed the targeted CSV, JSON/NDJSON, compression, SFTP lifecycle/failure, DuckDB compatibility-golden, benchmark, and memory-metrics checks.

## DuckDB compatibility

A DuckDB v2.0.0 executable was not available in the isolated execution container. `test/interop/phase3/golden_test.go` therefore uses fixtures and expected results frozen directly from DuckDB v2.0.0's regression corpus as the offline compatibility oracle.

Covered v2.0.0 oracle cases include:

- NDJSON `id/name` rows from DuckDB's `example_n.ndjson` regression corpus;
- nested LIST schema inference from DuckDB's `with_list.json` corpus;
- CSV quoted-newline behavior across a small buffer boundary from DuckDB's `csv_quoted_newline_after_boundary.test`.

The offline golden verifier passed as part of `./scripts/acceptance.sh 3`. It is compatibility evidence and does not claim that a local DuckDB process was executed.

## Memory / failure / performance

See:

- `benchmark-report.md`
- `memory-report.md`
- `failure-report.md`
- `known-limitations.md`

The Phase 3 memory scenario terminates with managed memory = 0 and SFTP total/leased/idle sessions = 0. Required Phase 3 failure classes have explicit regression coverage. Performance-critical text, compression, and SFTP pool paths have benchmark baselines.

## Acceptance decision

All Phase 3 deliverables and required Gates pass. Phase 3 is **ACCEPTED**. Phase 4 remains locked until it is explicitly started; acceptance of Phase 3 does not implicitly begin the next phase.
