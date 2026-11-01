# Phase 3 Known Limitations

Date: 2026-11-01
Phase: 3 — CSV / JSON / NDJSON + Compression + SFTP

## Product limitations within the Phase 3 contract

- Ordinary gzip is sequential/non-splittable by design.
- The Phase 3 zstd implementation is a sequential baseline; no seekable/splittable zstd capability is advertised.
- Range-parallel execution is enabled only when the VFS, compression layer, and format all permit it. If CSV/NDJSON cannot prove a safe split boundary, execution reduces parallelism or falls back to sequential behavior rather than guessing.
- JSON typed decoding supports the Phase 3 native LIST/STRUCT path; Phase 3 does not add unrelated future JSON SQL features.
- CSV and NDJSON schema inference is sampled/bounded. Full multi-file schema validation remains governed by the Source Introspection contract and is not silently enabled as a normal DESCRIBE path.
- The concrete SFTP transport uses the local OpenSSH client and requires an explicit `known_hosts` file. Insecure host-key fallback is intentionally unsupported.
- SFTP session reuse is endpoint/credential/security-profile scoped and bounded; the driver does not provide an unbounded connection cache.

## Validation-environment limitation

The implementation environment could not obtain a complete repository checkout from GitHub: outbound network access from the local execution container is blocked, while the GitHub connector exposes repository files but not a materializable full source archive. Therefore the mandatory exact-current-head full-repository commands from `AGENTS.md`:

```text
go test ./...
go test -race ./...
```

could not be executed against one complete local checkout.

What was executed locally:

- the complete Phase 3 changed/new package dependency graph: tests, race tests, vet, and architecture-check;
- exact current JSON, compression, SFTP, scan-capability and compatibility-verifier sources;
- CSV task-local exact tests throughout implementation, including lexer, boundary, decode, inference, Bind/Inspect and parallel/sequential equality;
- Phase 3 benchmarks;
- `cmd/phase3-metrics` memory/lifecycle evidence;
- DuckDB v2.0.0 offline golden compatibility tests using fixtures and expected outputs from DuckDB's own v2.0.0 regression corpus.

The accepted Phase 2 baseline already has full-repository test/race/vet/architecture evidence, and Phase 3 does not modify existing Phase 0–2 implementation files other than additive Phase 3 memory tags and scan-capability code. This is useful regression evidence, but it does not replace the explicit full-current-head Gate required by `AGENTS.md`.

Because of that environment limitation, Phase 3 must remain `IN_PROGRESS` until the complete-repository Gate is executed successfully. No Architecture Invariant or acceptance requirement is waived.
