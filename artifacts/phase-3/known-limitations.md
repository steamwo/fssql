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

## Compatibility evidence limitation

The isolated acceptance environment did not contain a DuckDB v2.0.0 executable and outbound download access was unavailable. Stable text-format compatibility is therefore verified by `test/interop/phase3/golden_test.go`, using fixtures and expected outputs frozen from DuckDB v2.0.0's own regression corpus. This is an offline DuckDB compatibility oracle, not execution of a local DuckDB process.

## Acceptance validation

The complete Phase 0–3 source/package/test graph was reconstructed locally from the current Phase 3 Git tree. The required full-repository Gate passed:

```text
go test ./...
go test -race ./...
go vet ./...
go run ./tools/architecture-check .
./scripts/acceptance.sh 3
```

No Architecture Invariant or acceptance requirement is waived.
