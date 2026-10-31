# Phase 3 — CSV / JSON / NDJSON + Compression + SFTP

## Goal

证明架构不只适用于 S3 + Parquet。

## Required Implementation

CSV：
- quote-aware state machine
- schema inference
- boundary-aware parallel scan

JSON：
- structural tokenizer
- Typed Decoder
- ProjectedPathSet
- LIST/STRUCT
- schema inference

NDJSON：
- range parallel scan when safe

Compression：
- codec abstraction
- gzip
- zstd sequential baseline
- capability composition

SFTP：
- VFS
- bounded session pool
- host-key verification
- poison/discard logic
- retry/cancel

## Deliverables

- P3-D1 CSV compliance corpus
- P3-D2 parallel CSV verification
- P3-D3 CSV schema inspection
- P3-D4 NDJSON parallel verification
- P3-D5 JSON typed decoder
- P3-D6 compression capability demo
- P3-D7 SFTP query demo
- P3-D8 SFTP pool verification
- P3-D9 host-key security test
- P3-D10 weak-network test

## Compression Rule

```text
Effective Scan Capability
=
VFS Capability
× Compression Capability
× Format Capability
```

普通 gzip：
```text
Sequential / Non-splittable
```

## Mandatory Equality Checks

- parallel CSV == sequential CSV
- parallel NDJSON == sequential NDJSON
- compressed logical result == uncompressed logical result

## Failure Gate

- malformed CSV
- malformed JSON
- gzip corruption
- SFTP connection reset
- wrong host key
- cancel

## Acceptance

`artifacts/phase-3/acceptance-report.md`

milestone：
```text
v0.3-remote-scan
```
