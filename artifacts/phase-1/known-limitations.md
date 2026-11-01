# Phase 1 Known Limitations

Status: **ACTIVE — Phase 1 blocked only by ADR-0001**

## Acceptance Blocker

1. **DECIMAL(19..38)** is not implemented. `core/types` maps these widths to PhysicalInt128, but the frozen Own Vector ABI has no approved canonical Int128 representation/accessors. ADR-0001 is PROPOSED and must be explicitly approved or otherwise resolved before implementation.

Repository-wide unit/race/architecture verification and the DuckDB/PyArrow/Polars/Spark interoperability matrix now pass; they are no longer acceptance blockers.

## Supported Nested Scope

The controlled reader supports the Phase 1 standard paths implemented in this branch:

- primitive scalar columns;
- standard three-level LIST with primitive element;
- non-repeated STRUCT with primitive direct children;
- standard three-level LIST whose element is a non-repeated STRUCT with primitive direct children.

It intentionally rejects ambiguous/legacy LIST layouts and deeper arbitrary recursive combinations that have not been defined by a frozen contract. In particular, explicit dotted projection of an individual leaf inside a LIST<STRUCT> is not a supported output-shape contract; projecting the top-level LIST preserves the nested value.

## DECIMAL Scope

DECIMAL widths 1..18 use Decimal64 scaled signed integer storage and support Parquet INT32, INT64, BYTE_ARRAY, and FIXED_LEN_BYTE_ARRAY representations with checked conversion. Widths 19..38 are rejected explicitly; values are not truncated, coerced to strings, or approximated.

The interoperability corpus includes a Decimal128 file and confirms that PyArrow, DuckDB, Polars, and Spark agree on its row count. That external compatibility evidence does not substitute for the missing Own Vector Int128 ABI inside fssql.

## Codec / Parquet Surface

Phase 1 implements the codecs/encodings exercised by the controlled reader and corpus, including uncompressed/Snappy/Gzip, PLAIN and dictionary paths, RLE/bit-packed levels, and Page V1/V2 basics. It is not a claim of full Parquet-format coverage for every encoding, compression codec, legacy annotation, encryption feature, bloom filter, or page-index feature.

## Execution / CLI Scope

`cmd/phase1-parquet` is the Phase 1 S3/Parquet verification CLI, not the final SQL frontend. It exposes source `describe`, `explain`, `analyze`, and scan paths but does not introduce Phase 2 relational operators.

Phase 1 multi-file planning/schema resolution, Hive discovery, SourceSnapshot and union-by-name behavior are implemented and tested. The phase does not claim a Phase 2-style parallel relational execution engine across files.

## Cache Scope

MetadataCache is a bounded in-process cache keyed by stable object identity. Phase 1 does not define a persistent compressed metadata cache format on disk.

## Verification Status

GitHub Actions run `41256775324` provides repository-wide evidence:

- `go test ./...` — PASS;
- `go run ./tools/architecture-check .` — PASS;
- `go test -race ./...` — PASS;
- four-engine interoperability runner — PASS.

Exact interoperability engine versions/results are recorded in `artifacts/phase-1/interop-output.json`.

## Phase Boundary

No external join, aggregate spill, sort spill, or other Phase 2 functionality is included. Phase 2 must remain LOCKED until ADR-0001 is resolved, DECIMAL(19..38) is implemented according to the approved design, final Phase 1 gates pass again, and Phase 1 acceptance is changed to ACCEPTED.
