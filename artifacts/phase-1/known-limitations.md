# Phase 1 Known Limitations

Status: **ACCEPTED — no Phase 1 acceptance blocker remains**

Phase 1 is accepted. ADR-0001 is accepted and DECIMAL(19..38) is implemented through the canonical PhysicalInt128 Own Vector representation. The limitations below are intentional Phase 1 scope boundaries, not failed acceptance criteria.

## Supported Nested Scope

The controlled reader supports the Phase 1 standard paths implemented in this branch:

- primitive scalar columns;
- standard three-level LIST with primitive element;
- non-repeated STRUCT with primitive direct children;
- standard three-level LIST whose element is a non-repeated STRUCT with primitive direct children.

It intentionally rejects ambiguous/legacy LIST layouts and deeper arbitrary recursive combinations that have not been defined by a frozen contract. Explicit dotted projection of an individual leaf inside a LIST<STRUCT> is not a supported output-shape contract; projecting the top-level LIST preserves the nested value.

## DECIMAL Scope

DECIMAL widths 1..18 use Decimal64 scaled signed integer storage and support Parquet INT32, INT64, BYTE_ARRAY, and FIXED_LEN_BYTE_ARRAY representations with checked conversion.

DECIMAL widths 19..38 use the ADR-0001 `types.Int128` / PhysicalInt128 representation. The controlled reader accepts BYTE_ARRAY and FIXED_LEN_BYTE_ARRAY big-endian two's-complement unscaled integers, sign-extends into Int128, preserves declared scale, and rejects signed-128 or declared-precision overflow. Dictionary copy and Vector serialization preserve Decimal128 without string coercion or approximation.

Parquet INT32 and INT64 DECIMAL encodings remain bounded by the precision those physical types can represent; incompatible declared high precision is rejected explicitly rather than reinterpreted.

## Codec / Parquet Surface

Phase 1 implements the codecs/encodings exercised by the controlled reader and corpus, including uncompressed/Snappy/Gzip, PLAIN and dictionary paths, RLE/bit-packed levels, and Page V1/V2 basics. It is not a claim of full Parquet-format coverage for every encoding, compression codec, legacy annotation, encryption feature, bloom filter, or page-index feature.

## Execution / CLI Scope

`cmd/phase1-parquet` is the Phase 1 S3/Parquet verification CLI, not the final SQL frontend. It exposes source `describe`, `explain`, `analyze`, and scan paths but does not introduce Phase 2 relational operators.

Phase 1 multi-file planning/schema resolution, Hive discovery, SourceSnapshot and union-by-name behavior are implemented and tested. The phase does not claim a Phase 2-style parallel relational execution engine across files.

## Cache Scope

MetadataCache is a bounded in-process cache keyed by stable object identity. Phase 1 does not define a persistent compressed metadata cache format on disk.

## Verification Status

Final GitHub Actions run `41259459358` provides repository-wide Phase 1 evidence:

- `go test ./...` — PASS;
- `go run ./tools/architecture-check .` — PASS;
- `go test -race ./...` — PASS;
- controlled Parquet benchmark — PASS;
- pinned four-engine interoperability runner — PASS.

Corresponding Phase 0 compatibility run `41259459399` also passes ordinary, race, and architecture verification.

## Phase Boundary

No external join, aggregate spill, sort spill, or other Phase 2 functionality is included in this Phase 1 implementation. Phase 2 may begin only after the machine state records Phase 1 as ACCEPTED; that state transition does not retroactively add Phase 2 code to this branch.
