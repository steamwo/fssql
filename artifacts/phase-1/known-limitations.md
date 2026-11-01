# Phase 1 Known Limitations

Status: **ACTIVE — Phase 1 remains IN_PROGRESS**

## Acceptance Blockers

1. **DECIMAL(19..38)** is not implemented. `core/types` maps these widths to PhysicalInt128, but the frozen Own Vector ABI has no canonical Int128 representation/accessors. ADR-0001 is PROPOSED and must be approved or otherwise resolved before implementation.
2. **DuckDB/PyArrow/Polars/Spark interoperability has not been executed** in the current environment. The reproducible runner exists, but all four Python dependencies are absent.
3. **Repository-wide test/race/architecture verification has not executed** on the latest branch because GitHub Actions is not assigning a runner; jobs terminate before any steps run. The connected VM cannot clone the private repository as a substitute.

## Supported Nested Scope

The controlled reader supports the Phase 1 standard paths implemented in this branch:

- primitive scalar columns;
- standard three-level LIST with primitive element;
- non-repeated STRUCT with primitive direct children;
- standard three-level LIST whose element is a non-repeated STRUCT with primitive direct children.

It intentionally rejects ambiguous/legacy LIST layouts and deeper arbitrary recursive combinations that have not been defined by a frozen contract. In particular, explicit dotted projection of an individual leaf inside a LIST<STRUCT> is not a supported output-shape contract; projecting the top-level LIST preserves the nested value.

## DECIMAL Scope

DECIMAL widths 1..18 use Decimal64 scaled signed integer storage and support Parquet INT32, INT64, BYTE_ARRAY, and FIXED_LEN_BYTE_ARRAY representations with checked conversion. Widths 19..38 are rejected explicitly; values are not truncated, coerced to strings, or approximated.

## Codec / Parquet Surface

Phase 1 implements the codecs/encodings exercised by the controlled reader and corpus definition, including uncompressed/Snappy/Gzip, PLAIN and dictionary paths, RLE/bit-packed levels, and Page V1/V2 basics. It is not a claim of full Parquet-format coverage for every encoding, compression codec, legacy annotation, encryption feature, or page-index feature.

## CLI Scope

`cmd/phase1-parquet` is the Phase 1 S3/Parquet verification CLI, not the final SQL frontend. It exposes source `describe`, `explain`, `analyze`, and scan paths but does not introduce Phase 2 relational operators.

## Environment Limitations

- No PyArrow, DuckDB, Polars, or PySpark are installed in the connected VM.
- The VM has no usable private-repository clone path/network credentials.
- Current GitHub Actions jobs do not receive a runner.

These limitations are recorded as blockers rather than converted into PASS results.

## Phase Boundary

No external join, aggregate spill, sort spill, or other Phase 2 functionality is included. Phase 2 must remain LOCKED until Phase 1 acceptance is changed from BLOCKED to ACCEPTED with complete evidence.
