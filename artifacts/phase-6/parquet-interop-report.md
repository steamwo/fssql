# Phase 6 Parquet Interoperability Report

Date: 2026-11-02
Candidate commit: `fd737c3c6f45f47fe1ba1735d09658e6c1120bc1`
Workflow run: `41430739607`

Pinned readers/writers in the release workflow:

- DuckDB 2.0.0
- PyArrow 26.0.0
- Polars 2.1.0
- PySpark 4.3.0

## Canonical corpus

All four readers returned the expected row counts for the canonical corpus:

| Corpus | Expected rows | DuckDB | PyArrow | Polars | Spark |
| --- | ---: | ---: | ---: | ---: | ---: |
| decimal128.parquet | 2 | 2 | 2 | 2 | 2 |
| flat_dictionary.parquet | 4 | 4 | 4 | 4 | 4 |
| nested.parquet | 4 | 4 | 4 | 4 | 4 |
| hive partitioned dataset | 4 | 4 | 4 | 4 | 4 |

## Producer × reader matrix

Each producer wrote three rows, and every reader returned three rows:

| Producer | DuckDB reader | PyArrow reader | Polars reader | Spark reader |
| --- | ---: | ---: | ---: | ---: |
| DuckDB | 3 | 3 | 3 | 3 |
| PyArrow | 3 | 3 | 3 | 3 |
| Polars | 3 | 3 | 3 | 3 |
| Spark | 3 | 3 | 3 | 3 |

This verifies the complete 4-producer × 4-reader matrix in addition to the canonical corpus.

## fssql writer interoperability

`test/interop/phase4/run.py` generated `fssql.parquet` with three rows:

```text
(1, "alpha")
(2, "beta")
(3, NULL)
```

DuckDB and PyArrow both read the same three rows successfully.

## Result

Parquet interoperability required by the v1 release contract is verified. The Phase 6 release runner also now enforces JSON-only stdout for the Phase 1 evidence stream, preventing diagnostics from corrupting machine-readable release evidence.
