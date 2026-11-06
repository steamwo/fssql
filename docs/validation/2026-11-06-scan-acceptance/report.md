# CSV inference, Parquet DECIMAL and scan acceptance — 2026-11-06

## Scope and status

CSV inference now recognizes ISO DATE, TIMESTAMP and TIMESTAMPTZ. Date-only and
naive timestamp values in one column promote to TIMESTAMP; execution accepts
both space and `T` separators and date-only values in that promoted column.
Mixed zoned/unzoned input conservatively remains VARCHAR. Custom date formats,
`auto_type_candidates`, TIME and general DuckDB inference equivalence are not
implemented. Fractional numeric values still default to DOUBLE; exact DECIMAL
is available through explicit `columns`.
[DuckDB's candidate-type documentation](https://duckdb.org/docs/2.0/data/csv/auto_detection)
distinguishes optional DECIMAL candidates from default inference.

Parquet DECIMAL precision ≤9 now uses INT32 in both schema and page data;
precision 10–18 uses INT64 and 19–38 uses FIXED_LEN_BYTE_ARRAY(16). Native
roundtrips at precisions 9/10/18/19/38 preserve precision-boundary negative,
positive and NULL values. DuckDB 2.0.0 and PyArrow independently read those
generated files with exact values and the expected physical types. The
[independent interop record](decimal-interop.json) includes tool versions,
file hashes, physical types and values.

SQL coverage remains a subset. `upper`, `abs`, `round`, `regexp_*`, `date_part`,
`try_cast` and `string_split` remain unsupported. This round does not implement
those functions or establish arbitrary DuckDB SQL compatibility; see
SQL-FUNCTION-001 and CSV-INFER-001 in `compatibility/deviations.yaml`.

## Original-size generated fixtures over loopback HTTP

The existing deterministic generator produced 1,000,000 rows with exactly the
original reported source sizes: Parquet 299,960,006 B, CSV 283,038,719 B and
NDJSON 344,274,015 B. A streaming loopback HTTP server serves actual file-backed
Range responses. Queries use the production S3 driver and its actual HTTP body
byte meter; no in-memory substitute or optimizer-plan estimate is used.

Every result matches an independently executed DuckDB 2.0.0 row count and full
result digest. The following are one-run correctness/I/O observations, not
comparative latency benchmarks.

| Case | Returned rows | HTTP body bytes | Managed peak bytes |
| --- | ---: | ---: | ---: |
| Parquet narrow projection | 1,000,000 | 24,233,731 | 697,376 |
| Parquet wide projection | 1,000,000 | 268,601,446 | 2,312,470 |
| Parquet LIMIT 100 | 100 | 1,267,162 | 2,296,886 |
| Parquet range filter | 1,000 | 360,411 | 494,168 |
| Parquet empty predicate | 0 | 36,043 | 36,035 |
| CSV, 64 MiB | 1,000 | 285,178,981 | 1,097,924 |
| CSV, 256 MiB | 1,000 | 285,178,981 | 1,097,924 |
| NDJSON, 64 MiB | 1,000 | 346,813,418 | 1,081,712 |
| NDJSON, 256 MiB | 1,000 | 346,813,418 | 1,081,712 |

The text queries filter on the final 1,000 IDs and consume the whole source.
Managed memory and temporary bytes return to zero for every case. All GETs
use Range and receive 206; there are no full GETs. HTTP bytes include metadata,
inference and overlapping range reads, so may exceed source size. Managed peak
is distinct from RSS; sampled RSS is retained in the raw results.

Reproduce with dependencies from `test/integration/requirements.txt`, Go on
PATH, and `python test/integration/retest_scan_acceptance.py`. This creates
fixtures under `.cache/scan-acceptance` without uploading objects. The script
checks complete case coverage, digests, budgets, cleanup, Range responses and
physical byte reduction thresholds. Raw evidence:
[manifest](loopback-manifest.json), [results](loopback-results.jsonl).

## Supplied production MinIO CSV

Endpoint: `http://10.34.20.138:9003`; object: `s3://test-bucket/1m.csv`.
Source size: 228,978,009 B. The object was downloaded read-only for independent
DuckDB reference calculation; fssql then reads the original S3 object through
the production adapter. Credentials are not included in this report or manifests.

| Case | Budget | Rows | HTTP body bytes | Managed peak bytes |
| --- | ---: | ---: | ---: | ---: |
| Numeric/string projection | 64 MiB | 1,000,000 | 231,101,325 | 1,131,677 |
| DATE/TIMESTAMP → VARCHAR | 64 MiB | 1,000,000 | 231,101,325 | 1,369,329 |
| Numeric/string projection | 256 MiB | 1,000,000 | 231,101,325 | 1,131,677 |
| DATE/TIMESTAMP → VARCHAR | 256 MiB | 1,000,000 | 231,101,325 | 1,369,329 |

All four complete result digests match DuckDB 2.0.0, with zero managed/temp bytes
after execution, no full GETs and no non-206 Range responses. DuckDB assigns
`column00` to the first of 24 unnamed columns; reference SQL maps fssql's
`column0` to that name. Temporal reference values are formatted through SQL
CAST to VARCHAR on both engines. Raw evidence:
[manifest](remote-csv-manifest.json), [results](remote-csv-results.jsonl).

To replay the retained remote manifest, set `FSSQL_MINIO_ENDPOINT`,
`FSSQL_MINIO_BUCKET=test-bucket`, `FSSQL_LARGE_READONLY_BUCKET=test-bucket`,
`FSSQL_MINIO_ACCESS_KEY`, `FSSQL_MINIO_SECRET_KEY`, `FSSQL_LARGE_MANIFEST` to
the retained manifest and `FSSQL_LARGE_REPORT` to a writable output file, then
run `go test -v -count=1 -timeout=30m -run '^TestMinIOLargeMatrix$' ./pkg/gosql`.
The bucket exception only permits SELECT compare scenes; write scenes still
require the dedicated test bucket prefix.

## Remaining acceptance gates

PERF-001 remains open: original-size loopback physical I/O passes, but the
production remote Parquet physical-I/O matrix has not been re-run. The other
existing 230,358,942-byte Parquet object has 15,000,000 rows, rather than the
original million-row fixture, and was not counted as that acceptance.

MEM-001 remains open: original-size loopback CSV/NDJSON and the supplied remote
CSV pass, but the latter is below 256 MiB. Original-size and replacement
>budget remote text fixtures, including NDJSON, remain unverified. This run
does not certify the broader 512 MiB/1 GiB text profiles in the testing standard.

The previous endpoint `192.168.233.106:9000` was unreachable. No objects were
uploaded, changed or deleted on either MinIO service.
