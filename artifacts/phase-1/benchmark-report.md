# Phase 1 Benchmark Report

Status: **PASS — reproducible baseline established**

## Purpose

Phase 1 performance evidence shows that the controlled Parquet reader has a repeatable hot-path baseline and that projection/pruning reduce unnecessary remote work. The Phase 1 specification does not define a numeric throughput SLO, so this report records regression evidence rather than inventing a pass/fail performance threshold.

## Final Acceptance Baseline

Final GitHub Actions run `41259459358`, verify job `124709511245`, executed:

```text
go test ./internal/format/parquet -run '^$' -bench '^BenchmarkRowGroupScannerInt32_1024$' -benchmem -count=3
```

Environment:

- OS: Ubuntu 24.04.5 / linux-amd64
- Go: 1.22.12
- CPU: AMD EPYC 9V74 80-Core Processor
- benchmark: `BenchmarkRowGroupScannerInt32_1024`
- workload: one 1024-row INT32 Parquet row group through `RowGroupScanner`, exact page read, direct Own Vector decode, chunk release, scanner close, and managed-memory-zero assertion

Measured results:

| Run | ns/op | MB/s | B/op | allocs/op |
| ---: | ---: | ---: | ---: | ---: |
| 1 | 41,342 | 99.08 | 40,088 | 1,069 |
| 2 | 41,497 | 98.71 | 40,088 | 1,069 |
| 3 | 41,206 | 99.40 | 40,088 | 1,069 |
| mean | 41,348 | 99.06 | 40,088 | 1,069 |

The raw benchmark output was uploaded by the same workflow as artifact `phase1-benchmark-evidence` (artifact ID `13064741576`).

This is a CPU/allocation baseline over a deterministic in-memory VFS fixture. It intentionally does **not** claim S3 network throughput; network performance depends on endpoint/region/runtime conditions and no Phase 1 network-throughput threshold is frozen.

## Work-Reduction Evidence

- Projection planning selects only requested ColumnChunks.
- `TestRowGroupScannerProjectionAvoidsUnprojectedRanges` verifies an unprojected column performs no range read.
- RowGroup pruning skips only `ALWAYS_FALSE` groups.
- `TestRowGroupScannerAlwaysFalsePrunesBeforeIO` verifies an always-false row group performs zero I/O.
- Footer discovery uses the mandatory exact sequence: last 8 bytes, then the exact metadata range; whole-file fallback is not used.
- RowGroupScanner/profile metrics report planned, transferred, and useful bytes separately together with request/page/row counters.
- RangePlanner supports sorting/coalescing without changing logical sub-range identity.
- RemoteClientManager bounds client reuse and avoids client creation per request.
- Decimal128 is decoded into fixed-width Own Vector storage; its implementation does not alter the benchmark's 40,088 B/op / 1,069 allocs/op controlled INT32 baseline.

These tests provide deterministic evidence that projection/pruning reduce work even though the benchmark itself is a single-column controlled-decode baseline.

## Same-Run Verification

The same verify job completed successfully before the benchmark:

- `go test ./...` — PASS
- `go run ./tools/architecture-check .` — PASS
- `go test -race ./...` — PASS
- benchmark command above — PASS

The benchmark also asserts the per-iteration memory scope returns to zero after scanner/chunk cleanup. The dependent pinned four-engine interop job `124709838587` also completed successfully.

## Gate Result

The Phase 1 performance/benchmark evidence is complete as a reproducible baseline. No unsupported numeric SLO is claimed.

Overall Benchmark Evidence: **PASS**.
