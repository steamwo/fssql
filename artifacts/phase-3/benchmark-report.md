# Phase 3 Benchmark Report

Date: 2026-11-01
Phase: 3 — CSV / JSON / NDJSON + Compression + SFTP
Status: BASELINES CAPTURED

## Environment

- Go: `go1.23.2 linux/amd64`
- Kernel: Linux 6.18.57 x86_64
- Benchmarks are local development baselines, not cross-machine performance targets.

## CSV hot paths

Task-local benchmark baselines captured when the exact CSV implementation was completed:

| Benchmark | Baseline |
|---|---:|
| quote-aware lexer, unquoted | ~382 MB/s, 0 alloc/op |
| quote-aware lexer, quoted | ~373 MB/s, 0 alloc/op |
| typed sequential decode | ~81 MB/s |
| boundary transition summary | ~66.5 MB/s |

The CSV boundary benchmark exercises the same lexical state machine used by parsing. No separate heuristic boundary parser is benchmarked because none exists.

## JSON typed decode

Latest local run:

```text
BenchmarkTypedDecode1024-5    246    1106347 ns/op    117292 B/op    4149 allocs/op
```

This is the Phase 3 baseline for structural-tokenizer + typed-decoder output into the engine Vector/DataChunk ABI, including nested LIST/STRUCT handling.

## Compression

Latest local run:

```text
BenchmarkGzipDecode-5    2113    118681 ns/op    949.10 MB/s    385392 B/op    13 allocs/op
BenchmarkZstdDecode-5     500    487906 ns/op    125.94 MB/s    164425 B/op    10 allocs/op
```

Both codecs advertise sequential/non-splittable compression capability. The benchmark therefore measures decode throughput, not parallel range scanning.

## SFTP session pool

Latest local run:

```text
BenchmarkPoolAcquireRelease-5    2432450    97.05 ns/op    112 B/op    1 allocs/op
```

The pool remains bounded; the benchmark does not create unbounded sessions or goroutines.

## Performance Gate

Phase 3 performance-critical paths have benchmark baselines and no Phase 3 regression threshold was violated during local implementation testing. Final phase acceptance still depends on the full-repository Gate recorded in `acceptance-report.md`.
