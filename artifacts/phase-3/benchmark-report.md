# Phase 3 Benchmark Report

Date: 2026-11-01
Phase: 3 — CSV / JSON / NDJSON + Compression + SFTP
Status: ACCEPTED BASELINES

## Environment

- Go: `go1.23.2 linux/amd64`
- CPU: AMD EPYC 9V74 80-Core Processor
- Benchmarks are local phase-acceptance baselines, not cross-machine performance targets.

## Final acceptance run

The benchmarks below were executed by `./scripts/acceptance.sh 3` after the full-repository test/race/vet/architecture Gate passed.

### CSV hot paths

```text
BenchmarkSummarizeChunk-5            128     917952 ns/op     66.93 MB/s      773 B/op       8 allocs/op
BenchmarkDecodeChunk-5               187     630467 ns/op     84.46 MB/s    88251 B/op      35 allocs/op
BenchmarkMachineStepUnquoted-5   1996249         59.47 ns/op  386.78 MB/s        0 B/op       0 allocs/op
BenchmarkMachineStepQuoted-5     1000000        119.0 ns/op   378.31 MB/s        0 B/op       0 allocs/op
```

The CSV boundary benchmark exercises the same lexical state machine used by parsing; there is no separate heuristic boundary parser.

### JSON typed decode

```text
BenchmarkTypedDecode1024-5    105    1132502 ns/op    117542 B/op    4149 allocs/op
```

This covers structural-tokenizer + typed-decoder output into the engine Vector/DataChunk ABI, including nested LIST/STRUCT handling.

### Compression

```text
BenchmarkGzipDecode-5    1480     82540 ns/op    1364.67 MB/s    385442 B/op    13 allocs/op
BenchmarkZstdDecode-5     322    369990 ns/op     166.06 MB/s    164545 B/op    10 allocs/op
```

Both codecs advertise sequential/non-splittable compression capability, so these measure decode throughput rather than parallel range scanning.

### SFTP session pool

```text
BenchmarkPoolAcquireRelease-5    1367839    95.83 ns/op    112 B/op    1 allocs/op
```

The pool remains bounded; this benchmark does not create unbounded sessions or goroutines.

## Performance Gate

All performance-critical Phase 3 paths have recorded baselines. No Phase 3 performance Gate failed during the final acceptance run.
