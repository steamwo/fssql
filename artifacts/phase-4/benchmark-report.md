# Phase 4 Benchmark Report

Date: 2026-11-01
Status: **PASS — acceptance baseline recorded**

Phase 4 benchmarks are machine-local baselines, not cross-machine absolute thresholds.

## Acceptance environment

- Workflow run: `41302222162`
- Ubuntu 24.04 / linux amd64
- Go 1.22.12
- CPU: AMD EPYC 7763 64-Core Processor
- Benchmark time: 100 ms

## Acceptance baselines

| Benchmark | ns/op | B/op | allocs/op |
| --- | ---: | ---: | ---: |
| `BenchmarkParquetWriterInteger1024-2` | 125,958 | 286,552 | 86 |
| `BenchmarkPartitionRouting2048x64-2` | 696,178 | 207,641 | 10,762 |
| `BenchmarkWriterPoolAcquireRelease-2` | 224.0 | 136 | 3 |

The benchmark command completed successfully:

```text
go test -run '^$' -bench 'Benchmark(WriterPoolAcquireRelease|PartitionRouting2048x64|ParquetWriterInteger1024)$' -benchmem -benchtime=100ms ./internal/export
```

## Interpretation

- WriterPool remains a small bounded control-path operation.
- Partition routing is the highest-allocation path in this Phase 4 baseline and should remain visible to later performance regression work; this report does not introduce a new Phase 5 optimization.
- Parquet writer memory remains governed through MemoryManager/BufferManager; RowGroup, Page and Multipart boundaries remain independent.
- No benchmark result authorizes unbounded Go heap buffers, unbounded open writers, or per-partition goroutines.
