# Phase 4 Benchmark Report

Date: 2026-11-01
Status: **PRELIMINARY — final acceptance benchmark run pending**

Phase 4 benchmarks are machine-local baselines, not cross-machine absolute thresholds.

## Recorded development baselines

| Benchmark | Development baseline | Notes |
| --- | ---: | --- |
| `BenchmarkWriterPoolAcquireRelease` | ~299 ns/op, 3 allocs/op | Fixed-capacity writer slot acquire/terminal release path |
| `BenchmarkPartitionRouting2048x64` | ~471 µs/op, ~177 KB/op, ~8714 allocs/op | 2048-row DataChunk routed to 64 partitions; includes route/path bookkeeping |
| `BenchmarkParquetWriterInteger1024` | pending final run | Memory-governed INTEGER Parquet PLAIN writer |

The first two numbers were captured during targeted local development runs and are retained only as comparison baselines. They must be replaced or supplemented by the final same-commit acceptance output.

## Final acceptance command

```text
go test -run '^$' -bench 'Benchmark(WriterPoolAcquireRelease|PartitionRouting2048x64|ParquetWriterInteger1024)$' -benchmem -benchtime=100ms ./internal/export
```

## Performance architecture checks

- Writer count is bounded by WriterPool slots, not partition cardinality.
- Each writer reserves `ExportBuffer` memory before construction.
- Partition row selection uses DICTIONARY Vector rather than copying payload columns.
- Parquet RowGroup, Page and S3 Multipart boundaries remain independent.
- No benchmark result authorizes unbounded Go heap buffers or per-partition goroutines.
