# Phase 0 Benchmark Baseline

Environment: `go version go1.23.2 linux/amd64`, linux/amd64, INTEL(R) XEON(R) PLATINUM 8573C. Each benchmark was run three times. Values below are medians.

| Benchmark | ns/op | MB/s | B/op | allocs/op | rows/s |
|---|---:|---:|---:|---:|---:|
| BenchmarkNumericAdd2048 | 161144.00 | 101.67 | 25880 | 14 | 12709129.722484238 |
| BenchmarkVectorHashInt64 | 56541.00 | 289.77 | 0 | 0 | 36221502.98013831 |
| BenchmarkHashTableLookup | 38.08 | — | 0 | 0 | — |
| BenchmarkVectorHashVarchar | 192304.00 | — | 0 | 0 | 10649804.476245943 |
| BenchmarkPushPipeline100K | 5517261.00 | — | 830695 | 377 | 18124935.543197975 |

## Interpretation

- `BenchmarkNumericAdd2048` operates on a 2048-row chunk. The 14 allocations are chunk/expression-control allocations and do not scale per row; `TestNumericKernelAllocationsDoNotScalePerRow` verifies this property.
- Int64 and VARCHAR VectorHash and primitive HashTable lookup report 0 allocations/op.
- The Push Pipeline baseline processes 100,000 rows with four workers and retains bounded managed-memory ownership.
- No DuckDB executable or Python DuckDB module is available in this isolated execution environment. Phase 0 has no SQL surface, so there is no Stable SQL differential workload yet. No known quantity-order regression is being suppressed; this baseline is the Phase 0 regression reference.
