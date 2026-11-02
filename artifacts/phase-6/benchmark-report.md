# Phase 6 Performance Baseline

Date: 2026-11-02
Candidate commit: `fd737c3c6f45f47fe1ba1735d09658e6c1120bc1`
Workflow run: `41430739607`
Runner CPU: Intel(R) Xeon(R) 6975P-C
OS/Arch: linux/amd64
Go: 1.22.12

These measurements are machine-local release baselines, not cross-machine absolute SLOs.

| Benchmark | ns/op | B/op | allocs/op | Notes |
| --- | ---: | ---: | ---: | --- |
| BenchmarkNativeQueryChunks | 36,157 | 18,152 | 172 | Native Go API |
| BenchmarkRowGroupScannerInt32_1024 | 40,074 | 40,088 | 1,069 | 102.21 MB/s |
| BenchmarkInnerHashJoin2048 | 1,586,752 | 507,584 | 146 | in-memory join |
| BenchmarkExternalHashJoinForcedSpill | 3,495,789 | 1,344,713 | 6,178 | forced spill |
| BenchmarkHashAggregateBigInt | 594,396 | 218,094 | 61 | in-memory aggregate |
| BenchmarkExternalAggregateForcedSpill | 2,236,079 | 951,168 | 3,242 | forced spill |
| BenchmarkExternalSortForcedSpill | 6,384,172 | 682,573 | 4,855 | forced spill |
| BenchmarkTopN | 2,124,054 | 275,193 | 4,366 | Top-N path |
| BenchmarkParseQueryPath | 37.68 | 32 | 1 | HTTP query path parsing |
| BenchmarkResultCursorPushNext | 78.93 | 0 | 0 | result backpressure path |
| BenchmarkWorkerCountersAddChunk | 0.3908 | 0 | 0 | worker-local profiler counter |
| BenchmarkRuntimeMetricsMergeWorker | 18.46 | 0 | 0 | profiler merge |

All benchmark commands in `scripts/phase6-release.sh` completed successfully in the release gate. Performance acceptance is therefore baseline establishment and regression observability, consistent with the frozen release contract; no new absolute latency/throughput requirement was invented in Feature Freeze.
