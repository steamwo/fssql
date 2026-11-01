# Phase 2 Benchmark Report

Date: 2026-11-01
Environment: linux/amd64, Go 1.23.2, AMD EPYC 9V74

All benchmarks are Phase 2 baselines, not release SLOs. Final local gate uses `-benchmem -benchtime=100ms`.

| Path | Result | Allocation notes |
|---|---:|---:|
| Temporary quota reserve/release | 46.06 ns/op | 32 B/op, 1 alloc/op |
| RLE spill codec, 64 KiB | 37.49 us/op, 1.75 GB/s | 0 B/op, 0 allocs/op |
| Spill page round-trip, 64 KiB | 50.10 us/op, 1.31 GB/s | 66.5 KiB/op, 17 allocs/op |
| Hash BIGINT+VARCHAR chunk | 145.26 us/op, 423 MB/s | 0 B/op, 0 allocs/op |
| Partition histogram AddHash | 11.58 ns/op | 0 B/op, 0 allocs/op |
| INNER hash join, 2048 rows | 1.210 ms/op | 507.6 KiB/op, 146 allocs/op |
| Forced-spill external hash join | 3.090 ms/op | 1.345 MB/op, 6178 allocs/op |
| Hash aggregate BIGINT | 450.75 us/op | 218.1 KiB/op, 61 allocs/op |
| Forced-spill aggregate | 2.046 ms/op | 951.3 KiB/op, 3242 allocs/op |
| Forced-spill DISTINCT | 47.27 ms/op | 11.18 MB/op, 40893 allocs/op |
| Forced-spill external sort | 4.262 ms/op | 682.7 KiB/op, 4855 allocs/op |
| TopN 2048 -> 100 | 1.995 ms/op | 275.3 KiB/op, 4366 allocs/op |

Performance-critical primitives (`VectorHash`, radix histogram, RLE) have zero per-row/per-item heap allocation. Operator-level allocations include managed-buffer/control-object setup and temporary-file operations and are recorded as a regression baseline for later phases.

Commands are encoded in `scripts/acceptance.sh 2`; the Phase 2 workflow reruns the baseline on the committed full repository.
