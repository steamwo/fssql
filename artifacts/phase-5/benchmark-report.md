# Phase 5 Benchmark Report

Date: 2026-11-02
Status: **PASS — remediated acceptance baseline recorded**

Phase 5 benchmarks are machine-local regression baselines, not cross-machine absolute thresholds.

## Acceptance environment

- Acceptance candidate: `114788a4ef6097008f4a8437ced71a821f7af260`
- Workflow run: `41424206444`
- Ubuntu 24.04.5 / linux amd64
- Go 1.22.12
- CPU: AMD EPYC 7763 64-Core Processor
- Benchmark time: 100 ms

## Acceptance baselines

| Package / benchmark | ns/op | B/op | allocs/op | Additional |
| --- | ---: | ---: | ---: | --- |
| `pkg/gosql` `BenchmarkNativeQueryChunks-2` | 131,483 | 18,154 | 172 | public native query/chunk path |
| server `BenchmarkParseQueryPath-2` | 59.03 | 32 | 1 | control-path parser |
| server `BenchmarkResultCursorPushNext-2` | 79.54 | 0 | 0 | bounded Push→Next→ReleaseChunk |
| profile `BenchmarkWorkerCountersAddChunk-2` | 0.6261 | 0 | 0 | worker-local hot counter |
| profile `BenchmarkRuntimeMetricsMergeWorker-2` | 10.01 | 0 | 0 | batch metric merge |
| FTP `BenchmarkPoolAcquireRelease-2` | 59.07 | 96 | 1 | bounded session pool |
| FTP `BenchmarkDriverReadRange64K-2` | 10,275 | 65,824 | 5 | 6,378.35 MB/s |
| FTP `BenchmarkParseMLSDLine-2` | 482.7 | 160 | 4 | listing control path |
| WebDAV `BenchmarkParsePropfind256-2` | 3,640,349 | 964,173 | 23,132 | 21.28 MB/s |
| WebDAV `BenchmarkReadRange64K-2` | 385,616 | 85,145 | 276 | 169.95 MB/s |

## Interpretation

- The remediated public native API now has an explicit Phase 5 baseline. Its allocations include query planning/control objects and public result wrapping; this benchmark is a regression reference, not permission to bypass Managed Buffer ownership.
- ResultCursor remains **0 B/op, 0 allocs/op** on the core Push→Next→ReleaseChunk handoff while bounding queued + consumer-held ownership.
- Profiling worker-local counters remain allocation-free on the measured hot paths.
- FTP/WebDAV figures are local in-process baselines rather than network SLAs.
- WebDAV PROPFIND XML parsing remains allocation-heavy and stays documented as a control/metadata-path limitation.
- No benchmark result authorizes unbounded result buffering, remote sessions, goroutines, unmanaged large allocations or per-row exporter calls.
