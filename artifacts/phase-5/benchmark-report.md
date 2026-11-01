# Phase 5 Benchmark Report

Date: 2026-11-01
Status: **PASS — acceptance baseline recorded**

Phase 5 benchmarks are machine-local baselines, not cross-machine absolute thresholds.

## Acceptance environment

- Workflow run: `41318791929`
- Ubuntu 24.04.5 / linux amd64
- Go 1.22.12
- CPU: AMD EPYC 7763 64-Core Processor
- Benchmark time: 100 ms

## Acceptance baselines

| Package / benchmark | ns/op | B/op | allocs/op | Additional |
| --- | ---: | ---: | ---: | --- |
| server `BenchmarkParseQueryPath-2` | 60.63 | 32 | 1 | control-path parser |
| server `BenchmarkResultCursorPushNext-2` | 79.35 | 0 | 0 | bounded Push→Next→ReleaseChunk |
| profile `BenchmarkWorkerCountersAddChunk-2` | 0.6310 | 0 | 0 | worker-local hot counter |
| profile `BenchmarkRuntimeMetricsMergeWorker-2` | 10.00 | 0 | 0 | batch metric merge |
| FTP `BenchmarkPoolAcquireRelease-2` | 58.41 | 96 | 1 | bounded session pool |
| FTP `BenchmarkDriverReadRange64K-2` | 12,375 | 65,824 | 5 | 5,296.03 MB/s |
| FTP `BenchmarkParseMLSDLine-2` | 437.7 | 160 | 4 | listing control path |
| WebDAV `BenchmarkParsePropfind256-2` | 3,698,863 | 964,175 | 23,132 | 20.94 MB/s |
| WebDAV `BenchmarkReadRange64K-2` | 395,164 | 85,194 | 276 | 165.85 MB/s |

## Interpretation

- ResultCursor's final I-10 ownership fix keeps the core chunk handoff path at **0 B/op, 0 allocs/op** while enforcing bounded queued + consumer-held ownership.
- Profiling's worker-local counters remain effectively allocation-free and do not call exporters from the execution hot path.
- FTP range throughput is a local in-process acceptance baseline; it is not a remote-network SLA.
- WebDAV XML PROPFIND parsing is materially allocation-heavy. It is a protocol metadata/control path rather than an execution row hot path, but the accepted baseline is recorded for Phase 6 regression/operations work.
- No benchmark result authorizes unbounded result buffering, unbounded remote sessions, per-row exporter calls, or unmanaged large allocations.
