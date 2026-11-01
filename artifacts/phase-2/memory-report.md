# Phase 2 Memory Report

Date: 2026-11-01
Scenario: 200,000 input rows, 10,000 groups, forced-spill aggregate, 32 MiB engine limit, 64 KiB operator spill threshold.

| Metric | Observed |
|---|---:|
| Managed peak | 960,865 B |
| Managed used after query | 0 B |
| Temporary disk peak | 17,627,184 B |
| Temporary disk used after query | 0 B |
| Temporary files after query | 0 |
| Go heap live after GC | 122,224 B |
| Go total allocated during process | 333,608,592 B |
| Go heap sys | 7,962,624 B |
| RSS after GC | 8,126,464 B |
| GC cycles | 118 |
| Aggregate spill passes | 98 |
| Spill pages | 6,272 |
| Spilled aggregate states | 200,000 |
| Peak aggregate managed bytes | 501,828 B |

The measurement is produced by `cmd/phase2-metrics`. Engine-owned payloads, hash/aggregate state, aggregate spill-record scratch, sort/run buffers, page directories, page lists and spill I/O buffers are MemoryManager/BufferManager-managed. Temporary bytes are owned by query-scoped TemporaryStorageManager quota. The measurement verifies terminal ownership of both managed bytes and temp bytes/files is zero.

`engine_memory_limit != RSS` is intentionally preserved: Go runtime/control objects and executable/runtime pages are reported separately rather than hidden inside engine accounting.
