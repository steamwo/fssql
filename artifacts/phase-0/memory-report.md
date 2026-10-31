# Phase 0 Memory Report

Decision: **PASS**.

Measured by `cmd/phase0-metrics` after the 10,000,000-row Vector demo:

- Rows: 10,000,000
- Result sum: 49,999,995,000,000
- Managed peak: 16,384 bytes
- Managed bytes at query/demo end: 0 bytes
- Go heap live after GC: 95,152 bytes
- Go total allocated during process: 82,753,416 bytes
- RSS at measurement: 7,282,688 bytes
- GC cycles: 22
- Temp disk peak: 0 bytes (Phase 0 has no spill/temp implementation)

The 10M-row workload is streamed through 2048-row managed Vectors. Large Vector payload, StringHeap, HashTable, TupleDataCollection, and serialization buffers reserve through MemoryManager before allocation. Production source inspection confirms no production `make([]byte, ...)` under `internal/` outside `memory.DefaultAllocator`.

`engine_memory_limit` is deliberately not treated as process RSS. The report records managed and runtime/process measurements separately as required by the Memory Contract.
