# Phase 0 Benchmark Report

Decision: **PASS**.

The Phase 0 performance baseline is recorded in `benchmark-phase0.json`, `benchmark-phase0.md`, and `benchmark-raw.txt`.

Performance-gate evidence:

- Numeric add kernel runs on 2048-row DataChunks with allocation count independent of row count (`TestNumericKernelAllocationsDoNotScalePerRow`).
- VectorHash Int64: 0 B/op, 0 allocs/op.
- VectorHash VARCHAR: 0 B/op, 0 allocs/op, covering both SSO and managed StringHeap values; SSO content correctness has an explicit regression test.
- HashTable lookup: 0 B/op, 0 allocs/op.
- Push Pipeline has a repeatable 100K-row baseline.
- 10M-row demo completes using chunked managed buffers rather than a 10M-row Go-heap payload.

DuckDB performance comparison is not executable in this environment because neither the DuckDB CLI nor Python module is installed. Phase 0 exposes no Stable SQL surface. The conditional requirement to investigate a known quantity-order regression is therefore not triggered; this report establishes the Phase 0 baseline for future regression comparisons.
