# Phase 0 Known Limitations

These are intentional Phase 0 scope boundaries, not hidden semantic downgrades:

- No SQL parser, binder, optimizer, catalog, VFS, Parquet, S3, CSV/JSON, export, or spill algorithms are implemented. Those belong to later locked phases.
- SQLLogic, DuckDB SQL differential, and external interop suites are not applicable yet because Phase 0 exposes no Stable SQL/external query surface. The project-wide testing architecture remains required once those surfaces exist.
- DECIMAL is a real LogicalType with validated precision/scale and physical mapping; Phase 0 does not implement the complete DECIMAL SQL expression/cast matrix.
- Date/Timestamp have canonical physical representations; full SQL temporal semantics are outside the Phase 0 kernel proof.
- LIST and STRUCT are real Vector capabilities with child-vector ownership/layout primitives; broad expression kernels over nested values are not part of Phase 0.
- RowLayout/TupleDataCollection currently exercise fixed-width row layouts. VARCHAR execution data is supported by Vector/StringHeap and VectorHash; generalized variable-width tuple rows are deferred to the later hash/join infrastructure work that consumes these primitives.
- HashTable is a pre-reserved open-addressing primitive with a fixed load-factor ceiling. Adaptive/external join and spill growth are explicitly Phase 2 work.
- QueryContext in Phase 0 owns cancellation, task accounting, and managed memory. Temp, remote I/O, profiler, and source-snapshot ownership become concrete only when their corresponding modules are introduced.
- No remote spill is implemented; remote spill is DEFERRED by frozen architecture.
