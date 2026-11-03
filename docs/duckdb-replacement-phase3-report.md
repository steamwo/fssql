# DuckDB replacement phase 3 report

## Scope

Phase 3 adds fixed-schema zero-argument registered table functions, managed typed batch ingestion, direct streaming for one reference, and query-scoped native materialization/replay for multiple references. The implementation reuses the existing managed Vector/DataChunk representation and temporary page store; it does not introduce CSV staging, ordinary persistent tables, INSERT, or an Appender-compatible API.

## Implemented checks

The phase includes tests for caller BLOB-buffer reuse, one upstream open across two query references, identical results with in-memory materialization and forced spill, empty-source schema preservation, source error cleanup, cancellation cleanup, and managed-memory return to baseline after query close.

Registration rejects invalid/duplicate names, unsupported schema types, missing callbacks, and invalid DECIMAL definitions. Batch publication checks row bounds and non-nullable cells. Variable-width values are copied into engine-owned managed buffers, and DECIMAL ingestion uses exact integer scaling rather than `float64`.

## Resource and failure behavior

Single-reference scans stay streaming. Multi-reference scans serialize chunks into managed buffers up to `SpillMemoryBudgetBytes`; after that they use the existing checksummed query temporary page store. Memory-limit and temporary-storage-limit failures propagate as query failures. The shared source is never reopened after a failure. Query cleanup owns reader close, resident buffer release, spill-store close, temporary files, and final memory-scope closure.

## Verification status

The repository connector available for this change does not provide a runnable checkout, and the current branch has no commit status checks. The Phase 3 tests are therefore committed as executable acceptance coverage, but this report does not claim they were executed in this session. CI or a normal repository checkout must run the package/full test suite before Phase 3 is treated as release-gated acceptance complete.
