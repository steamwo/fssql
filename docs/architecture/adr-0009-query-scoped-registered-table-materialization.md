# ADR-0009: Query-scoped materialization for registered table functions

## Status

Accepted for DuckDB replacement phase 3.

## Context

Database row streams are commonly single-pass and may reuse driver-owned buffers. SQL can nevertheless reference one logical source more than once through CTE expansion, joins, or scalar/statistical branches. Reopening the database stream can observe a different snapshot and repeat side effects. Converting rows through CSV would also lose logical type identity and add unnecessary allocation and I/O.

The engine already has managed `DataChunk` vectors, native chunk serialization, query-scoped temporary storage, and a checksummed spill page store.

## Decision

Public registered table functions have fixed schema and zero SQL arguments. Registration is frozen at `gosql.Open`. Bind uses the frozen schema plus a query-scoped fingerprint and never opens the external stream.

For exactly one query reference, execution returns a direct streaming source. If binding or the final physical tree proves that a registered source has multiple references, the first consumer materializes the upstream once. Each batch is serialized in the engine native chunk format. Serialized batches remain managed in memory within the configured spill-memory budget; when the budget is exceeded, already retained and subsequent batches are written to the existing query temporary page store. Every consumer then deserializes through an independent replay cursor.

String and binary setters copy caller data before returning, so source drivers may immediately reuse their buffers. DECIMAL conversion is exact. Source open/read/materialization failures are terminal and never trigger reopen or fallback.

Reader, resident buffers, and spill-store ownership are attached to `query.Context` cleanup callbacks. Cleanup runs before the query temporary and memory scopes close.

## Consequences

Single-reference sources preserve streaming first-batch behavior and avoid materialization I/O. Multi-reference SQL observes one source snapshot without requiring CREATE TABLE/INSERT/Appender compatibility. Query-lifetime replay may consume temporary storage and remains bounded by existing memory/temp quotas. It does not provide process-restart recovery for an external database cursor.
