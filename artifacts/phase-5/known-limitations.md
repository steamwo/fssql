# Phase 5 Known Limitations

Date: 2026-11-02
Status: **ACCEPTED**

## Product / contract limitations

1. **HTTP is the v1 required remote server transport; gRPC is intentionally not Stable in Phase 5.**
   ADR-0005 / AD-043 resolves the former `HTTP/gRPC Server` ambiguity. A future Stable gRPC service requires a separate ADR defining its public wire contract; Phase 5 does not freeze protobuf messages or gRPC framing.

2. **Server Source Inspect coverage is narrower than the full table-function surface.**
   Unsupported inspector combinations fail explicitly rather than invoking a second schema-inference path. This preserves the invariant that introspection schema must match the real Bind/schema-resolution path.

3. **Full multi-file schema validation remains Experimental.**
   Normal source inspection uses ordinary Bind/schema-resolution behavior. Full validation is opt-in and is not a default Stable DESCRIBE behavior.

4. **Native result ownership requires explicit release.**
   A consumer that obtains a native chunk must complete ownership with the public/native release path. Internally ResultCursor capacity bounds queued + consumer-held chunks together, and terminal publication waits for held Managed DataChunks to drain.

5. **`database/sql` parameter binding is outside the frozen Stable v1 API contract.**
   Non-empty argument lists are rejected rather than inventing an unfrozen placeholder/prepared-statement contract. This is registry entry `DBSQL-001` and is marked `stable_sql_semantics: false`.

6. **LIST/STRUCT values are lossless through the native Chunk API, not through `database/sql` driver.Value.**
   `database/sql` returns an explicit error for nested values because the standard driver.Value set has no lossless nested representation. This is intentional deviation `DBSQL-002` and does not change Stable SQL semantics.

7. **DECIMAL/HUGEINT cross the `database/sql` boundary as exact text.**
   The native Chunk API retains exact physical values; `database/sql` uses exact base-10 strings because driver.Value has no standard Decimal/Int128 representation. This is intentional deviation `DBSQL-003`.

8. **Server DATA encoding remains an external adapter boundary.**
   Arrow IPC may be used for external payloads but does not enter the Core/Execution/Query ABI. Phase 5 keeps the custom Vector/DataChunk representation internal.

9. **WebDAV XML metadata parsing remains allocation-heavy in the accepted local baseline.**
   `BenchmarkParsePropfind256-2` records approximately 964 KiB and 23k allocations/op on the 2026-11-02 acceptance runner. It is a protocol metadata/control path, not a row execution hot path, but remains a visible regression target.

10. **Process-level RSS/Go-heap numeric baselines are not emitted by the Phase 5 acceptance script.**
    Phase 5 verifies managed ownership, bounded structures, race safety and repeated lifecycle cleanup. Process-level operational telemetry remains a release/operations measurement item rather than a fabricated acceptance metric.

## Stable compatibility boundary

Stable SQL behavior is release-gated against DuckDB 2.0.0 through the SQLLogic/differential corpus. The deviation registry is machine validated: unknown mismatches fail; stale Stable classifications fail; a known `bug` still fails acceptance; and `unsupported_feature` cannot waive a frozen Stable SQL case. At Phase 5 re-acceptance there are no unresolved Stable SQL corpus deviations.

## Feature Freeze

Phase 5 acceptance closes Stable feature implementation. Phase 6 is Feature Freeze / v1 Release: only bug, security, compatibility, regression, operations, documentation and test/release work may proceed. New Stable features require the architecture/release process rather than implementation convenience.
