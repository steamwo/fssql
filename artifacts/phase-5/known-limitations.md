# Phase 5 Known Limitations

Date: 2026-11-01
Status: **ACCEPTED**

## Product / contract limitations

1. **HTTP is the v1 required remote server transport; gRPC is intentionally not Stable in Phase 5.**
   ADR-0005 / AD-043 resolves the former `HTTP/gRPC Server` ambiguity. A future Stable gRPC service requires a separate ADR defining its public wire contract; Phase 5 does not freeze protobuf messages or gRPC framing.

2. **Server Source Inspect currently has Bind-backed CSV and Parquet inspectors.**
   The generic `SourceSpec` recognizes JSON/NDJSON format identifiers, but there is no JSON/NDJSON TableFunction.Bind-backed inspector in the current repository. Those formats return an explicit unsupported-source error rather than falling back to a second schema-inference path. This preserves I-11 (`introspection schema == actual bind schema`) but leaves broader Source Inspect format coverage for later work.

3. **Full multi-file schema validation remains Experimental.**
   Normal source inspection uses the same ordinary Bind/schema-resolution behavior as query planning. Full validation is opt-in and is not a default Stable DESCRIBE behavior.

4. **ResultCursor requires explicit consumer release acknowledgement.**
   A consumer that obtains a native DataChunk through `Next` must complete ownership with `ReleaseChunk`. This is required so Query terminal publication cannot precede Managed Buffer release. Cursor capacity bounds queued + consumer-held chunks together.

5. **Server DATA encoding remains an external adapter boundary.**
   AD-022 allows Arrow IPC for Server v1 DATA payloads but does not require Arrow inside the execution core. Phase 5 does not change the Own Vector/DataChunk ABI or make Arrow/protobuf SDK types part of Core/Execution/Query.

6. **WebDAV XML metadata parsing is allocation-heavy in the accepted local baseline.**
   `BenchmarkParsePropfind256-2` records roughly 964 KiB and 23k allocations/op on the acceptance runner. This is a protocol metadata/control path, not a row execution hot path, but should remain visible to Phase 6 performance regression work.

7. **Process-level RSS/Go-heap numeric baselines are not emitted by the Phase 5 acceptance script.**
   Phase 5 verifies managed ownership, bounded structures, race safety and repeated lifecycle cleanup. The memory report does not invent RSS/GC numbers that were not measured; process-level operational telemetry remains a release/operations measurement item.

## Stable boundary

No Phase 6 feature is introduced by Phase 5 acceptance. Feature Freeze follows the Phase 5 milestone; any new Stable capability after this point must follow the release/architecture process rather than being added for implementation convenience.
