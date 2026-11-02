# fssql v1 Operational Guide

Status: Phase 6 Feature Freeze / v1 release documentation.

This guide documents already-implemented Stable behavior. It does not add a new Stable feature.

## Deployment model

The v1 release is primarily an embeddable Go engine through `pkg/gosql`. `gosql.Open` creates an in-process engine with bounded managed memory, temporary spill ownership, catalog metadata, Stable SQL execution, and optional S3 VFS support. `gosql.OpenSQLDB` exposes the same execution path through `database/sql`.

The repository also contains the Phase 5 HTTP Query transport adapter. The transport is an embedding boundary, not a separate SQL engine: query lifetime remains owned by QueryService/QueryHandle and result ownership remains bounded by ResultCursor.

## Resource configuration

`gosql.Config` controls the in-process resource envelope. Zero values select the frozen defaults:

- engine managed memory: 256 MiB;
- temporary storage quota: 1 GiB;
- per-operator spill memory budget: 16 MiB, capped by engine memory;
- spill block target: 64 KiB;
- catalog entries: 4096;
- Parquet metadata cache entries: 256;
- spill root: `${TMPDIR}/fssql-spill`;
- COPY recovery manifests: `${TMPDIR}/fssql-copy-manifests`.

Production deployments should set `TempDir` and `ManifestDir` to controlled local paths with sufficient capacity and appropriate filesystem permissions. Temporary spill ownership is query-scoped; COPY manifests are recovery state and are not public output sidecars.

`engine_memory_limit` is not a process RSS limit. The release resource envelope records managed peak, Go heap live/allocated, RSS, GC cycles, and temporary-disk peak independently.

## S3 configuration and security

S3 is registered in the public in-process engine only when `Config.S3` is supplied. Remote protocols are not auto-discovered and unsupported schemes fail through VFS.

Use HTTPS endpoints by default. `AllowInsecureHTTP` is an explicit opt-in and must not be enabled merely to recover from TLS or certificate failures. Credentials are carried through the S3 configuration boundary and are not copied into SQL text or Catalog metadata. Credential scope/version and security profile are part of endpoint identity and capability probing.

The frozen VFS rules remain in force:

- effective capabilities are static driver capabilities narrowed by endpoint probe;
- no silent whole-file fallback for an unavailable exact-range capability;
- S3 single-object publication uses `AtomicPublishWrite`; it does not claim `AtomicRename`;
- remote scan uses the unified VFS/I/O path.

## Query cancellation and ownership

All public query entry points accept a `context.Context`. Cancellation is part of correctness, not advisory telemetry.

For native `QueryChunks`, each `Chunk` is borrowed only for the callback duration. Do not retain a Chunk or data obtained as a borrowed view after the callback returns.

For `database/sql`, closing `Rows` or canceling the query context stops the producer and releases queued result ownership. Result buffering is bounded; a slow consumer creates backpressure rather than unbounded memory growth.

A successful terminal query state must not hide live query memory, temporary spill files, admission leases, remote-session leases, or result-chunk ownership.

## COPY recovery

Stable `COPY TO` uses the existing Export/VFS lifecycle and local manifest recovery state. Single-file object publication must not expose an incomplete final object. On restart/interruption, reconciliation uses the manifest/staging state rather than silently treating a partial destination as complete.

Keep `ManifestDir` durable enough for the deployment's recovery requirements and do not delete active manifest files while COPY work is in progress.

## HTTP query transport

The frozen HTTP Query surface is:

- `POST /v1/queries` — submit SQL and resource requirements;
- `GET /v1/queries/{id}` — read query state;
- `POST /v1/queries/{id}/cancel` — request cancellation;
- `GET /v1/queries/{id}/results` — claim and stream the single result consumer;
- `DELETE /v1/queries/{id}` — forget terminal query retention.

Submit JSON is bounded (default 1 MiB at the HTTP adapter). Unknown JSON fields are rejected. DATA encoding is an explicitly supplied external encoder; Arrow or another interchange format must not become the internal Vector/DataChunk ABI.

Operators should cancel abandoned result streams and eventually forget terminal query records so bounded retention capacity is recycled.

## Graceful shutdown

Shutdown must stop new admission, cancel or drain active queries, wait for owned resources to reach their terminal state, and report a timeout/failure if ownership remains. A shutdown path must never report success while a Query still owns managed memory, result chunks, temp files, admission leases, or remote operations.

## Compatibility and upgrade policy

Stable SQL is release-gated against DuckDB 2.0.0. `compatibility/deviations.yaml` is the machine-validated deviation registry. Unknown Stable mismatches fail release acceptance; known bugs still fail; `unsupported_feature` cannot waive a frozen Stable corpus mismatch.

Configuration migrations are explicit and versioned. A future/unknown config version must fail rather than silently downgrade semantics or security.

## Release verification

Run the Phase 6 gate in an environment containing the pinned interoperability dependencies:

```text
bash ./scripts/acceptance.sh phase-6
```

The gate includes full ordinary/race/vet/architecture checks, Stable SQL differential verification, forced-spill equality, fault/leak/security/stability checks, Parquet interoperability, resource-envelope workloads, API/doc checks, and performance baselines.
