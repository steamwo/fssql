# fssql v1 Public API

Status: Phase 6 Feature Freeze / v1 release documentation.

This document describes the frozen public API surface already implemented by the v1 release. Internal package types are not part of the public ABI.

## Go package

Import:

```go
import "github.com/steamwo/fssql/pkg/gosql"
```

### Open an engine

```go
db, err := gosql.Open(gosql.Config{})
if err != nil {
    // handle configuration/composition error
}
defer db.Close()
```

`gosql.Config` exposes bounded engine memory, temporary storage, spill policy, catalog/cache capacities, worker/queue limits, local spill/COPY manifest roots, and optional S3 configuration. Zero values select the documented conservative defaults.

`DB.Close` prevents new work. Query calls already in progress retain their own query-scoped context/ownership and finish or cancel through the supplied context.

### Native chunk query API

```go
columns, err := db.QueryChunks(ctx, "SELECT 1", func(chunk *gosql.Chunk) error {
    for row := 0; row < chunk.RowCount(); row++ {
        value, valid, err := chunk.Int64(row, 0)
        if err != nil {
            return err
        }
        if valid {
            _ = value
        }
    }
    return nil
})
```

`QueryChunks` executes the production Parser → Binder → Planner → Push-Pipeline path. A callback receives a borrowed native Chunk. The Chunk must not be retained after the callback returns.

Result schema is returned as `[]gosql.Column`. Public logical type metadata uses `gosql.Type`/`gosql.StructField`; internal planner/type objects are not exposed.

`Chunk` provides:

- `RowCount()` and `ColumnCount()`;
- `Type(column)` and `IsNull(row, column)`;
- exact integer/date/timestamp/narrow-DECIMAL access through `Int64`;
- FLOAT/DOUBLE through `Float64`;
- BOOLEAN through `Bool`;
- VARCHAR through `String`;
- HUGEINT and DECIMAL width > 18 through lossless `Int128`.

`gosql.Int128` is the public two-limb signed 128-bit representation. Its `String` method returns exact base-10 text.

Nested LIST/STRUCT logical metadata is public through `Type.Child`/`Type.Fields`; lossless nested result access remains a native-Chunk boundary and is not coerced into the limited `database/sql/driver.Value` type set.

### Execute non-row statements

```go
err := db.Exec(ctx, "COPY (SELECT 1) TO 's3://bucket/out.parquet'")
```

`Exec` runs a Stable statement and discards any produced result chunks. It is intended for DDL/control/COPY statements. SELECT callers should use `QueryChunks` or `database/sql`.

### Managed-memory observation

```go
used := db.MemoryUsed()
```

`MemoryUsed` reports currently charged engine-managed memory. It is not process RSS or total Go heap usage.

## database/sql compatibility

```go
sqlDB, err := gosql.OpenSQLDB(gosql.Config{})
if err != nil {
    // handle error
}
defer sqlDB.Close()

rows, err := sqlDB.QueryContext(ctx, "SELECT 1")
```

`OpenSQLDB` uses `sql.OpenDB` with a private Connector over the same native DB execution path; it does not register a process-global driver name.

Frozen v1 boundary rules:

- positional/named database/sql parameters are not part of Stable v1 and non-empty argument lists are rejected;
- LIST/STRUCT values are not losslessly representable by `driver.Value` and therefore remain native-Chunk-only;
- DECIMAL/HUGEINT cross `database/sql` as exact base-10 text;
- `LastInsertId` is not applicable;
- transaction/statement preparation is not a separately frozen v1 execution model and the driver does not invent one behind the native engine.

These boundary differences are documented in `compatibility/deviations.yaml` and are marked as not changing Stable SQL semantics.

## S3 public configuration

`gosql.S3Config` contains endpoint, region, path-style option, explicit insecure-HTTP opt-in, credentials, credential scope/version, security profile, retry limit, and request timeout.

S3 is registered only when this configuration is present. No remote scheme is silently auto-enabled, and unsupported schemes do not bypass VFS capability/security checks.

## Stable HTTP query wire surface

The v1 HTTP Query adapter exposes these wire operations when embedded by a server application:

```text
POST   /v1/queries
GET    /v1/queries/{id}
POST   /v1/queries/{id}/cancel
GET    /v1/queries/{id}/results
DELETE /v1/queries/{id}
```

Submit payload fields are:

```json
{
  "sql": "SELECT 1",
  "resource_group": "interactive",
  "class": "interactive",
  "cpu_slots": 1,
  "memory_bytes": 1048576,
  "remote_io_slots": 1,
  "spill_io_slots": 1
}
```

The HTTP adapter bounds control-body size and rejects unknown JSON fields. Query submission returns HTTP 202 with a stable query ID. Status returns the query state snapshot. Cancel requests cancellation; terminal CANCELED is published only after lifecycle cleanup. Results are single-consumer and stream through a caller-supplied external encoder.

The DATA encoder and its media type are explicit embedding inputs. External Arrow IPC may be used by an adapter, but Arrow is not the Core/Execution/Query ABI.

## Context, cancellation, and ownership

All query calls are context-aware. A canceled context must unblock work and release query-scoped ownership. Public APIs do not authorize detached background query goroutines or unbounded result buffering.

For native chunks, the callback lifetime is the ownership boundary. For `database/sql`, `Rows.Close`/context cancellation is the consumer lifecycle boundary.

## Compatibility target

Stable SQL behavior targets DuckDB compatibility and is release-gated against DuckDB 2.0.0. Any intentional semantic deviation must be explicit in the compatibility registry; unknown mismatches are release blockers.
