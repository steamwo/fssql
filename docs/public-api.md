# fssql v1 Public API

Status: Phase 6 Feature Freeze / v1 release documentation.

This document describes the implemented public Go API and the internal HTTP adapter's wire contract. Internal package types and constructors are not part of the public Go API. The architecture feature contract describes the engine as a whole; it does not imply that every internal adapter is configurable through `gosql.Config`.

For a complete runnable example of both Go query APIs, see [quickstart](../examples/quickstart/main.go) and run `go run ./examples/quickstart` from the repository root.

## Availability through the public Go package

| Capability | Available to an external Go module today |
| --- | --- |
| Native queries and statements | `Open`, `DB.QueryChunks`, `DB.Exec`, `DB.Close`, `DB.MemoryUsed`. |
| `database/sql` | `OpenSQLDB`; query and execute through the same engine. |
| Catalog | `Open` constructs a MemoryCatalog shared within that DB instance. SQLiteCatalog exists internally, but Config has no catalog injection or persistence option. |
| Data sources and COPY destinations | `Config.LocalFiles` and `Config.S3` register explicitly configured local and S3 drivers. Unconfigured schemes fail explicitly. TempDir/ManifestDir configure spill and recovery state. |
| Other protocol adapters | SFTP, FTP and WebDAV implementations are internal; Config has no public driver registration option. |
| Source introspection | SQL `DESCRIBE`, `parquet_schema` and `parquet_file_metadata` use the query path. There is no exported `DB.InspectSource` method. |
| HTTP service | The wire adapter is internal; the public package has no HTTP server constructor. |
| Nested results | Type metadata is exposed for LIST/STRUCT; Chunk currently has no public accessor for nested element values. |

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

`gosql.Config` exposes bounded engine memory, temporary storage, spill policy, catalog/cache capacities, worker/queue limits, local spill/COPY manifest roots, and optional local-file/S3 configuration. Zero values select defaults:

| Field | Default |
| --- | --- |
| `MemoryLimitBytes` | 256 MiB |
| `TempLimitBytes` | 1 GiB |
| `SpillMemoryBudgetBytes` | 16 MiB, capped at MemoryLimitBytes |
| `SpillBlockBytes` | 64 KiB |
| `CatalogMaxEntries` | 4096 |
| `ParquetMetadataEntries` | 256 |
| `TempDir` | `filepath.Join(os.TempDir(), "fssql-spill")` |
| `ManifestDir` | `filepath.Join(os.TempDir(), "fssql-copy-manifests")` |

MemoryUsed and MemoryLimitBytes describe engine-managed memory, not total process RSS. Configuring local spill or manifest paths does not enable reading or exporting local data files; configure `LocalFiles.RootDir` explicitly for local data access.

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

Nested LIST/STRUCT logical metadata is public through `Type.Child`/`Type.Fields`. The current public Chunk has no LIST/STRUCT element-value accessors, so callers cannot traverse nested values through this API. The database/sql adapter also rejects these values instead of coercing them into `driver.Value`.

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

Current boundary rules:

- `QueryContext`/`ExecContext` and prepared statements support `?`, `$1`/`$2`, or `$name` parameters in SELECT statements. Do not mix placeholder styles. Numbered placeholders must be contiguous from `$1`; named placeholders use `sql.Named` and can be repeated.
- Parameters are bound as AST values, never interpolated into SQL. Supported values are `nil`, bool, int64, float64, string, []byte and time.Time, including the standard `database/sql` conversions and `driver.Valuer`. A time.Time parameter preserves nanoseconds as TIMESTAMP_NS and must fit that type's range. An untyped NULL parameter projected directly defaults to nullable VARCHAR.
- `PrepareContext` caches parsed syntax. Each execution binds its own values and plans against a fresh statement/source snapshot; physical plans and query resources are not shared across executions. Parameterized DDL/COPY is rejected.
- Transactions are not supported; `Begin`/`BeginTx` return an explicit error, including read-only transactions.
- LIST/STRUCT values are rejected by the row adapter; the current public native Chunk exposes their type metadata but has no nested value accessors.
- DECIMAL/HUGEINT cross `database/sql` as exact base-10 text. Use an explicit CAST for decimal/wide-integer input, for example `SELECT CAST(? AS DECIMAL(38,2))` with a decimal string.
- `LastInsertId` is not applicable.

```go
stmt, err := sqlDB.PrepareContext(ctx, "SELECT CAST($amount AS DECIMAL(38,2))")
if err != nil {
    // handle error
}
defer stmt.Close()
var amount string
err = stmt.QueryRowContext(ctx, sql.Named("amount", "12345678901234567890.12")).Scan(&amount)
```

SQL compatibility additions:

- `TABLESAMPLE` samples one FROM input while preserving its qualifiers; `USING SAMPLE` samples the entire FROM input after joins and before WHERE/aggregation. The latter may appear immediately after FROM or after WHERE/GROUP BY/HAVING, before ORDER BY.
- Both accept fixed rows (reservoir), percentages using PERCENT or %, explicit reservoir/bernoulli/system methods, and REPEATABLE seeds or `(method, seed)` syntax. Fixed-size sampling defaults to reservoir; percentages default to system. System sampling makes one decision per execution batch; Bernoulli sampling decides per row. Reservoir percentages first spill/store the population to determine the exact sample size and may use more temporary I/O.
- BIGINT `%` supports signed operands and NULL propagation; a zero divisor returns an error.
- Integer arithmetic covers TINYINT/SMALLINT/INTEGER/BIGINT/HUGEINT and their supported unsigned forms, with checked arithmetic overflow. Mixed signed/unsigned operands widen to BIGINT or HUGEINT without losing the unsigned range. FLOAT arithmetic retains its FLOAT result type.
- DECIMAL-to-integer CAST rounds halfway cases away from zero and checks the rounded value against the target range.
- Aggregate SELECT without FROM uses the single implicit input row; WHERE FALSE produces the standard empty global-aggregate result.
- Explicit `CAST(... AS HUGEINT)` supports exact decimal text and checked numeric conversions. Overflow returns an error.
- Non-correlated scalar subqueries may be combined with aggregates/HAVING and FROM derived tables/CTEs. They are materialized before outer execution; zero rows produce NULL and more than one row is an error. Supported correlated forms and remaining scope limits are described below.
- Nested scalar dependencies are prepared from inner to outer queries and reuse managed runtime constants. Dependencies removed by EXISTS projection pruning are not evaluated.

Common SQL extensions:

- `IN (...)` / `NOT IN (...)` accept expression lists or single-column SELECT subqueries and preserve SQL NULL semantics, including empty sets and a NULL left operand. Subquery membership is lowered to a reusable right set, joins and NULL statistics. `BETWEEN` / `NOT BETWEEN` evaluate the tested value once.
- `LIKE` / `NOT LIKE` support `%`, `_` and an optional empty or single-byte ESCAPE string. The default has no escape character. Matching follows DuckDB 2.0 Unicode semantics: `_` consumes one Unicode code point.
- Non-correlated `EXISTS (SELECT ...)` / `NOT EXISTS` test row presence, including multi-column projections, aggregates, LIMIT and OFFSET. Correlated EXISTS, IN and scalar queries use equality joins or parameter-domain decorrelation. Missing COUNT groups return zero; scalar lookups returning more than one row fail explicitly. See the scope limits below.
- `UNION ALL` preserves duplicates and combines compatible column types without converting exact integers to floating point. Output names come from the first SELECT; a trailing ORDER BY/LIMIT/OFFSET applies to the combined result. Plain UNION/INTERSECT/EXCEPT remain unsupported.
- `COUNT/SUM/AVG/MIN/MAX(DISTINCT expression)` support one scalar argument and retain their normal NULL, empty-input, result-type and overflow behavior. COUNT/SUM/AVG share a sort-and-mark input for the same DISTINCT argument; MIN/MAX do not require a de-duplication pass. Different DISTINCT arguments may add sorting and temporary I/O.
- `JOIN USING (column, ...)` supports merged unqualified keys and each side's original qualified keys, including outer joins. SELECT * retains the left input's column order and omits duplicate right keys. Compatible numeric join keys are materialized in their common type before hashing.
- Derived SELECT inputs support explicit `LATERAL` and implicit references to preceding FROM inputs, including comma/CROSS/INNER joins and LEFT JOIN. Chained inputs, local aggregates, constant per-parameter row limits, prepared values and derived column aliases are supported. Dependent RIGHT/FULL/SEMI/ANTI joins and aggregates owned by a sibling input are rejected, as in DuckDB 2.0.0.

Parameter-domain correlation supports non-equality/OR predicates, constant per-parameter LIMIT/OFFSET with ORDER BY, nested expression-subquery references across query levels, global aggregate expressions/HAVING, and references to grouping columns from outer aggregate outputs/HAVING. Domains propagate through derived tables/CTEs, UNION ALL and JOINs, including outer/semi/anti joins and expression subqueries in ON. Pure outer aggregate calls are evaluated by their owning scope; child scalar cardinality and grouping legality are still checked. Non-constant correlated LIMIT/OFFSET and correlation pushed through SAMPLE remain rejected. Sampling an independent source before a correlated filter is supported. LATERAL table functions retain the static-argument binding restriction; quantified ANY/ALL remains unsupported. Plans without usable hash keys currently use a cross product/filter and retain the CROSS build side in managed memory. See [subquery lowering](architecture/subquery-lowering.md).

Remaining boundary differences are documented in `compatibility/deviations.yaml`.

## S3 public configuration

`gosql.S3Config` contains endpoint, region, path-style option, explicit insecure-HTTP opt-in, credentials, credential scope/version, security profile, retry limit, and request timeout.

S3 is registered only when this configuration is present. No remote scheme is silently auto-enabled, and unsupported schemes do not bypass VFS capability/security checks.

## Stable HTTP query wire surface

The internal v1 HTTP Query adapter defines these wire operations. Its constructors live under `internal/server`; external Go modules cannot instantiate it through `pkg/gosql`. This section documents the wire contract, not an exported server startup API:

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
