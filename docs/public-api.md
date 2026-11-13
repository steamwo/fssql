# fssql v1 Public API

Status: implemented API reference for the current branch. The frozen v1 baseline and later extensions are distinguished in the [feature contract](architecture/feature-contract.md). Implementation coverage does not imply complete release or large-data performance acceptance.

This document describes the implemented public Go API and the internal HTTP adapter's wire contract. Internal package types and constructors are not part of the public Go API. The architecture feature contract describes the engine as a whole; it does not imply that every internal adapter is configurable through `gosql.Config`.

For a complete runnable example covering both Go query APIs and the other public features, see [quickstart](../examples/quickstart/README.md) and run `go run ./examples/quickstart` from the repository root.

## Availability through the public Go package

| Capability | Available to an external Go module today |
| --- | --- |
| Native queries and statements | `Open`, `DB.QueryChunks`, `DB.Exec`, `DB.CopyTo`, `DB.Close`, `DB.MemoryUsed`. Native QueryChunks/Exec accept SQL text; SELECT value parameters use database/sql. |
| `database/sql` | `OpenSQLDB`; SELECT parameters, prepared syntax and queries/statements through the same engine. Transactions remain unsupported. |
| Custom sources | `Config.TableFunctions`, `TableFunction`, `BatchReader`, `BatchWriter`; fixed-schema, zero-argument sources. |
| Custom scalar functions | `Config.ScalarFunctions`, `ScalarFunction`, `ScalarValue`, NULL policies and `NewHMACSHA256ScalarFunction`. |
| Structured export | `DB.CopyTo`, `CopyOptions`, `ExportResult`, `Config.ParquetKeys` and `ParquetKeyResolver`. |
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
| `Workers` | 1 |
| `BlockingQueue` | 1 |
| `TempDir` | `filepath.Join(os.TempDir(), "fssql-spill")` |
| `ManifestDir` | `filepath.Join(os.TempDir(), "fssql-copy-manifests")` |
| `LocalFiles`, `S3` | nil; no local/S3 data driver registered |
| `TableFunctions`, `ScalarFunctions` | empty; built-ins only |
| `ParquetKeys` | nil; no application-provided export key resolver |

MemoryUsed and MemoryLimitBytes describe engine-managed memory, not total process RSS. Configuring local spill or manifest paths does not enable reading or exporting local data files; configure `LocalFiles.RootDir` explicitly for local data access.

`SpillMemoryBudgetBytes` is an operator policy, not a separate memory allowance; hash joins use an effective minimum budget of 4096 bytes. All managed allocations still share the DB memory limit. CROSS build inputs remain resident, and some stateful window/sampling plans execute serially even when `Workers` is higher. Caller allocations inside source/UDF callbacks are outside managed-memory accounting.

`DB.Close` prevents new work. Query calls already in progress retain their own query-scoped context/ownership and finish or cancel through the supplied context.

### Local files

`LocalFileConfig.RootDir` must be an absolute directory. Use `FileURI` for platform paths rather than constructing `file://` strings, especially on Windows. Configuring a root does not make paths outside that root accessible.

```go
root, err := filepath.Abs("data") // an existing application data directory
if err != nil { return err }
db, err := gosql.Open(gosql.Config{
    LocalFiles: &gosql.LocalFileConfig{RootDir: root},
})
if err != nil { return err }
defer db.Close()
uri, err := gosql.FileURI(filepath.Join(root, "input.parquet"))
if err != nil { return err }
// Use SQL quoting for native SQL text, or bind uri through database/sql:
_ = uri
```

With an `OpenSQLDB` instance configured for the same root, use `QueryContext(ctx, "SELECT * FROM read_parquet(?)", uri)`. Parameters supply values; table names, column identifiers and SQL fragments cannot be parameters.

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
- unsigned integers through `Uint64`;
- FLOAT/DOUBLE through `Float64`;
- BOOLEAN through `Bool`;
- VARCHAR through `String`;
- BLOB through `Bytes`, which returns a copy that can outlive the callback;
- HUGEINT and DECIMAL width > 18 through lossless `Int128`.

`gosql.Int128` is the public two-limb signed 128-bit representation. Its `String` method returns exact base-10 text.

Cell accessors use zero-based `(row, column)` indexes and return `(value, valid, error)`. `valid == false` with no error means SQL NULL; the zero value is not a substitute for checking validity. Type/index mismatches return errors. `Type(column)` describes the logical type; DECIMAL uses `Width`/`Scale`, LIST uses `Child`, and STRUCT uses `Fields`.

Native DECIMAL integers are unscaled storage values: DECIMAL(10,2) value 12.34 reads as 1234. `Int128.String()` also formats the unscaled integer; apply the declared scale when formatting a wide DECIMAL. DATE uses days since the Unix epoch; timestamp integer units follow the source logical timestamp type. Cast explicitly to `TIMESTAMP_NS` when callers require a fixed native unit instead of inferring it from `TIMESTAMPTZ` alone.

Public constructors include `UTinyIntType`, `USmallIntType`, `UIntegerType`, `UBigIntType`, `BlobType`, `TimestampNSType` and `TimestampTZType`. Other scalar types can be described with `Type{ID: "BIGINT"}` or `Type{ID: "DECIMAL", Width: 10, Scale: 2}`.

Nested LIST/STRUCT logical metadata is public through `Type.Child`/`Type.Fields`. The current public Chunk has no LIST/STRUCT element-value accessors, so callers cannot traverse nested values through this API. The database/sql adapter also rejects these values instead of coercing them into `driver.Value`.

### Execute non-row statements

```go
err := db.Exec(ctx, "COPY (SELECT 1) TO 's3://bucket/out.parquet'")
```

`Exec` runs a Stable statement and discards any produced result chunks. It is intended for DDL/control/COPY statements. SELECT callers should use `QueryChunks` or `database/sql`.

### Structured export

```go
result, err := db.CopyTo(ctx, "SELECT 1 AS id", destination, gosql.CopyOptions{
    Format: gosql.CopyFormatParquet,
    Compression: "snappy",
})
if err != nil { return err }
// result.Rows, result.Bytes and result.Columns describe committed output.
```

`destination` must use an enabled local/S3 driver. `CopyFormatCSV`, `CopyFormatJSON`, `CopyFormatNDJSON` and `CopyFormatParquet` select the format. Text output compression is `identity` (default), `gzip` or `zstd`; Parquet uses page compression `snappy` (default) or `identity`. Explicit `none` also disables compression. These defaults apply to both `CopyTo` and SQL COPY. CSV `Header` defaults to true; `Delimiter` and `Quote` configure single-byte separators/quoting. A failure returns a zero `ExportResult`; successful return follows final-object commit through the same export path as SQL COPY.

Parquet export automatically considers `RLE_DICTIONARY` for low-cardinality VARCHAR/BLOB column chunks, including encrypted output. Dictionaries are limited to 256 entries and 32 KiB of PLAIN dictionary bytes; small chunks, high cardinality, insufficient estimated savings or a preparation-time memory limit retain PLAIN encoding. Encoding selection is internal and does not change SQL values, NULL handling or the compression option. Page/column statistics remain available independently of encoding.

For encrypted Parquet export, set `CopyOptions.Encryption` to `ParquetEncryptionOptions{Algorithm: gosql.ParquetAES256GCM, KeyID: keyID}` and provide `Config.ParquetKeys`. The resolver receives `(context.Context, keyID, algorithm)` and must return 16 bytes for `ParquetAES128GCM` or 32 bytes for `ParquetAES256GCM`. SQL/options/manifests carry the key identity, not key bytes. This is an export API; it does not expose a general encrypted-file read key provider.

### Go table and scalar functions

Register definitions through `Config.TableFunctions` / `Config.ScalarFunctions` when opening either API. Registration is DB-scoped; names must be valid function identifiers and cannot collide with existing functions.

| API | Contract |
| --- | --- |
| `TableFunction{Name, Columns, Open}` | Fixed nonempty schema, zero SQL arguments; up to 27 registered definitions. Open is lazy and at most once per query for that registered source. |
| `BatchReader.Next(ctx, writer)` | Fill at most `writer.Capacity()` rows and return the row count. `(0, io.EOF)` ends the source; `(n > 0, io.EOF)` publishes the last batch. Return errors to abort. |
| `BatchReader.Close()` | Release caller-owned source resources when the query finishes, fails or closes early. |
| `BatchWriter` | Valid only during Next; setters copy into engine-owned storage. Use `SetNull` explicitly for NULL cells and do not retain the writer. |
| `ScalarFunction` | Declare `Name`, fixed `Arguments`, `Result`, `NullHandling`, `Deterministic` metadata and `Eval`. Eval returns a `ScalarValue` or an error that aborts the query. |
| `ScalarNullPropagate` | Default: any NULL input produces NULL without invoking Eval. |
| `ScalarNullCall` | Eval receives NULL inputs as `ScalarValue{Valid: false}` and decides the result. |

BatchWriter provides `SetBool`, `SetInt64`, `SetUint64`, `SetFloat64`, `SetDecimal`, `SetString`, `SetBytes`, `SetDate`, `SetTimestamp` and `SetTimestampNS`. `SetDecimal` accepts exact decimal text. ScalarValue uses the corresponding typed field (`Int128` for HUGEINT, `Decimal` text for DECIMAL, `Time` for temporal values); `Valid: false` returns NULL. Variable-width UDF inputs/results are copied at the boundary. Callbacks should honor cancellation where available and synchronize shared mutable state if workers can invoke them concurrently; `Deterministic` is metadata, not a promise about invocation counts.

For a bounded source returning IDs 1, 2 and 3, the reader and registration can be implemented as follows (imports: context, io and gosql):

```go
type idReader struct { next int64 }

func (r *idReader) Next(ctx context.Context, w gosql.BatchWriter) (int, error) {
    if err := ctx.Err(); err != nil { return 0, err }
    if r.next == 3 { return 0, io.EOF }
    n := min(w.Capacity(), int(3-r.next))
    for row := 0; row < n; row++ {
        if err := w.SetInt64(row, 0, r.next+int64(row)+1); err != nil {
            return 0, err
        }
    }
    r.next += int64(n)
    return n, nil
}
func (*idReader) Close() error { return nil }

func appSource() gosql.TableFunction {
    return gosql.TableFunction{
        Name: "app_rows",
        Columns: []gosql.Column{{Name: "id", Type: gosql.Type{ID: "BIGINT"}}},
        Open: func(context.Context) (gosql.BatchReader, error) {
            return &idReader{}, nil
        },
    }
}
```

Open with `gosql.Config{TableFunctions: []gosql.TableFunction{appSource()}}`, then query `SELECT sum(id) FROM app_rows()`; its result is 6. `Config.TableFunctions` is also supported by `OpenSQLDB`.

For example, a typed VARCHAR UDF can be registered as follows:

```go
upper := gosql.ScalarFunction{
    Name: "app_upper",
    Arguments: []gosql.Type{{ID: "VARCHAR"}},
    Result: gosql.Type{ID: "VARCHAR"},
    NullHandling: gosql.ScalarNullPropagate,
    Deterministic: true,
    Eval: func(args []gosql.ScalarValue) (gosql.ScalarValue, error) {
        return gosql.ScalarValue{
            Type: gosql.Type{ID: "VARCHAR"}, Valid: true,
            String: strings.ToUpper(args[0].String),
        }, nil
    },
}
// gosql.OpenSQLDB(gosql.Config{ScalarFunctions: []gosql.ScalarFunction{upper}})
// SELECT app_upper('hello') returns HELLO; app_upper(NULL) returns NULL.
```

`NewHMACSHA256ScalarFunction(key)` returns the deterministic VARCHAR function `data_plane_hmac_sha256`, with lowercase hexadecimal output and NULL propagation. An empty key is rejected; the helper copies key bytes into the DB-scoped function definition. Register the returned definition in `Config.ScalarFunctions`.

Runnable source/UDF usage and lifecycle cases are covered by [table function tests](../pkg/gosql/table_function_test.go), [UDF tests](../pkg/gosql/scalar_udf_error_test.go) and [HMAC tests](../pkg/gosql/scalar_sql_test.go).

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

- `QueryContext`/`ExecContext` and prepared statements support `?`, `$1`/`$2`, or `$name` value parameters in SELECT, CREATE VIEW/CTAS/EXTERNAL TABLE/SECRET, COPY, DESCRIBE and SET statements. Do not mix placeholder styles. Numbered placeholders must be contiguous from `$1`; named placeholders use `sql.Named` and can be repeated.
- Parameters are bound as AST values, never interpolated into SQL. Supported values are `nil`, bool, int64, float64, string, []byte and time.Time, including the standard `database/sql` conversions and `driver.Valuer`. A time.Time parameter preserves nanoseconds as TIMESTAMP_NS and must fit that type's range. An untyped NULL parameter projected directly defaults to nullable VARCHAR.
- `PrepareContext` caches parsed syntax. Each execution binds its own values and plans against a fresh statement/source snapshot; physical plans and query resources are not shared across executions. Parameterized views persist typed values through an AST serializer and fixed column aliases; identifiers cannot be parameterized.
- Transactions are not supported; `Begin`/`BeginTx` return an explicit error, including read-only transactions.
- LIST/STRUCT cells remain explicit errors in database/sql. Native Chunk.Value/List/Struct return owned recursive values, including explicit NULL validity, exact native numerics and copied binary/string payloads; these values may outlive the callback.
- DECIMAL/HUGEINT cross `database/sql` as exact base-10 text. Use an explicit CAST for decimal/wide-integer input, for example `SELECT CAST(? AS DECIMAL(38,2))` with a decimal string.
- `LastInsertId` is not applicable.
- `RowsAffected()` currently returns zero; use `CopyTo` when export row/byte counts are required. `ColumnTypes` exposes database type names and nullability; precision/scale and Go scan-type metadata are not supplied by adapter-specific interfaces.

### Row value representation

| SQL result | Native Chunk | database/sql driver value |
| --- | --- | --- |
| Signed integers | `Int64` | int64 |
| UTINYINT / USMALLINT / UINTEGER | `Uint64` | int64 |
| UBIGINT | `Uint64` | exact decimal string |
| HUGEINT | `Int128` | exact decimal string |
| DECIMAL | `Int64` or `Int128`, unscaled | scaled decimal string |
| FLOAT / DOUBLE | `Float64` | float64 |
| BOOLEAN | `Bool` | bool |
| VARCHAR / BLOB | `String` / `Bytes` | string / copied []byte |
| DATE / timestamps | `Int64` raw temporal values | time.Time in UTC |
| NULL | accessor validity is false | nil; use sql.Null* or nullable destinations |
| LIST / STRUCT | type metadata only | explicit error for non-NULL values |

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

- `upper(VARCHAR)` uses simple Unicode case mapping, including `ß` → `ẞ`, and preserves NULL. `abs` preserves numeric input types, including unsigned integers, HUGEINT and DECIMAL; signed minimum-value overflow returns an error. Untyped `abs(NULL)` returns a nullable BIGINT.
- `round(value[, precision])` supports signed integer, FLOAT/DOUBLE and DECIMAL inputs; unsigned inputs widen to BIGINT/HUGEINT. Precision is INTEGER. Halfway cases round away from zero, negative precision rounds to powers of ten, and exact integer/DECIMAL overflow returns an error. DECIMAL output scale/precision bind from supported constant integer precision, including exact arithmetic/casts and selected pure built-ins such as abs. NULL precision returns the resolved SQLNULL type. Arbitrary UDFs are not evaluated while binding.
- `TRY_CAST(value AS type)` returns NULL on a supported data conversion failure. Input-expression, binding and resource errors still propagate. Supported families include text to scalar types, numeric to integer/float/boolean (including HUGEINT to BOOLEAN), integer/DECIMAL conversions, FLOAT/DOUBLE to DECIMAL, text output and DATE/TIMESTAMP conversions. Floating-to-DECIMAL scales the binary input in DOUBLE precision and rounds halves away from zero; NaN, infinity and out-of-width results fail conversion. Missing kernels remain explicit binding errors. Text-to-integer casts support exact decimal/exponent forms and halves away from zero; text floating overflow yields infinity, while finite DOUBLE-to-FLOAT overflow yields NULL in TRY_CAST. BOOLEAN to floating/DECIMAL and 0x/0b integer text are supported. Temporal infinity is represented explicitly; invalid finite sentinel endpoints still fail conversion; this is not the full DuckDB cast matrix.
- SQL timestamp text casts accept ISO dates (including BC and supported wide years), space/T-separated times, optional seconds and fractional digits, Z, numeric offsets (`+HH`, `+HHMM`, `+HH:MM`, `+HH:MM:SS`) and UTC. Fractions beyond the target precision are truncated. TIMESTAMP/TIMESTAMP_NS validate and ignore offsets; TIMESTAMPTZ applies them, with unzoned text interpreted in UTC. `24:00:00` advances to the next day. DATE accepts validated timestamp text and keeps the written date. CAST rejects malformed or out-of-range timestamps; TRY_CAST returns NULL. Execution and Parquet literal pruning share the parser. Named IANA zones, BC/wide years and infinity are supported. Unzoned TIMESTAMPTZ text uses the session TimeZone (default UTC). Ambiguous local times choose the later occurrence; spring gaps normalize using the pre-transition offset. Non-UTC temporal literal casts remain residual for safe Parquet filtering.
- `date_part(part, value)` supports scalar DATE/TIMESTAMP/TIMESTAMP_NS/TIMESTAMPTZ input: calendar, clock, ISO week/year, epoch, Julian day and UTC timestamp timezone parts. Literal integer part names return BIGINT; epoch, julian, NULL part names and dynamic column names return DOUBLE. Part names may be literals, parameters, supported pure constant string expressions or dynamic expressions. Constant string lists return a STRUCT with the requested part names. INTERVAL and untyped NULL temporal input remain unsupported. TIMESTAMP_NS is rounded to microseconds before extraction, matching the DuckDB 2.0 scalar overload. Typed NULL values propagate. A directly cast NULL temporal constant binds a DOUBLE result even for integer part names; a nullable column retains its normal bound result type.
- Regular expressions use RE2-compatible Go regexp syntax: matches/full_match, replace (first by default, g for all), extract/extract_all, split_to_array and escape. Supported options include c/i/l/m/n/p/s; capture replacement uses backslash-number syntax. Extract supports a constant group-name list for STRUCT output. Invalid patterns/options/replacements fail explicitly; data-path scratch/output uses query memory limits.
- string_split returns LIST, including empty elements and Unicode characters for an empty separator. list_value, list_extract (one-based/negative indices), array_length and struct_extract expose supported nested results. Native Chunk.Value preserves recursive type and NULL information.
- Physical database/sql connections have separate TEMP namespaces and TimeZone/threads settings. TEMP shadows unqualified names; main.name bypasses TEMP, temp.name selects it. Physical connection close releases TEMP snapshots. Use sql.Conn to pin session state; pool reuse follows the physical connection lifetime. SET/RESET supports TimeZone and threads/max_workers; unknown settings fail explicitly.
- read_csv supports auto_type_candidates lists plus dateformat/timestampformat (and underscore aliases) for existing scalar types and documented strftime directives. Explicit schema wins; fallback remains VARCHAR and default inference remains ISO.
- `IN (...)` / `NOT IN (...)` accept expression lists or single-column SELECT subqueries and preserve SQL NULL semantics, including empty sets and a NULL left operand. Subquery membership is lowered to a reusable right set, joins and NULL statistics. `BETWEEN` / `NOT BETWEEN` evaluate the tested value once.
- `LIKE` / `NOT LIKE` support `%`, `_` and an optional empty or single-byte ESCAPE string. The default has no escape character. Matching follows DuckDB 2.0 Unicode semantics: `_` consumes one Unicode code point.
- Non-correlated `EXISTS (SELECT ...)` / `NOT EXISTS` test row presence, including multi-column projections, aggregates, LIMIT and OFFSET. Correlated EXISTS, IN and scalar queries use equality joins or parameter-domain decorrelation. Missing COUNT groups return zero; scalar lookups returning more than one row fail explicitly. See the scope limits below.
- Positional `UNION`, `INTERSECT` and `EXCEPT` support default/explicit DISTINCT and ALL variants. DISTINCT compares complete rows and treats NULLs as equal; ALL preserves the sum, minimum or nonnegative difference of each row's occurrence counts, respectively. Compatible integer types widen exactly; floating-point branches may select a floating result type, and DATE/TIMESTAMP inputs can select a common temporal type. Branch casts preserve inner DISTINCT, ORDER BY and LIMIT boundaries.
- `INTERSECT` has higher precedence than `UNION`/`EXCEPT`; equal-precedence operations associate left to right. Parentheses preserve branch-local modifiers. Output names come from the left input; trailing ORDER BY/LIMIT/OFFSET applies to the full result. Bare ORDER BY aliases can resolve names from other branches, with left-side names taking precedence; computed order expressions use the result's left-side names and execute after set de-duplication. UNION BY NAME and UNION ALL BY NAME align case-insensitive output names, retain left-then-new-right order and fill absent values with typed NULL; duplicate names in a branch fail. Existing nested-result operator restrictions still apply.
- `COUNT/SUM/AVG/MIN/MAX(DISTINCT expression)` support one scalar argument and retain their normal NULL, empty-input, result-type and overflow behavior. COUNT/SUM/AVG share a sort-and-mark input for the same DISTINCT argument; MIN/MAX do not require a de-duplication pass. Different DISTINCT arguments may add sorting and temporary I/O.
- `JOIN USING (column, ...)` supports merged unqualified keys and each side's original qualified keys, including outer joins. SELECT * retains the left input's column order and omits duplicate right keys. Compatible numeric join keys are materialized in their common type before hashing.
- Derived SELECT inputs support explicit `LATERAL` and implicit references to preceding FROM inputs, including comma/CROSS/INNER joins and LEFT JOIN. Chained inputs, local aggregates, constant per-parameter row limits, prepared values and derived column aliases are supported. Dependent RIGHT/FULL/SEMI/ANTI joins and aggregates owned by a sibling input are rejected, as in DuckDB 2.0.0.

Parameter-domain correlation supports non-equality/OR predicates, constant per-parameter LIMIT/OFFSET with ORDER BY, nested expression-subquery references across query levels, global aggregate expressions/HAVING, and references to grouping columns from outer aggregate outputs/HAVING. Domains propagate through derived tables/CTEs, positional set operations (including ALL) and JOINs, including outer/semi/anti joins and expression subqueries in ON. Pure outer aggregate calls are evaluated by their owning scope; child scalar cardinality and grouping legality are still checked. Non-constant correlated LIMIT/OFFSET and correlation pushed through SAMPLE remain rejected. Sampling an independent source before a correlated filter is supported. Quantified ANY/ALL/SOME comparisons support correlated and non-correlated subqueries, including empty sets and NULL. Numeric range/generate_series and LIST unnest accept constant or lateral arguments. File readers retain the column-argument restriction also present in DuckDB 2.0; supported constant string expressions can form their paths. Plans without usable hash keys currently use a cross product/filter and retain the CROSS build side in managed memory. See [subquery lowering](architecture/subquery-lowering.md).

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
