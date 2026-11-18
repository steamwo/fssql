# fssql

**English** | [简体中文](README.md)

fssql is an **embeddable read-only SQL query, transformation, and export engine written in Go**. It is designed for analytical workloads over local files, object storage, and application-provided data: read data, query or transform it with SQL, then export the result to common data formats.

The engine uses its own Vector/DataChunk representation and push-based execution pipeline, with bounded memory, temporary storage, and spill support. The public Go package is `github.com/steamwo/fssql/pkg/gosql`, with both a native Chunk API and a `database/sql` interface.

## Project focus

fssql focuses on a deliberately narrow data-processing path:

**Read data → Query / transform with SQL → Export results**

- **Input formats**: CSV, JSON, NDJSON, and Parquet.
- **Storage**: local files, S3-compatible object storage such as MinIO, FTP/FTPS, SFTP, and WebDAV; HTTP/HTTPS sources are also supported.
- **Analytical SQL**: filtering, projection, aggregation, JOINs, window functions, subqueries, set operations, CTEs, and related features.
- **Output formats**: CSV, JSON, NDJSON, and Parquet.
- **Go embedding**: native Chunk callbacks or standard `database/sql`.
- **Resource control**: configurable memory, temporary storage, spill policy, workers, and queue limits.
- **Extensibility**: Go table functions, scalar UDFs, HMAC helper functions, and materialized snapshots.

fssql is **not intended to replace DuckDB or become a general-purpose database**. DuckDB is primarily used as a reference for SQL semantics, file-format interoperability, and performance measurement.

The current priority is read-only querying, transformation, and reliable export. Transactions, mutable persistent tables, and `UPDATE` / `DELETE` / `MERGE` are not primary goals at this stage; they may be reconsidered later if real use cases justify the additional complexity.

## Quick start

Go 1.22 or later is required.

```sh
go get github.com/steamwo/fssql/pkg/gosql
```

Minimal example using `database/sql`:

```go
package main

import (
    "context"
    "fmt"
    "log"

    "github.com/steamwo/fssql/pkg/gosql"
)

func main() {
    db, err := gosql.OpenSQLDB(gosql.Config{})
    if err != nil {
        log.Fatal(err)
    }
    defer db.Close()

    var answer int64
    if err := db.QueryRowContext(
        context.Background(),
        "SELECT 42 AS answer",
    ).Scan(&answer); err != nil {
        log.Fatal(err)
    }

    fmt.Println(answer)
}
```

Running the program prints:

```text
42
```

The repository also includes a complete quick-start example that creates temporary CSV, JSON, NDJSON, and Parquet files and cleans them up automatically:

```sh
go run ./examples/quickstart
```

See [examples/quickstart/README.md](examples/quickstart/README.md) for the walkthrough.

## Query local CSV, JSON, and Parquet

Local file access must be explicitly enabled with an absolute root directory.

```go
root, err := filepath.Abs("data")
if err != nil {
    return err
}

sqlDB, err := gosql.OpenSQLDB(gosql.Config{
    LocalFiles: &gosql.LocalFileConfig{RootDir: root},
})
if err != nil {
    return err
}
defer sqlDB.Close()

uri, err := gosql.FileURI(filepath.Join(root, "orders.parquet"))
if err != nil {
    return err
}

rows, err := sqlDB.QueryContext(ctx, `
    SELECT region, sum(amount) AS total
    FROM read_parquet(?)
    GROUP BY region
    ORDER BY region
`, uri)
```

Common read functions:

| Data source | Example |
| --- | --- |
| CSV | `SELECT * FROM read_csv(?, header=true)` |
| JSON array | `SELECT * FROM read_json(?)` |
| NDJSON | `SELECT * FROM read_ndjson(?)` |
| Parquet | `SELECT * FROM read_parquet(?)` |
| Parquet glob | `SELECT * FROM read_parquet(?)` with a `*.parquet` URI |
| Schema inspection | `DESCRIBE SELECT * FROM read_parquet(?)` |

Parameters bind values, not table names, column names, or arbitrary SQL fragments.

## SQL capabilities

The engine supports analytical SQL features including:

- filtering, projection, ordering, LIMIT and Top-N;
- grouped and global aggregation;
- JOINs;
- window functions;
- non-recursive CTEs;
- scalar and correlated subqueries;
- `EXISTS`;
- `UNION`, `INTERSECT`, and `EXCEPT`, including supported `ALL` variants;
- `UNION BY NAME`;
- explicit and implicit LATERAL derived-table joins;
- sampling;
- common string, date/time, numeric, and regular-expression functions.

For the supported Go and SQL entry points and known deviations, see the [public API](docs/public-api.md) and [compatibility deviations](compatibility/deviations.yaml).

## Export query results

Structured export is available through `DB.CopyTo`.

```go
db, err := gosql.Open(gosql.Config{
    LocalFiles: &gosql.LocalFileConfig{RootDir: root},
})
if err != nil {
    return err
}
defer db.Close()

destination, err := gosql.FileURI(filepath.Join(root, "result.parquet"))
if err != nil {
    return err
}

result, err := db.CopyTo(
    ctx,
    "SELECT id FROM range(3) AS t(id)",
    destination,
    gosql.CopyOptions{
        Format:      gosql.CopyFormatParquet,
        Compression: "snappy",
    },
)
if err != nil {
    return err
}

fmt.Printf("rows=%d bytes=%d\n", result.Rows, result.Bytes)
```

Supported export formats include:

| Format | API value | Compression |
| --- | --- | --- |
| CSV | `CopyFormatCSV` | identity / gzip / zstd |
| JSON | `CopyFormatJSON` | identity / gzip / zstd |
| NDJSON | `CopyFormatNDJSON` | identity / gzip / zstd |
| Parquet | `CopyFormatParquet` | snappy / identity |

SQL `COPY` uses the same configured local, S3, FTP/FTPS, SFTP, or WebDAV driver as `CopyTo`. Publication defaults to strict mode; FTP/FTPS requires an explicit `CopyPublishCompatible`. SFTP strict requires `hardlink@openssh.com=1`; WebDAV strict requires a deployment verified for atomic MOVE and concurrent no-overwrite behavior. Default concurrent Apache must use compatible mode, which can overwrite a competing destination. See the [protocol guarantees](docs/public-api.md#remote-protocols).

Preserve the returned JobID and `ManifestDir`. After `ErrCommitUncertain`, call `db.RecoverExport(ctx, jobID)` to reconcile the existing export without rerunning SQL. HTTP/HTTPS sources and gzip/zstd text input use the same query engine and resource limits; configuration and boundaries are documented in the [public API](docs/public-api.md).

## S3 / MinIO

S3 support is opt-in through `gosql.Config`.

```go
sqlDB, err := gosql.OpenSQLDB(gosql.Config{
    S3: &gosql.S3Config{
        Endpoint:          "http://127.0.0.1:9000",
        Region:            "us-east-1",
        ForcePathStyle:    true,
        AllowInsecureHTTP: true,
        AccessKey:         os.Getenv("AWS_ACCESS_KEY_ID"),
        SecretKey:         os.Getenv("AWS_SECRET_ACCESS_KEY"),
        SessionToken:      os.Getenv("AWS_SESSION_TOKEN"),
    },
})
```

Then query S3 objects directly:

```go
var count int64
err = sqlDB.QueryRowContext(
    ctx,
    "SELECT count(*) FROM read_parquet(?)",
    "s3://my-bucket/orders.parquet",
).Scan(&count)
```

S3 URIs can also be used as export destinations when the S3 driver is configured.

## Go APIs

### `database/sql`

`gosql.OpenSQLDB` returns a standard `*sql.DB`. It supports value parameters, prepared statements, context cancellation, and the same query engine.

Supported placeholder styles are `?`, contiguous `$1` / `$2`, or named `$name`. Do not mix styles within a statement.

### Native Chunk API

For batch-oriented result processing, use `gosql.Open` and `QueryChunks`.

```go
db, err := gosql.Open(gosql.Config{})
if err != nil {
    return err
}
defer db.Close()

columns, err := db.QueryChunks(
    ctx,
    "SELECT id FROM range(3) AS t(id) ORDER BY id",
    func(chunk *gosql.Chunk) error {
        for row := 0; row < chunk.RowCount(); row++ {
            id, valid, err := chunk.Int64(row, 0)
            if err != nil {
                return err
            }
            if valid {
                fmt.Println(id)
            }
        }
        return nil
    },
)
_ = columns
```

A Chunk is borrowed and is valid only during the callback. Owned nested values can be obtained through the native value APIs where supported.

## Go table functions and scalar UDFs

Application data can be exposed as a SQL source with `Config.TableFunctions`. Scalar Go logic can be registered with `Config.ScalarFunctions`.

This allows applications to combine file data, S3 data, and application-owned streams under the same SQL query layer without turning fssql into a storage database.

See [docs/public-api.md](docs/public-api.md) and [examples/quickstart/functions.go](examples/quickstart/functions.go) for complete examples.

## Resource control

A zero-value configuration currently uses:

| Setting | Default |
| --- | ---: |
| Managed memory | 256 MiB |
| Temporary storage | 1 GiB |
| Spill memory budget | 16 MiB |
| Workers | 1 |

`MemoryUsed()` reports engine-managed memory, not process RSS or the entire Go heap.

Queries accept `context.Context`; cancellation, failures, and early result closure enter the engine resource-cleanup path.

## Current public API boundary

| Capability | Current public entry point |
| --- | --- |
| Native query | `Open` → `QueryChunks` |
| Row-oriented query | `OpenSQLDB` → `*sql.DB` |
| Statements / COPY | `DB.Exec` |
| Structured export | `DB.CopyTo` |
| Local files | `Config.LocalFiles` |
| S3 / MinIO | `Config.S3` |
| HTTP / HTTPS sources | `Config.HTTP` |
| FTP / implicit-TLS FTPS | `Config.FTP` |
| SFTP | `Config.SFTP` |
| WebDAV / WebDAVS | `Config.WebDAV` |
| Export recovery | `DB.RecoverExport` |
| Go table functions | `Config.TableFunctions` |
| Go scalar functions | `Config.ScalarFunctions` |
| Catalog | in-memory catalog scoped to the DB instance |
| Resource limits | `gosql.Config` |

The public configuration currently does not expose SQLiteCatalog, custom VFS injection, or an HTTP server constructor, even if some related implementations exist internally.

## Current boundaries and non-goals

fssql is currently optimized around read-only analytical access and export rather than mutable database storage.

Not currently supported as public database features:

- transactions;
- native mutable persistent tables;
- `UPDATE`, `DELETE`, and `MERGE`;
- recursive CTEs.

TEMP tables, views, settings, and other session state are scoped according to the physical connection. Use `sql.Conn` when a workflow depends on session state.

These boundaries are intentional for the current project direction, rather than an attempt to provide the full feature set of a general-purpose database.

## DuckDB as a reference, not a replacement target

DuckDB is used to validate SQL behavior, file interoperability, and performance characteristics. fssql does not aim to match the entire DuckDB feature set.

The [testing standard](docs/testing-standard.md) defines the DuckDB baseline, differential checks, and performance measurement methodology.

## Documentation

- [Public API, defaults, and boundaries](docs/public-api.md)
- [Table functions and source contracts](docs/contracts/table-functions.md)
- [Architecture constraints](ARCHITECTURE.md)
- [Testing standard and DuckDB differential/performance baseline](docs/testing-standard.md)
- [Performance optimization progress](docs/performance-optimization-progress-2026-11-04.md)
- [SQL compatibility deviations](compatibility/deviations.yaml)
- [DuckDB compatibility roadmap](docs/duckdb-compatibility-roadmap.md)

## Development

The repository is organized around `pkg/gosql` (public API), `internal` (engine implementation), `cmd` (commands), `test` (cross-module tests), and `examples` (runnable examples).

Run the quick-start and public package tests with:

```sh
go test ./examples/quickstart ./pkg/gosql
```
