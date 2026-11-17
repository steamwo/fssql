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
| Data sources and COPY destinations | `Config.LocalFiles`, `Config.S3`, `Config.FTP`, `Config.SFTP` and `Config.WebDAV` register explicitly configured drivers. Single-file COPY uses the configured protocol's strict or explicitly selected compatible publication contract. Unconfigured schemes fail explicitly. TempDir/ManifestDir configure spill and recovery state. |
| Other protocol adapters | `Config.FTP` (FTP/implicit-TLS FTPS), `Config.SFTP` (OpenSSH), and `Config.WebDAV` (WebDAV/WebDAVS) enable endpoint-scoped reads and recoverable single-file COPY. Strict prerequisites and compatible limitations are documented below. Custom VFS registration remains internal. |
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

`gosql.Config` exposes bounded engine memory, temporary storage, spill policy, catalog/cache capacities, worker/queue limits, local spill/COPY manifest roots, and optional local-file/S3/FTP/SFTP/WebDAV/HTTP configuration. Zero values select defaults:

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
| `FTP`, `SFTP`, `WebDAV` | nil; no corresponding remote data driver registered |
| `HTTP` | empty; no HTTP/HTTPS data drivers registered |
| `TableFunctions`, `ScalarFunctions` | empty; built-ins only |
| `ParquetKeys` | nil; no application-provided export key resolver |

MemoryUsed and MemoryLimitBytes describe engine-managed memory, not total process RSS. Configuring local spill or manifest paths does not enable reading or exporting local data files; configure `LocalFiles.RootDir` explicitly for local data access.

`SpillMemoryBudgetBytes` is an operator policy, not a separate memory allowance; hash joins use an effective minimum budget of 4096 bytes. All managed allocations still share the DB memory limit. CROSS build inputs remain resident, and some stateful window/sampling plans execute serially even when `Workers` is higher. Caller allocations inside source/UDF callbacks are outside managed-memory accounting.

`DB.Close` prevents new work and releases configured remote pools and idle HTTP connections. It cancels outstanding HTTP source requests. Leased FTP/SFTP sessions close when returned; subsequent acquisitions fail, so callers should finish or cancel remote queries before closing the DB. Query calls retain their own query-scoped context/ownership and also observe their supplied context.

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

### Remote protocols

The same configuration works with `Open` and `OpenSQLDB`. Each optional config binds one endpoint; a query URI must use the same scheme and exact host/port spelling. Endpoints must be URLs without user info, data paths, queries or fragments. Put the data path in the query URI. `Open` validates configuration without connecting; DNS, server authentication, SSH executable/file availability and protocol capability checks occur when querying.

```go
sqlDB, err := gosql.OpenSQLDB(gosql.Config{
    FTP: &gosql.FTPConfig{
        Endpoint: "ftps://ftp.example.com:990", // implicit TLS
        Username: "reader",
        Password: os.Getenv("FTP_PASSWORD"),
    },
    SFTP: &gosql.SFTPConfig{
        Endpoint:       "sftp://sftp.example.com:22",
        Username:       "reader",
        KnownHostsFile: "/app/ssh/known_hosts",
        IdentityFile:   "/app/ssh/id_ed25519",
    },
    WebDAV: &gosql.WebDAVConfig{
        Endpoint: "webdavs://dav.example.com",
        Username: "reader",
        Password: os.Getenv("WEBDAV_PASSWORD"),
    },
})
if err != nil { return err }
defer sqlDB.Close()

rows, err := sqlDB.QueryContext(ctx,
    "SELECT * FROM read_csv(?, header=true)",
    "sftp://sftp.example.com:22/data/input.csv")
if err != nil { return err }
defer rows.Close()
// Consume rows and check rows.Err().
```

- `FTPConfig.Endpoint` accepts `ftp://` (port 21) and `ftps://` (implicit TLS, port 990). Plain FTP requires `AllowPlainFTP: true`; explicit AUTH TLS is not implemented. `MaxSessions` defaults to 4 and `MaxAttempts` to 2.
- `SFTPConfig` requires `Username` and `KnownHostsFile`. It invokes `SSHBinary` (default `ssh`) with strict host-key checking and batch mode, using `IdentityFile` or the SSH agent. `ConnectTimeout` defaults to 15 seconds, `MaxSessions` to 4 and `MaxAttempts` to 2. Password prompts are disabled. Network access follows OpenSSH configuration; `RemoteNetworkConfig` applies to FTP/WebDAV/HTTP.
- `WebDAVConfig.Endpoint` accepts `webdav://` (HTTP, port 80) and `webdavs://` (HTTPS, port 443). Authentication uses `Username` and `Password` as Basic Auth. Range reads require a server that returns exact HTTP 206 responses.
- FTP and WebDAV accept `Network: gosql.RemoteNetworkConfig{AllowPrivate: true}` for private servers; `AllowLoopback` and `AllowLinkLocal` are independent opt-ins. Default policy rejects all three address classes. The configured endpoint port is the only permitted control/HTTP port; FTP passive data connections remain bound to the validated server IP.
- `FTPConfig.TLSConfig` and `WebDAVConfig.TLSConfig` accept a standard `*tls.Config`, for example with custom `RootCAs`. The configuration is cloned when opening the DB; normal certificate verification applies by default. Credentials are supplied through configuration rather than URI user info or SQL.

Parquet/CSV/JSON/NDJSON readers use the same registered VFS drivers. Available operations remain bounded by server capabilities (for example FTP REST STREAM and MLSD). Single-file SQL COPY and `CopyTo` use staged publication and v2 recovery manifests. Strict mode is the default and never downgrades automatically.

| Protocol | Strict publication | Explicit compatible publication |
| --- | --- | --- |
| SFTP | SFTP v3 handshake must advertise `hardlink@openssh.com` version `1`. Exclusive staging is created in the final directory; a hardlink publishes without replacing an existing destination. The server must support links on that filesystem. Extension/version are persisted and checked again on recovery. | Exclusive staging followed by standard SFTP rename. No POSIX overwrite-rename fallback; strict no-overwrite is not guaranteed across server implementations. |
| WebDAV/WebDAVS | Set `WebDAVConfig.AtomicMoveVerified: true` only after validating both atomic same-directory MOVE and no-overwrite behavior under concurrent destination creation. Uses `MOVE Overwrite: F`. OPTIONS/DAV headers and a successful rename alone do not prove these guarantees. | Uses `MOVE Overwrite: F`, subject to server behavior. Default concurrent Apache `mod_dav_fs` can overwrite a competing destination despite this header. Crash-safe atomic visibility is also deployment dependent. |
| FTP/FTPS | Rejected before creating staging. | STOR a unique same-directory staging name, check target absence, then RNFR/RNTO. There is a TOCTOU window: if another client creates the target after the check, a server whose rename overwrites may replace that competing file. |

WebDAV publishing first attempts conditional staging creation with an infinite exclusive depth-zero DAV lock. Only an explicit LOCK 405/501 selects the conditional PUT protocol, which requires a strong ETag in its creation response. Staging upload, MOVE and deletion remain conditional. FTP publishing requires SIZE and an exact parent MLSD listing to distinguish a missing target from a permission-related 550; full-byte verification uses RETR without requiring REST. SFTP requires exclusive OPEN and ordinary file reads/removal. No publication path deletes the final destination before rename. Staging creation/publish requests are not replayed implicitly after an unknown response, and unknown staging ownership is retained as `cleanup_pending` for operator inspection rather than guessed cleanup.

For a configured FTP/FTPS endpoint, opt into its compatible contract explicitly:

```go
result, err := db.CopyTo(ctx, querySQL, "ftps://ftp.example.com:990/data/output.csv", gosql.CopyOptions{
    Format: gosql.CopyFormatCSV,
    PublishMode: gosql.CopyPublishCompatible,
})
```

The SQL equivalent uses `(FORMAT csv, PUBLISH_MODE 'compatible')`. Keep `ManifestDir` across DB/process restarts and use the returned/error JobID with `RecoverExport`; do not repeat COPY to resolve an uncertain publication. See [Export recovery](#export-recovery).

### HTTP and HTTPS sources

```go
sqlDB, err := gosql.OpenSQLDB(gosql.Config{
    HTTP: []gosql.HTTPConfig{
        {
            Endpoint:    "https://data.example.com",
            BearerToken: os.Getenv("DATA_TOKEN"),
            // Empty FallbackPolicy defaults to HTTPFallbackMaterialize.
        },
        {
            Endpoint:       "https://archive.example.com:8443",
            Headers:        map[string]string{"X-API-Key": os.Getenv("ARCHIVE_KEY")},
            FallbackPolicy: gosql.HTTPFallbackRequireRange,
        },
    },
})
if err != nil { return err }
defer sqlDB.Close()
rows, err := sqlDB.QueryContext(ctx, "SELECT * FROM read_csv(?, header=true)", signedURL)
if err != nil { return err }
defer rows.Close()
```

HTTP sources work with CSV, JSON, NDJSON and Parquet through Native API and `database/sql`, including JOINs across configured endpoints. URLs need no file suffix. Raw path escaping, repeated slashes, query order, duplicate parameters, empty values and an explicit empty query separator are preserved. Query wildcard characters do not trigger glob expansion. URL path globs, directory listing, URI userinfo, fragments and HTTP export destinations are unsupported.

Each `HTTPConfig.Endpoint` must be `http(s)://host[:port]` without a path (including `/`), query, userinfo or fragment. Default ports are normalized when rejecting duplicate origins. HTTP and HTTPS require separate authorization. Unconfigured origins are denied, including redirect destinations. `Open` validates and copies the endpoint list and headers without DNS or remote requests.

Authentication is anonymous, Basic (`Username`/`Password`), Bearer (`BearerToken`), or fixed custom headers for API keys. Basic and Bearer cannot be combined. `Authorization`, `Host`, `Proxy-Authorization`, Range/conditional headers, `Accept-Encoding`, hop-by-hop/connection headers and `Content-Length` cannot be customized. Redirects are manual, limited to five, reject HTTPS-to-HTTP downgrades, and regenerate authentication and custom headers from each target endpoint's own configuration. Cookies and environment proxies are disabled. DNS, IP and the configured port are checked at every hop and again at actual dialing; connections use the validated IPs. Private, loopback and link-local addresses require independent `Network` opt-ins.

`MaxConnections` defaults to 8 per origin. `RequestTimeout` defaults to 0, adding no request deadline beyond the caller's context. Positive timeouts cover connection-slot waits, DNS, requests and response bodies. TCP connect and TLS handshake timeouts are 10 seconds; the response-header timeout is 30 seconds. `TLSConfig` is stored with `Clone()`, which is a **shallow copy**: replacing caller top-level fields is isolated, but certificate pools, certificates, callbacks and captured state may remain shared. Do not mutate shared objects after `Open`; dynamic TLS callbacks must be concurrency-safe.

Reading starts with `GET Range: bytes=0-0`, without requiring HEAD. A valid identity-encoded 206 with a known total and strong ETag creates a pinned remote snapshot; every subsequent range uses `If-Match` and verifies the exact interval, total, ETag and body length. Later redirects, 200, 412 or identity/size changes fail the query instead of switching versions.

`HTTPFallbackMaterialize` reuses an initial complete 200 body, or issues one full GET after a valid partial response lacks a strong validator/known total or uses HTTP gzip. Partial gzip bytes are validated as a range response and closed, never handed to a gzip decoder. Complete responses stream through a 64 KiB managed buffer into query temporary storage, reserving quota before each write; all materialized bytes count toward `TempLimitBytes`. `HTTPFallbackRequireRange` closes responses lacking stable physical Range with a capability error without downloading the full object. A 416 with `Content-Range: bytes */0` is a valid empty object for either policy. Protocol, authentication, network and TLS errors never trigger a whole-object retry.

Materialized logical views have content SHA-256 SnapshotIDs, query-local sharing and cleanup on cancellation, bind/runtime failure and early Rows.Close. HTTP footers never enter the shared cross-query Parquet cache. Diagnostics redact query values; different signed URLs retain separate identities. HTTP `Content-Encoding` supports identity and gzip; br, unknown or stacked transport encodings fail explicitly. Go's automatic decompression remains disabled.

### Compressed text sources

`read_csv`, `read_json`, `read_ndjson`, `read_csv_auto` and `read_json_auto` accept `compression='auto'|'none'|'gzip'|'zstd'`. The default is `auto`; options may also be bound as `database/sql` value parameters. `read_json_auto(..., format='newline_delimited', compression='gzip')` retains its existing NDJSON alias behavior.

```sql
SELECT * FROM read_csv('s3://bucket/no-suffix', header=true, compression='auto');
SELECT * FROM read_json('https://data.example.com/signed?token=...', compression='gzip');
SELECT * FROM read_ndjson('file:///data/events.zst', compression='zstd');
```

File compression uses content magic, independently of suffix, Content-Type or protocol. It applies uniformly to local, S3, FTP/FTPS, SFTP, WebDAV/WebDAVS and HTTP/HTTPS inputs, including mixed compressed/plain files selected by a local or remote glob. `none` disables file decoding; an explicit gzip/zstd mode requires matching magic. Concatenated gzip members, concatenated zstd frames and multiple zstd skippable-frame prefixes are supported. Decoder checksum and truncation errors fail the query before publishing a SnapshotID.

The resolver first decodes HTTP Content-Encoding, then sniffs and decodes optional file compression, then writes **one** logical temporary object. HTTP gzip plus a gzip or zstd file therefore does not create an intermediate temporary file. The SnapshotID hashes the final logical bytes; the physical ETag/version remains separate. Compressed Content-Length does not become logical Size or the decoded quota reservation. `TempLimitBytes` applies to actual decoded bytes, including decompression expansion; quota exhaustion stops input and removes incomplete files.

Decoder workspaces are reserved against `MemoryLimitBytes` before allocation: gzip reserves 1 MiB per decoding layer; zstd reserves a 1 MiB base plus its bounded window (up to 8 MiB), including transient window growth. A 4-byte managed magic prefix and managed copy/range buffers are separately charged. Unknown lengths, streaming reads, cancellation, bind/runtime failure and early close share query resource cleanup. Identical view specifications initialize once per query; `auto`, `none`, `gzip` and `zstd` retain separate identities and read routes. Uncompressed RandomRead objects in auto mode retain remote/local range access after the small magic probe.

`HTTPFallbackRequireRange` still permits file gzip/zstd decoding and temporary materialization when the underlying HTTP object has stable Range support. `compression='none'` does not disable HTTP gzip decoding. Parquet supports HTTP gzip materialization, while whole-file `.parquet.gz`/`.parquet.zst` wrappers remain unsupported; existing Parquet page compression is unchanged.

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
    ParquetRowGroupRows: 8192,
    ParquetPageRows: 2048,
})
if err != nil { return err }
// result.JobID, result.Rows, result.Bytes and result.Columns describe committed output.
```

`destination` must use an enabled local/S3/FTP/FTPS/SFTP/WebDAV/WebDAVS driver and satisfy its selected publication mode. `CopyFormatCSV`, `CopyFormatJSON`, `CopyFormatNDJSON` and `CopyFormatParquet` select the format. Text output compression is `identity` (default), `gzip` or `zstd`; Parquet uses page compression `snappy` (default) or `identity`. Explicit `none` also disables compression. These defaults apply to both `CopyTo` and SQL COPY. CSV `Header` defaults to true; `Delimiter` and `Quote` configure single-byte separators/quoting. A failure returns a zero `ExportResult`; successful return follows final-object commit through the same export path as SQL COPY.

`CopyOptions.ParquetRowGroupRows` and `ParquetPageRows` configure Parquet row-group/page boundaries independently of input chunks, including encrypted output. Zero selects the existing defaults: 2048 rows per group and 512 rows per page (clamped to smaller groups). Positive page rows must not exceed the effective group rows; negative sizes and layout options on text formats are rejected before opening output. Larger groups use the same DB memory budget and may fail when it is insufficient. These options apply to `CopyTo`; SQL COPY retains its existing layout defaults.

Parquet export automatically considers `RLE_DICTIONARY` for low-cardinality VARCHAR/BLOB column chunks, including encrypted output. Dictionaries are limited to 256 entries and 32 KiB of PLAIN dictionary bytes; small chunks, high cardinality, insufficient estimated savings or a preparation-time memory limit retain PLAIN encoding. Encoding selection is internal and does not change SQL values, NULL handling or the compression option. Page/column statistics remain available independently of encoding.

For encrypted Parquet export, set `CopyOptions.Encryption` to `ParquetEncryptionOptions{Algorithm: gosql.ParquetAES256GCM, KeyID: keyID}` and provide `Config.ParquetKeys`. The resolver receives `(context.Context, keyID, algorithm)` and must return 16 bytes for `ParquetAES128GCM` or 32 bytes for `ParquetAES256GCM`. SQL/options/manifests carry the key identity, not key bytes. This is an export API; it does not expose a general encrypted-file read key provider.

### Export recovery

Local/S3 and remote file-protocol single-file `CopyTo` and SQL COPY write v2 manifests under `Config.ManifestDir`. Each invocation generates a fresh random 32-character lowercase hexadecimal JobID; `ExportResult.JobID` identifies a successful typed export. Format and compression tails finish before the protocol Seal; durable PREPARED and COMMITTING snapshots precede publication. The final file is verified against the physical output size and SHA-256 before COMMITTED is saved. Verification uses managed 64 KiB buffers and reads raw output, including compression/encryption, without applying source decompression.

`CopyOptions.PublishMode` accepts `CopyPublishStrict` (default) and `CopyPublishCompatible`; SQL uses `PUBLISH_MODE 'strict'` or `'compatible'`. Both retain Local/S3 atomic no-replace publication; an existing target conflicts even when its contents match. Strict never silently downgrades. Remote protocol prerequisites and compatible limitations are listed in [Remote protocols](#remote-protocols).

SFTP/FTP/WebDAV staging names are fixed in WRITING intent before creation. Successful creation is durably recorded before accepting payload. Seal finishes the private stream, records its identity and verifies its complete physical size/SHA-256; it never renames, links or moves the final target. Publication and recovery use the same coordinator. Staging verification may perform several full reads before publication, followed by a complete final read, so budget network bandwidth and read permissions as well as upload permissions. Protocol/metadata workspace reserves 512 KiB per active file-publication handle, with a separate managed 64 KiB readback buffer. Zero-byte files use the same staging lifecycle.

WebDAV publication first requests `LOCK If-None-Match: *`, `Depth: 0`, and `Timeout: Infinite`. The grant must be an infinite exclusive write lock; finite leases are rejected before payload writes. The manifest stores only a SHA-256 digest of the lock token. Recovery reauthorizes current credentials, retrieves DAV:lockdiscovery, matches that digest, and uses a tagged `If` condition on staging operations. A missing, replaced or administratively broken lock cannot authorize staging publication or deletion; cleanup remains pending. A lost LOCK reply also leaves unknown ownership for administrator cleanup. MOVE removes the source lock; Abort never unlocks or deletes the final object. Only LOCK 405/501 selects the older strong-ETag creation protocol; later HEAD cannot replace its creation proof. Existing intents retain their original strong-ETag protocol.

Default Apache 2.4.65 `mod_dav_fs` is supported in compatible mode through DAV locks, including its missing creation ETag and omitted DAV:lockroot. Its concurrent MOVE implementation has an observed destination overwrite window. Strict mode therefore requires a separately verified deployment, for example request serialization combined with same-filesystem atomic rename and exclusive access through that server; the default concurrent configuration must keep `AtomicMoveVerified` false. Weak ETags are never submitted as HTTP If-Match, and weak-to-strong promotion of the same opaque tag is accepted during full-byte publication verification.

WebDAV recovery checks the currently configured credential scope and, for committed jobs, the recorded final ETag. SFTP/FTP servers generally expose only size and second-resolution modification time: these are checked before/after physical readback, but cannot identify a same-byte replacement or every concurrent rewrite. Full SHA-256, rather than those weak metadata fields, verifies content. Recovery also requires the original protocol/mode/endpoint/security scope; changing the SFTP extension/version or WebDAV atomicity declaration invalidates the old token.

```go
result, err := db.CopyTo(ctx, querySQL, destination, gosql.CopyOptions{
    Format: gosql.CopyFormatCSV,
    PublishMode: gosql.CopyPublishStrict,
})
if err != nil {
    var uncertain *gosql.CommitUncertainError
    if errors.As(err, &uncertain) {
        // Preserve the same ManifestDir and reopen with the target driver enabled.
        _, recoverErr := db.RecoverExport(ctx, uncertain.JobID)
        return recoverErr
    }
    return err
}
_, err = db.RecoverExport(ctx, result.JobID) // read-only verification after commit
```

`errors.Is(err, gosql.ErrCommitUncertain)` means publication may have taken effect; the error exposes `JobID` through `CommitUncertainError`. It preserves COMMITTING evidence and staging. Other errors after ownership creation expose `ExportJobError.JobID`; having a JobID alone does not mean a commit is uncertain. `ErrCommitConflict` reports a known target conflict/content mismatch; `ErrExportNotRecoverable` reports missing or unsealed evidence. Recovery never binds or reruns the query and does not need the original source snapshot; it uses an independent managed memory scope and currently configured target authorization.

Only PREPARED/COMMITTING jobs may resume publication. WRITING/FAILED jobs may clean proven job-owned staging but cannot publish. Unknown ownership remains `cleanup_pending`. COMMITTED recovery only verifies the final object and retries staging cleanup: a missing final returns `ErrExportNotRecoverable`, changed bytes or recorded stable identity return `ErrCommitConflict`, and neither recreates the target. Cleanup failure after durable COMMITTED preserves success and leaves `cleanup_pending` for a later call. Cleanup never deletes final output.

S3 multipart parts use the final key but remain unpublished until CompleteMultipartUpload. Ownership snapshots record UploadID and confirmed part number, ETag, size and available SHA/CRC checksums before further payload actions. Seal fixes the complete part list and full physical SHA-256 without completing the upload. Recovery first verifies an existing final object; when explicitly absent, it reconciles all sealed parts with paginated ListParts before continuing. A completed UploadID may have disappeared, so final verification does not depend on it. Complete success is determined from its full XML response; HTTP 200 can contain an Error. See the [AWS CompleteMultipartUpload contract](https://docs.aws.amazon.com/AmazonS3/latest/API/API_CompleteMultipartUpload.html).

Actual zero-byte output uses a sealed `empty_put` descriptor; only COMMITTING publication issues its conditional PUT. Zero rows may still emit a CSV header, JSON array, compressed frame or Parquet footer and therefore use multipart. S3 final verification pins VersionID where available, checks the current target identity, and reads the complete physical output; ETag and multipart/composite checksums do not replace the full SHA-256. This verification incurs a full-object download. Recovery requires current target credentials and permissions for these reads, part listing and exact-upload cleanup. Recovery tokens are bounded to 4 MiB and metadata/part buffers use the DB memory budget.

An unknown UploadID after a lost create response cannot be safely recovered by listing and aborting every upload for the final key. Such a job retains `cleanup_pending` and needs a deployment lifecycle rule such as AbortIncompleteMultipartUpload to reclaim the unknown upload. Known uploads are cleaned only by their recorded UploadID. Create and final publish requests are not implicitly replayed after a lost response; an uncertain publication is reconciled using the same JobID.

Recovery accepts new 32-character v2 IDs and legacy 64-character lowercase hexadecimal v1 IDs, validates the manifest's internal ID/version, and rejects paths or other encodings. V1 recovery only confirms an already visible file with reliable recorded identity; local v1 also requires its full recorded SHA-256. It does not fabricate staging tokens or upgrade the manifest. Normal writes and recovery hold the same cancelable OS job lock through reload, publication, verification and state updates; persistent `.lock` files are intentional. Coordination is limited to a local ManifestDir shared by processes on one host, not distributed hosts or unverified network filesystems.

Runnable Go examples for preserving error JobIDs and recovering with a fresh context/DB are in [remote_export.go](../examples/quickstart/remote_export.go).

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
