# fssql quickstart

本目录是 `github.com/steamwo/fssql/pkg/gosql` 的完整可运行示例。它按功能分组演示当前公共 API 的全部主要能力，不依赖外部数据源或凭据：运行时会在系统临时目录创建隔离工作区，生成 CSV/JSON/NDJSON/Parquet 文件，结束后自动清理。

```sh
go run ./examples/quickstart
```

也可以在仓库根目录运行测试，校验示例输出与 HMAC 结果：

```sh
go test ./examples/quickstart -count=1
```

## 文件职责

| 文件 | 内容 |
| --- | --- |
| `main.go` | 示例编排、工作区生命周期、共用配置、`database/sql` 打印助手 |
| `native.go` | `DB.QueryChunks` 原生回调与全部 `Chunk` 类型访问器 |
| `sql_api.go` | `OpenSQLDB`、参数绑定、预编译语句、类型映射与事务边界 |
| `sql_features.go` | CTE、JOIN、聚合、窗口、采样、子查询、LATERAL、UNION ALL 等 SQL 能力 |
| `functions.go` | `TableFunction`/`BatchReader`/`BatchWriter`、标量 UDF、HMAC、`Materialize` |
| `files.go` | `LocalFiles`/`FileURI`、读取函数、四种导出格式、SQL COPY、外部表、glob、加密导出 |
| `catalog.go` | CTAS、视图、`DESCRIBE`、`information_schema.columns`、`CREATE SECRET` |
| `config.go` | 资源限制、`MemoryUsed`、`S3Config`、`SampleCompatibility` |
| `boundaries.go` | 未实现语法、零除数、访问器误用、取消与关闭后的错误行为 |

## 逐节说明

### 1. 原生 Chunk 查询与全部类型访问器

- `DB.QueryChunks(ctx, sql, func(*Chunk) error)` 执行生产 Parser → Binder → Planner → Push-Pipeline 路径。
- 回调收到的 `*Chunk` 是借用视图，**只能在回调期间读取**。示例在回调内立即复制值，回调结束后再用返回的 `[]Column` 打印列名、逻辑类型和可空性。
- 访问器与类型严格对应：

| 逻辑类型 | 访问器 | 说明 |
| --- | --- | --- |
| `TINYINT`/`SMALLINT`/`INTEGER`/`BIGINT` | `Int64` | 有符号整数 |
| `DATE` | `Int64` | Unix epoch 起的天数 |
| `TIMESTAMP`/`TIMESTAMPTZ` | `Int64` | 原生整数，单位取决于源逻辑类型 |
| `TIMESTAMP_NS` | `Int64` | 固定纳秒 |
| `UTINYINT`/`USMALLINT`/`UINTEGER`/`UBIGINT` | `Uint64` | 无符号整数 |
| `FLOAT`/`DOUBLE` | `Float64` | 浮点 |
| `BOOLEAN` | `Bool` | 布尔 |
| `VARCHAR` | `String` | 文本 |
| `BLOB` | `Bytes` | 返回副本，可跨回调保留 |
| `HUGEINT`、宽度 > 18 的 `DECIMAL` | `Int128` | `String()` 返回精确十进制 |
| 宽度 ≤ 18 的 `DECIMAL` | `Int64` | 未缩放存储值，按 `Type.Scale` 解释 |

- `IsNull(row, column)` 或访问器的 `valid` 返回值才是 NULL 判断依据；零值不是 NULL。
- `Type(column)` 返回 `gosql.Type`：`ID`、`Width`/`Scale`、`Child`、`Fields`。LIST/STRUCT 只公开类型元数据，当前没有嵌套元素访问器。
- 访问器类型不匹配会返回错误（例如对 `UTINYINT` 调用 `Int64`），不会静默转换。

### 2. database/sql 行式接口与参数绑定

- `gosql.OpenSQLDB(Config)` 通过 `sql.OpenDB` + 私有 Connector 创建 `*sql.DB`，不注册进程级 driver。
- SELECT 参数支持三种互不混用的样式：
  - 位置参数 `?`；
  - 连续编号 `$1`、`$2`；
  - `sql.Named("amount", ...)` 对应 `$amount`。
- 参数作为 AST 值绑定，不会拼接进 SQL；表名、列名和 SQL 片段不能参数化。
- `PrepareContext` 缓存解析语法，每次执行重新绑定参数并重新规划；示例用同一预编译语句执行两次不同阈值。
- 行式结果类型映射：
  - 有符号整数 → `int64`；
  - UTINYINT/USMALLINT/UINTEGER → `int64`；
  - UBIGINT/HUGEINT/DECIMAL → 精确十进制文本；
  - FLOAT/DOUBLE → `float64`；
  - VARCHAR/BLOB → `string`/`[]byte`；
  - DATE/timestamps → `time.Time`（UTC）；
  - NULL → `nil`。
- `Rows.ColumnTypes()` 提供 `DatabaseTypeName` 与可空性；精度、scale 和 Go scan type 不由适配器补充。
- `Begin`/`BeginTx` 明确返回错误；当前不支持事务。
- `RowsAffected()` 返回 0，需要导出计数时使用 `DB.CopyTo`。
- 未配置的 scheme（示例中的 `s3://`）在绑定阶段显式失败，不会静默回退。

### 3. SQL 查询能力

示例用 `demo_rows()` 表函数展示：

- CTE、`GROUP BY`/`HAVING`、`ORDER BY`/`LIMIT`/`OFFSET`；
- `FULL JOIN ... USING (k)`：合并键可用无限定符的 `k`，同时保留两侧限定列；
- `COUNT/SUM/AVG/MIN/MAX` 与 `COUNT/SUM/AVG(DISTINCT expr)`；
- `ROW_NUMBER() OVER (PARTITION BY ... ORDER BY ...)`；
- `COUNT(*) OVER ()` 与显式 `ROWS BETWEEN UNBOUNDED PRECEDING AND 1 PRECEDING`；
- `USING SAMPLE reservoir(3 ROWS) REPEATABLE(42)`、`bernoulli(100 PERCENT)`、`TABLESAMPLE`；
- 非相关标量子查询（零行返回 NULL，多行报错）；
- `IN (SELECT ...)`、相关 `EXISTS`；
- `LEFT JOIN LATERAL (...)` 相关派生表；
- `UNION ALL`（保留重复行）；
- `BETWEEN`、Unicode `LIKE`/`ESCAPE`、`IS [NOT] DISTINCT FROM`；
- `HUGEINT`/`UBIGINT`/`DECIMAL` 精确数值与 `%` 取模。

未实现的普通 `UNION` 去重、`INTERSECT`/`EXCEPT`、量化 `ANY`/`ALL`、递归 CTE、`UPDATE`/`DELETE` 等在第 8 节展示显式失败。

### 4. Go 表函数、标量 UDF、HMAC 与物化快照

- `Config.TableFunctions` 注册固定 schema、零参数的表函数：
  - `Open` 惰性调用，同一查询最多一次；
  - `BatchReader.Next` 每次最多填充 `BatchWriter.Capacity()` 行；
  - `(0, io.EOF)` 结束，`(n>0, io.EOF)` 发布最后一批；
  - `BatchWriter` 只在 `Next` 期间有效，setter 复制值；
  - 查询结束、失败或提前关闭时调用 `Close`。示例打印 `Open`/`Close` 次数验证生命周期。
- `BatchWriter` 的全部 setter 都有示例：`SetBool`、`SetInt64`、`SetUint64`、`SetFloat64`、`SetDecimal`、`SetString`、`SetBytes`、`SetDate`、`SetTimestamp`、`SetTimestampNS`、`SetNull`。
- `Config.ScalarFunctions` 注册标量 UDF：
  - `ScalarNullPropagate`（默认）在任一输入为 NULL 时不调用 `Eval`，直接返回 NULL；
  - `ScalarNullCall` 让 `Eval` 收到 `Valid=false` 的参数并自行决定结果。
- `gosql.NewHMACSHA256ScalarFunction(key)` 生成确定性的 `data_plane_hmac_sha256` VARCHAR 函数，输出小写十六进制；空 key 会被拒绝，key 被复制进 DB 作用域定义。
- `DB.Materialize(ctx, sql)` 执行一次查询并发布有界快照：
  - 返回的 `*Materialized` 可通过 `SQL()` 在后续查询中重复扫描；
  - 内存预算内驻留，超出后 spill；
  - `Close` 后拒绝新的绑定，已绑定查询保留 reader lease 直到自身关闭；
  - 所有对象关闭后 `DB.MemoryUsed()` 回到 0。

### 5. 本地文件、读取函数与结构化导出

- `Config.LocalFiles.RootDir` 必须显式配置为绝对目录；只允许访问该根目录以下路径。`TempDir`/`ManifestDir` 不会启用本地数据读取。
- `gosql.FileURI(path)` 生成规范 `file://` URI，避免在 Windows 上手工拼接。
- 读取函数：
  - `read_csv`：支持 `header`、`auto_detect`、`allow_quoted_nulls`、`columns`（对象或字符串形式）；`read_csv_auto` 是别名；
  - `read_json`、`read_ndjson`、`read_json_auto(format='newline_delimited')`；
  - `read_parquet`：示例用 `WHERE` 展示投影/谓词下推路径；
  - `glob`：返回冻结文件集合的单列 `file`。
- `DB.CopyTo(ctx, query, destination, CopyOptions)`：
  - `CopyFormatCSV`/`CopyFormatJSON`/`CopyFormatNDJSON`/`CopyFormatParquet`；
  - 文本格式整文件压缩支持 `identity`/`gzip`/`zstd`；Parquet 页压缩支持 `identity`/`snappy`；
  - CSV 支持 `Header`（默认 true）、`Delimiter`、`Quote`、`ForceQuote`、`DuckDBCompatible`；
  - 返回 `ExportResult{Rows, Bytes, Columns}`，只在最终对象提交后返回；
  - 目标已存在时不会覆盖。
- SQL `COPY (...) TO '...' (FORMAT ..., COMPRESSION ...)` 与 `CopyTo` 共用 Export/VFS 原子提交路径。
- `CREATE EXTERNAL TABLE name USING read_parquet('...')` 通过真实表函数 Bind 获取 schema，不保存 SQL 文本。
- 加密导出：`CopyOptions.Encryption` + `Config.ParquetKeys`。resolver 按 `(keyID, algorithm)` 返回 16 字节（AES-128-GCM）或 32 字节（AES-256-GCM）；SQL 和清单只保存 key identity。公共包不提供通用加密文件读取密钥注入入口。

### 6. Catalog DDL、DESCRIBE 与元数据

- `Open` 构造的 `MemoryCatalog` 只在该 DB 实例内共享，不跨实例持久化。
- `CREATE TEMP TABLE ... AS SELECT` 创建 DB 生命周期的类型化快照。
- `CREATE [OR REPLACE] TEMP VIEW` 保存查询定义，查询时重新绑定。
- `DESCRIBE <relation>` 只绑定 schema；relation 形式保留真实可空性，query 形式按 DuckDB 行为统一显示 `YES`。
- `information_schema.columns` 是 Planner 虚拟关系，不进入函数目录；示例列出当前实例的表、视图和列。
- `CREATE SECRET name (TYPE s3, key_id ..., SUBJECT ..., SCHEME ..., HOST ...)` 只写入内存 `SecretManager`；示例使用无效主机名，不发起网络请求。
- 未支持的 `ATTACH` 协议显式失败。

### 7. 资源配置、受管内存与 S3

- `Config` 零值选择默认值；示例显式写出内存、临时存储、spill、worker、队列和 Catalog 缓存配置。
- `MemoryUsed()` 只统计引擎受管内存，不是进程 RSS 或 Go heap；所有查询/快照关闭后应回到 0。
- `S3Config` 展示 Endpoint、Region、ForcePathStyle、AllowInsecureHTTP、凭证字段、重试和超时。只有 `Config.S3 != nil` 时才注册 S3 driver；示例会 Open 一次验证配置注册，但不会发起网络请求，也不回显密钥。使用 `http` 端点时必须显式设置 `AllowInsecureHTTP`。
- `SampleCompatibility` 支持空值、`duckdb`、`duckdb-msvc`；非法值在 `Open` 阶段被拒绝。该选项只影响采样流的参考引擎兼容性，要求输入顺序和 chunk 边界与参考引擎一致。

### 8. 错误边界与上下文取消

示例断言以下调用必须失败：

- 普通 `UNION`（去重）、`INTERSECT`、`EXCEPT` 未实现；
- `BIGINT % 0` 返回错误；
- 对无符号列调用 `Int64` 返回类型错误；
- 已取消的上下文返回 `context.Canceled`；
- `Close` 后的 DB 拒绝新的查询。

事务不支持与未配置 S3 的失败行为分别在第 2 节展示。

## 预期输出

输出以 `=== n/8 标题 ===` 分节，最后打印 `全部示例执行完成。`。示例值固定，便于对照：

- 原生类型示例返回 18 列 1 行；
- `demo_rows()` 共 5 行，`amount` 合计 110；
- `USING SAMPLE reservoir(3 ROWS)` 计数为 3；
- HMAC 输出为固定十六进制字符串；
- 导出后 `read_csv`/`read_json`/`read_ndjson`/`read_parquet` 的 `sum(amount)` 均为 110（过滤查询按条件变化）。

## 边界与未覆盖项

- S3/远程协议需要真实端点与凭据，quickstart 只展示配置结构。
- 加密 Parquet 只演示导出；公共包没有通用加密读取密钥注入接口。
- LIST/STRUCT 只公开类型元数据，`Chunk` 没有嵌套元素值访问器，行式适配器会拒绝非 NULL 嵌套值。
- 能力矩阵中列出的 `parquet_schema`/`parquet_file_metadata` 在当前分支的公共函数目录中未注册；需要列定义时使用 `DESCRIBE SELECT * FROM read_parquet(...)`。
- 事务、原生持久化表、`UPDATE`/`DELETE`/`MERGE`、递归 CTE、一般窗口 frame 等仍不在当前公共能力范围内。

更完整的 API 与 SQL 边界见：

- [公共 API](../../docs/public-api.md)
- [SQL 能力矩阵](../../docs/architecture/feature-contract.md)
- [相关子查询与 LATERAL](../../docs/architecture/subquery-lowering.md)
- [兼容性差异](../../compatibility/deviations.yaml)
