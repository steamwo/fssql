# fssql

[English](README_EN.md) | **简体中文**

fssql 是用 Go 实现的**嵌入式只读 SQL 查询与数据转换/导出引擎**。它面向本地文件、对象存储和应用内数据的分析型工作负载：读取数据，用 SQL 查询和转换，再导出为常见数据格式。

内部使用自研 Vector/DataChunk 和 Push Pipeline，通过受控内存、临时存储与 spill 执行查询。对外 Go 包为 `github.com/steamwo/fssql/pkg/gosql`，提供原生 Chunk 回调和 `database/sql` 两种查询方式。

## 项目定位

fssql 当前专注于一条明确的数据处理路径：

**读取数据 → SQL 查询/转换 → 导出结果**

- **数据读取**：CSV、JSON、NDJSON、Parquet，来源包括本地文件、S3 / MinIO、FTP/FTPS、SFTP、WebDAV 和 HTTP/HTTPS。
- **SQL 查询**：过滤、投影、聚合、JOIN、窗口函数、子查询、集合操作、CTE 等分析型 SQL。
- **数据导出**：CSV、JSON、NDJSON、Parquet，支持本地文件及已配置的 S3、FTP/FTPS、SFTP、WebDAV 目标和提交恢复。
- **Go 嵌入**：既可使用原生 Chunk API，也可直接通过 `database/sql` 查询。
- **资源控制**：提供内存、临时存储、spill、worker 和队列限制，适合在 Go 服务内嵌运行。
- **可扩展数据源与函数**：支持 Go Table Function、Scalar UDF 和物化快照。

fssql **不以替代 DuckDB 或成为通用数据库为目标**。DuckDB 在项目中主要作为 SQL 语义、格式互操作和性能测试的参考基线。

当前重点是只读查询、数据转换和可靠导出；事务、可变持久化表以及 `UPDATE` / `DELETE` / `MERGE` 不是当前阶段的主要目标，未来可根据实际需求再评估。


## 快速开始

需要 Go 1.22 或更高版本。在已有 Go module 中添加依赖：

```sh
go get github.com/steamwo/fssql/pkg/gosql
```

私有仓库也可以作为依赖；拉取环境需要 Git 访问凭据，并配置 `GOPRIVATE=github.com/steamwo/fssql`。

下面是可保存为 `main.go` 的完整示例：

```go
package main

import (
    "context"
    "fmt"
    "log"

    "github.com/steamwo/fssql/pkg/gosql"
)

// main 通过 database/sql 查询一个常量结果。
func main() {
    db, err := gosql.OpenSQLDB(gosql.Config{})
    if err != nil {
        log.Fatal(err)
    }
    defer db.Close()

    var answer int64
    if err := db.QueryRowContext(context.Background(), "SELECT 42 AS answer").Scan(&answer); err != nil {
        log.Fatal(err)
    }
    fmt.Println(answer)
}
```

运行 `go run .`，输出 `42`。在本仓库根目录运行完整示例，无需外部数据源或凭据；示例会在临时目录生成并清理 CSV/JSON/NDJSON/Parquet 文件：

```sh
go run ./examples/quickstart
```

示例按 8 个分节覆盖原生 Chunk、database/sql、SQL 查询能力、Go 表函数/UDF/HMAC/物化快照、本地文件与导出、Catalog 元数据、资源配置与错误边界。逐节说明与预期输出见 [quickstart README](examples/quickstart/README.md)，代码入口见 [main.go](examples/quickstart/main.go)。

## 常用使用例子

以下片段放在返回 `error` 的函数中使用；`ctx` 是调用方传入的 `context.Context`。`sqlDB` 表示 `OpenSQLDB` 返回的 `*sql.DB`，`db` 表示 `Open` 返回的 `*gosql.DB`，两者分别创建独立的引擎实例。按片段需要导入 Go 标准库及 `gosql`。

### 参数查询、预编译与 NULL

日常行式查询可以直接使用 `database/sql`，无需注册 driver 名称：

```go
sqlDB, err := gosql.OpenSQLDB(gosql.Config{})
if err != nil {
    return err
}
defer sqlDB.Close()

stmt, err := sqlDB.PrepareContext(ctx,
    "SELECT id FROM range(5) AS t(id) WHERE id >= ? ORDER BY id")
if err != nil {
    return err
}
defer stmt.Close()

rows, err := stmt.QueryContext(ctx, int64(3))
if err != nil {
    return err
}
defer rows.Close()
for rows.Next() {
    var id int64
    if err := rows.Scan(&id); err != nil {
        return err
    }
    fmt.Println(id) // 依次输出 3、4
}
if err := rows.Err(); err != nil {
    return err
}
if err := rows.Close(); err != nil {
    return err
}

var amount string
err = sqlDB.QueryRowContext(ctx,
    "SELECT CAST($amount AS DECIMAL(38,2))",
    sql.Named("amount", "12345678901234567890.12"),
).Scan(&amount)
if err != nil {
    return err
}
fmt.Println(amount) // 12345678901234567890.12

var optional sql.NullString
if err := sqlDB.QueryRowContext(ctx,
    "SELECT CAST(? AS VARCHAR)", nil).Scan(&optional); err != nil {
    return err
}
fmt.Println(optional.Valid) // false
```

参数支持 `?`、连续编号 `$1`/`$2` 或 `$name`，同一语句不能混用。参数绑定值，不能替代表名、列名或 SQL 片段。`PrepareContext` 缓存解析语法，每次执行仍会绑定参数并重新规划。多行结果应关闭 `Rows` 并检查 `Rows.Err()`；单行查询使用 `QueryRowContext(...).Scan(...)`。

### 查询本地 CSV、JSON 和 Parquet

下面的片段会创建一个临时 CSV，按地区汇总，并在结束后清理。额外导入 `os`、`path/filepath`、`fmt`：

```go
root, err := os.MkdirTemp("", "fssql-example-*")
if err != nil {
    return err
}
defer os.RemoveAll(root)

path := filepath.Join(root, "orders.csv")
if err := os.WriteFile(path, []byte(
    "id,region,amount\n1,east,10\n2,east,20\n3,west,30\n",
), 0o600); err != nil {
    return err
}
uri, err := gosql.FileURI(path)
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

rows, err := sqlDB.QueryContext(ctx, `
    SELECT region, sum(amount) AS total
    FROM read_csv(?, header=true,
        columns={'id':'BIGINT','region':'VARCHAR','amount':'BIGINT'})
    GROUP BY region ORDER BY region`, uri)
if err != nil {
    return err
}
defer rows.Close()
for rows.Next() {
    var region string
    var total int64
    if err := rows.Scan(&region, &total); err != nil {
        return err
    }
    fmt.Println(region, total) // east 30，然后 west 30
}
if err := rows.Err(); err != nil {
    return err
}
```

`LocalFiles.RootDir` 必须是绝对目录，文件访问限制在该根目录内。已有数据目录可先用 `filepath.Abs("data")` 取得绝对路径。本地 URI 使用 `gosql.FileURI` 生成，尤其不要手工拼接 Windows 路径。`TempDir`/`ManifestDir` 只用于临时存储，不会启用本地数据文件访问。

其他读取方式如下；`?` 绑定对应文件的 URI，文件需要事先存在：

| 数据格式 / 场景 | 查询语句 |
| --- | --- |
| CSV 自动推断 | `SELECT * FROM read_csv(?, header=true)` |
| JSON 数组 | `SELECT * FROM read_json(?)` |
| 逐行 JSON | `SELECT * FROM read_ndjson(?)` |
| Parquet 过滤 | `SELECT id, amount FROM read_parquet(?) WHERE amount >= 20` |
| 文件集合 | `SELECT * FROM read_parquet(?)`，参数可用根目录内 `*.parquet` 的 URI |
| 查看列定义 | `DESCRIBE SELECT * FROM read_parquet(?)` |

### 聚合、JOIN、窗口与集合查询

以下 SQL 可分别传给 `QueryContext` 执行，不需要外部数据。`range(5)` 生成 0 到 4，`range(1, 6)` 生成 1 到 5，结束值不包含在结果中。

```sql
-- CTE、分组聚合与 HAVING：返回 bucket=0,total=6 和 bucket=1,total=9。
WITH orders AS (
    SELECT id, id % 2 AS bucket FROM range(1, 6) AS t(id)
)
SELECT bucket, sum(id) AS total
FROM orders GROUP BY bucket HAVING sum(id) >= 6 ORDER BY bucket;

-- JOIN：返回 id=1 和 id=2。
SELECT a.id, b.id AS matched
FROM range(3) AS a(id)
JOIN range(1, 4) AS b(id) ON a.id = b.id
ORDER BY a.id;

-- 窗口函数：按 id 倒序编号，返回 (2,1)、(1,2)、(0,3)。
SELECT id, row_number() OVER (ORDER BY id DESC) AS rn
FROM range(3) AS t(id) ORDER BY rn;

-- EXISTS 子查询：返回 id=1 和 id=2。
SELECT a.id FROM range(4) AS a(id)
WHERE EXISTS (SELECT 1 FROM range(1, 3) AS b(id) WHERE a.id = b.id)
ORDER BY a.id;

-- UNION 去重，UNION ALL 保留重复行。
SELECT 1 AS id UNION SELECT 1 UNION SELECT 2 ORDER BY id;
```

还支持 `INTERSECT`/`EXCEPT`（含 `ALL`）、`UNION BY NAME`、LATERAL、采样、标量子查询和常用字符串/日期/正则函数。完整范围见[能力矩阵](docs/architecture/feature-contract.md)与[公共 API 文档](docs/public-api.md)。

### 临时表、视图与会话

需要跨多条语句保留 TEMP 表或会话设置时，使用 `sql.Conn` 固定连接。额外导入 `fmt`：

```go
conn, err := sqlDB.Conn(ctx)
if err != nil {
    return err
}
defer conn.Close()
if _, err := conn.ExecContext(ctx,
    "CREATE TEMP TABLE orders AS SELECT id FROM range(3) AS t(id)"); err != nil {
    return err
}
if _, err := conn.ExecContext(ctx,
    "CREATE TEMP VIEW report AS SELECT count(*) AS n FROM orders"); err != nil {
    return err
}
var count int64
if err := conn.QueryRowContext(ctx, "SELECT n FROM report").Scan(&count); err != nil {
    return err
}
fmt.Println(count) // 3
```

TEMP 对象和 `TimeZone`/`threads` 等设置属于物理连接；`sql.Conn.Close()` 将连接归还池中，物理连接关闭时释放其 TEMP 对象。`Open` 使用内存 Catalog，重新打开引擎不会恢复表和视图；CTAS 是查询快照，视图保存查询定义。可以查询 `information_schema.columns` 获取元数据。

### 原生 Chunk 与物化快照

需要批量读取结果时，使用 `Open` 和 `QueryChunks`。额外导入 `fmt`：

```go
db, err := gosql.Open(gosql.Config{})
if err != nil {
    return err
}
defer db.Close()
columns, err := db.QueryChunks(ctx,
    "SELECT id FROM range(3) AS t(id) ORDER BY id",
    func(chunk *gosql.Chunk) error {
        for row := 0; row < chunk.RowCount(); row++ {
            id, valid, err := chunk.Int64(row, 0)
            if err != nil {
                return err
            }
            if valid {
                fmt.Println(id) // 依次输出 0、1、2
            }
        }
        return nil
    })
if err != nil {
    return err
}
fmt.Println(columns[0].Name) // id

snapshot, err := db.Materialize(ctx, "SELECT id FROM range(3) AS t(id)")
if err != nil {
    return err
}
defer snapshot.Close()
// SQL() 返回查询该快照的 SELECT，可作为派生表用于后续查询。
_, err = db.QueryChunks(ctx, "SELECT count(*) FROM ("+snapshot.SQL()+") AS s",
    func(chunk *gosql.Chunk) error {
        n, _, err := chunk.Int64(0, 0)
        if err != nil {
            return err
        }
        fmt.Println(n) // 3
        return nil
    })
if err != nil {
    return err
}
```

`Chunk` 仅在回调期间有效，不要保留它。访问器按 `(row, column)` 从 0 索引，`valid=false` 表示 NULL；访问器需要匹配列类型。常用方法包括 `Int64`、`Uint64`、`Float64`、`Bool`、`String`、`Bytes`、`Int128`。`Value`、`List`、`Struct` 返回拥有独立数据的值，可在回调结束后保留；嵌套结果建议使用原生 API。

`Materialize` 执行一次并创建有界快照，超出内存预算后可 spill；使用完毕必须关闭快照。更多类型访问与生命周期示例见 [native.go](examples/quickstart/native.go) 和 [functions.go](examples/quickstart/functions.go)。

### 导出 CSV、JSON、NDJSON 和 Parquet

下面将查询结果导出为 Parquet。`root` 为已存在的绝对数据目录，额外导入 `path/filepath`、`fmt`：

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
result, err := db.CopyTo(ctx,
    "SELECT id FROM range(3) AS t(id)", destination,
    gosql.CopyOptions{
        Format:      gosql.CopyFormatParquet,
        Compression: "snappy",
    })
if err != nil {
    return err
}
fmt.Printf("rows=%d bytes=%d\n", result.Rows, result.Bytes) // rows=3
```

将 `Format` 换成 `CopyFormatCSV`、`CopyFormatJSON` 或 `CopyFormatNDJSON`，并修改目标扩展名，即可导出其他格式。文本格式压缩支持 `identity`/`gzip`/`zstd`，Parquet 页压缩支持 `snappy`/`identity`。CSV 默认输出表头，分隔符等通过 `CopyOptions` 配置。目标已存在时不会覆盖；成功返回表示最终文件已提交。

Local/S3 及远程文件协议的单文件导出使用 v2 清单和随机 JobID，先完成 Seal，再持久化 PREPARED/COMMITTING 后发布。`CopyOptions.PublishMode` 默认 strict；Local/S3 的 compatible 也保持不覆盖保证。提交应答丢失等情况返回携带 JobID 的 `CommitUncertainError`，可通过 `db.RecoverExport(ctx, jobID)` 核对或恢复，恢复不会重新执行查询。S3 Seal 不 Complete，零字节输出不提前 PUT；恢复先核验 final，再按需要核对分片。核验会完整读取物理输出，远程协议因此有额外下载成本。已提交目标被删除时不会重建；清理失败或未知 ownership 保留 `cleanup_pending`。请保留相同 `ManifestDir`，详见 [导出恢复 API](docs/public-api.md#export-recovery)。

也可通过 `ExecContext` 执行 SQL COPY，目标 URI 作为参数传入：

```go
_, err := sqlDB.ExecContext(ctx,
    "COPY (SELECT id FROM range(3) AS t(id)) TO ? (FORMAT PARQUET)",
    destination)
if err != nil {
    return err
}
```

SQL COPY 使用配置了本地或 S3 driver 的 `sqlDB`，目标同样必须尚不存在。需要导出行数、字节数时使用 `CopyTo`；`database/sql` 的 `RowsAffected()` 当前返回 0。加密 Parquet 导出和压缩往返示例见 [files.go](examples/quickstart/files.go)。

### 注册 Go 标量函数与表函数

标量 UDF 可将 Go 逻辑用于 SQL 表达式。下面注册大写转换函数，额外导入 `strings`、`fmt`：

```go
upper := gosql.ScalarFunction{
    Name:          "app_upper",
    Arguments:     []gosql.Type{{ID: "VARCHAR"}},
    Result:        gosql.Type{ID: "VARCHAR"},
    Deterministic: true,
    NullHandling:  gosql.ScalarNullPropagate,
    Eval: func(args []gosql.ScalarValue) (gosql.ScalarValue, error) {
        return gosql.ScalarValue{
            Type: gosql.Type{ID: "VARCHAR"}, Valid: true,
            String: strings.ToUpper(args[0].String),
        }, nil
    },
}
sqlDB, err := gosql.OpenSQLDB(gosql.Config{
    ScalarFunctions: []gosql.ScalarFunction{upper},
})
if err != nil {
    return err
}
defer sqlDB.Close()
var text string
if err := sqlDB.QueryRowContext(ctx,
    "SELECT app_upper(?)", "hello").Scan(&text); err != nil {
    return err
}
fmt.Println(text) // HELLO
```

默认 NULL 传播策略不会为 NULL 输入调用 `Eval`。需要自行处理 NULL 时使用 `ScalarNullCall` 并检查参数的 `Valid`。`gosql.NewHMACSHA256ScalarFunction(key)` 可创建 `data_plane_hmac_sha256` 函数，按相同方式注册。

应用内数据可以通过 `Config.TableFunctions` 暴露为 SQL 数据源：声明 `TableFunction{Name, Columns, Open}`，由 `Open` 返回实现 `Next(ctx, BatchWriter)` 和 `Close()` 的 `BatchReader`。每批最多填充 `Capacity()` 行，`io.EOF` 表示结束；当前自定义表函数使用固定 schema、零 SQL 参数。最小 ID 数据源示例见[公共 API](docs/public-api.md#go-table-and-scalar-functions)，完整批量读取实现见 [functions.go](examples/quickstart/functions.go)。

### S3 / MinIO 与资源限制

本地文件和 S3 都需要显式启用。下面是 MinIO 配置，额外导入 `os`、`time`：

```go
sqlDB, err := gosql.OpenSQLDB(gosql.Config{
    MemoryLimitBytes:       128 << 20, // 128 MiB 受管内存
    TempLimitBytes:         1 << 30,   // 1 GiB 临时存储
    SpillMemoryBudgetBytes: 8 << 20,
    Workers:               2,
    S3: &gosql.S3Config{
        Endpoint:          "http://127.0.0.1:9000",
        Region:            "us-east-1",
        ForcePathStyle:    true,
        AllowInsecureHTTP: true, // 此示例使用本地 HTTP 端点
        AccessKey:         os.Getenv("AWS_ACCESS_KEY_ID"),
        SecretKey:         os.Getenv("AWS_SECRET_ACCESS_KEY"),
        SessionToken:      os.Getenv("AWS_SESSION_TOKEN"),
        MaxRetries:        3,
        RequestTimeout:    10 * time.Second,
    },
})
if err != nil {
    return err
}
defer sqlDB.Close()
var count int64
if err := sqlDB.QueryRowContext(ctx,
    "SELECT count(*) FROM read_parquet(?)",
    "s3://my-bucket/orders.parquet").Scan(&count); err != nil {
    return err
}
```

运行前准备好端点、凭据和对象；使用 HTTPS 端点时无需 `AllowInsecureHTTP`。S3 URI 同样可用于 COPY/CopyTo 导出。以上代码从环境变量显式传入凭据，不会自动启用未配置的协议。

`Config{}` 默认使用 256 MiB 受管内存、1 GiB 临时存储和 1 个 worker；零值选择默认值。`MemoryUsed()` 只统计引擎受管内存，不代表进程 RSS 或 Go heap。超时可统一传入 `context.WithTimeout` 返回的上下文，并 `defer cancel()`；查询失败、取消和提前关闭都会进入资源清理路径。

## 当前公共 API 能力

| 能力 | 当前入口与边界 |
| --- | --- |
| 原生查询 | `Open` → `QueryChunks`；Chunk 仅在回调期间有效，结果列信息通过 `[]Column` 返回。 |
| 行式查询 | `OpenSQLDB` 返回 `*sql.DB`；支持值参数、`PrepareContext` 及上下文取消。 |
| 控制语句与导出 | `DB.Exec` 执行 DDL、控制语句和 COPY；`DB.CopyTo` 提供结构化选项及导出统计。目标由已配置的 VFS driver 支持。 |
| 数据源 | `Config.LocalFiles` 开启受根目录限制的本地文件访问，`Config.S3` 开启 S3；`Config.FTP`、`Config.SFTP`、`Config.WebDAV` 开启对应远程协议，`Config.HTTP` 授权多个 HTTP/HTTPS 端点。内建读取函数支持 Parquet、CSV、JSON、NDJSON。 |
| Go 扩展 | `Config.TableFunctions` 注册固定 schema 的零参数批量表函数；`Config.ScalarFunctions` 注册标量函数，另有 HMAC-SHA256 辅助构造器。 |
| Catalog | `Open` 当前使用 MemoryCatalog；其元数据保存在当前 DB 实例内。 |
| 资源控制 | `Config` 配置内存、临时存储、spill、worker 和队列限制；`MemoryUsed` 统计引擎受管内存。 |

`Config.FTP` 支持 FTP/隐式 TLS FTPS，`Config.SFTP` 使用系统 OpenSSH 并要求配置 `KnownHostsFile`，`Config.WebDAV` 支持 WebDAV/WebDAVS。各配置绑定一个端点，未配置的协议保持禁用。FTP 明文连接须显式设置 `AllowPlainFTP`；FTP/WebDAV 默认拒绝私网、回环和链路本地地址，可通过 `RemoteNetworkConfig` 按需开启。读取和单文件 COPY/CopyTo 均已接入。SFTP strict 要求握手声明 `hardlink@openssh.com=1`；WebDAV strict 要求部署方验证后设置 `AtomicMoveVerified: true`。FTP/FTPS 仅支持显式 compatible，其目标检查与 rename 之间存在 TOCTOU 窗口，可能覆盖竞争创建的目标；SFTP compatible 也不承诺严格不覆盖，WebDAV compatible 不承诺服务端的崩溃原子可见性。配置及服务端前提见[公共 API 文档](docs/public-api.md#remote-protocols)。

当前 `Config` 没有 SQLiteCatalog、自定义 VFS 的注入入口，也没有公共 HTTP 服务构造器。内部模块的实现范围与外部项目可直接使用的范围，请以[公共 API 文档](docs/public-api.md)为准。

`Config.HTTP` 支持匿名、Basic、Bearer 和固定 API Key 请求头，保留签名 URL 的原始路径和 query。只访问已配置的源站，重定向逐跳切换目标认证并禁止 HTTPS 降级；默认拒绝私网、回环及链路本地地址。有效强 ETag Range 视图按需读取，默认 `materialize` 策略在能力不足时将完整对象写入受 `TempLimitBytes` 管理的查询临时空间，也可配置 `require_range` 要求稳定物理 Range。支持 HTTP identity/gzip；文本读取函数及现有别名统一接受 `compression='auto'|'none'|'gzip'|'zstd'`，默认按内容魔数自动识别。HTTP 解码和文件解压串联后只写一个查询临时对象，解压后字节受 TempLimit 管理，解码工作区受 MemoryLimit 管理。详见 [HTTP/HTTPS 配置](docs/public-api.md#http-and-https-sources)与[压缩文本读取](docs/public-api.md#compressed-text-sources)。

值参数可用于 SELECT、已支持的 CREATE、COPY、DESCRIBE 和 SET 等语句。当前引擎面向只读查询与导出工作负载，事务暂不支持。DECIMAL/HUGEINT 和 UBIGINT 的行式结果以精确十进制文本返回；原生 DECIMAL 访问器返回未缩放整数，需结合 `Type.Scale` 解释。日期/时间的行式结果为 UTC `time.Time`。LIST/STRUCT 可通过原生 `Chunk.Value`/`List`/`Struct` 读取，`database/sql` 对非 NULL 嵌套值仍返回错误。

当前不提供原生可变持久化表，也不支持 `UPDATE`/`DELETE`/`MERGE` 和事务；这些能力不属于当前只读查询/导出方向的主要目标。递归 CTE 目前同样不支持。TEMP 命名空间与设置按物理连接隔离；依赖会话状态时应使用 `sql.Conn`。

SQL 已实现非递归 CTE、集合谓词、按位置的 UNION/INTERSECT/EXCEPT（含 ALL）、嵌套标量子查询、参数域去相关，以及显式/隐式 LATERAL 派生表关联。具体支持范围、拒绝边界和验证入口见[公共 API 文档](docs/public-api.md)，不代表完整 DuckDB SQL 兼容或全部性能验收完成。

## 文档与开发

- [公共 API、资源默认值与使用边界](docs/public-api.md)
- [SQL、数据源和执行能力矩阵](docs/architecture/feature-contract.md)
- [相关子查询与 LATERAL 的实现和支持范围](docs/architecture/subquery-lowering.md)
- [架构约束](ARCHITECTURE.md)
- [项目测试规范与 DuckDB 差分验证/性能基线方案](docs/testing-standard.md)
- [2026-11-07 与 DuckDB 2.0.0 的差分与性能基线报告](docs/validation/2026-11-07-full-duckdb-comparison/report.md)：262/262 差分用例一致；28 个计时场景（本地与 MinIO）共 812 条正式样本，总体 `time_ratio` 中位 3.86。该报告用于验证语义正确性、发现性能差距和建立优化基线，不代表项目以替代 DuckDB 为目标
- [引擎优化与改进计划及诊断证据](docs/optimization-review-2026-11-06.md)
- [SQL 兼容性差异](compatibility/deviations.yaml)
- [DuckDB 兼容性完善方案与实施进度](docs/duckdb-compatibility-roadmap.md)

仓库按 `pkg/gosql`（公共入口）、`internal`（引擎实现）、`cmd`（命令行工具）、`test`（跨模块测试）和 `examples`（示例）组织。外部项目应通过公共包访问引擎。

验证快速开始示例与公共包：

```sh
go test ./examples/quickstart ./pkg/gosql
```
