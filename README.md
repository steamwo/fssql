# fssql

fssql 是用 Go 实现的单节点 OLAP 查询引擎，可嵌入 Go 应用。内部使用自研 Vector/DataChunk 和 Push Pipeline，通过受控内存与临时存储执行查询。

对外 Go 包为 `github.com/steamwo/fssql/pkg/gosql`，提供原生 Chunk 回调和 `database/sql` 两种查询方式。

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

运行 `go run .`，输出 `42`。在本仓库根目录运行以下命令，可同时体验两种 API，无需外部数据源或凭据：

```sh
go run ./examples/quickstart
```

```text
native: 42
database/sql: 42
```

完整代码见 [quickstart](examples/quickstart/main.go)。

## 当前公共 API 能力

| 能力 | 当前入口与边界 |
| --- | --- |
| 原生查询 | `Open` → `QueryChunks`；Chunk 仅在回调期间有效，结果列信息通过 `[]Column` 返回。 |
| 行式查询 | `OpenSQLDB` 返回 `*sql.DB`；使用 `QueryContext`、`QueryRowContext`、`ExecContext`。 |
| 控制语句与导出 | `DB.Exec` 执行 DDL、控制语句和 COPY；COPY 目标必须由已配置的 VFS driver 支持。 |
| 数据源 | 配置 `Config.S3` 后注册 S3；可使用 `read_parquet`、`read_csv`、`read_json`、`read_ndjson` 等表函数。零配置示例使用不需要数据源的常量查询。 |
| Catalog | `Open` 当前使用 MemoryCatalog；其元数据保存在当前 DB 实例内。 |
| 资源控制 | `Config` 配置内存、临时存储、spill、worker 和队列限制；`MemoryUsed` 统计引擎受管内存。 |

当前 `Config` 没有 SQLiteCatalog、自定义 VFS、SFTP、FTP 或 WebDAV 的注入入口，也没有公共 HTTP 服务构造器。内部模块的实现范围与外部项目可直接使用的范围，请以[公共 API 文档](docs/public-api.md)为准。

`database/sql` 当前不支持查询参数、事务和预编译语句；DECIMAL/HUGEINT 以精确十进制文本返回。LIST/STRUCT 公开类型元数据，但当前 `Chunk` 没有读取嵌套元素值的方法。

## 文档与开发

- [公共 API、资源默认值与使用边界](docs/public-api.md)
- [架构约束](ARCHITECTURE.md)
- [项目测试规范与 DuckDB 对标方案](docs/testing-standard.md)
- [SQL 兼容性差异](compatibility/deviations.yaml)

仓库按 `pkg/gosql`（公共入口）、`internal`（引擎实现）、`cmd`（命令行工具）、`test`（跨模块测试）和 `examples`（示例）组织。外部项目应通过公共包访问引擎。

验证快速开始示例与公共包：

```sh
go test ./examples/quickstart ./pkg/gosql
```
