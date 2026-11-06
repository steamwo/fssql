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

运行 `go run .`，输出 `42`。在本仓库根目录运行完整示例，无需外部数据源或凭据；示例会在临时目录生成并清理 CSV/JSON/NDJSON/Parquet 文件：

```sh
go run ./examples/quickstart
```

示例按 8 个分节覆盖原生 Chunk、database/sql、SQL 查询能力、Go 表函数/UDF/HMAC/物化快照、本地文件与导出、Catalog 元数据、资源配置与错误边界。逐节说明与预期输出见 [quickstart README](examples/quickstart/README.md)，代码入口见 [main.go](examples/quickstart/main.go)。

## 当前公共 API 能力

| 能力 | 当前入口与边界 |
| --- | --- |
| 原生查询 | `Open` → `QueryChunks`；Chunk 仅在回调期间有效，结果列信息通过 `[]Column` 返回。 |
| 行式查询 | `OpenSQLDB` 返回 `*sql.DB`；支持 SELECT 参数、`PrepareContext` 及上下文取消。 |
| 控制语句与导出 | `DB.Exec` 执行 DDL、控制语句和 COPY；`DB.CopyTo` 提供结构化选项及导出统计。目标由已配置的 VFS driver 支持。 |
| 数据源 | `Config.LocalFiles` 开启受根目录限制的本地文件访问，`Config.S3` 开启 S3；内建读取函数支持 Parquet、CSV、JSON、NDJSON。 |
| Go 扩展 | `Config.TableFunctions` 注册固定 schema 的零参数批量表函数；`Config.ScalarFunctions` 注册标量函数，另有 HMAC-SHA256 辅助构造器。 |
| Catalog | `Open` 当前使用 MemoryCatalog；其元数据保存在当前 DB 实例内。 |
| 资源控制 | `Config` 配置内存、临时存储、spill、worker 和队列限制；`MemoryUsed` 统计引擎受管内存。 |

当前 `Config` 没有 SQLiteCatalog、自定义 VFS、SFTP、FTP 或 WebDAV 的注入入口，也没有公共 HTTP 服务构造器。内部模块的实现范围与外部项目可直接使用的范围，请以[公共 API 文档](docs/public-api.md)为准。

SELECT 参数支持 `?`、`$1` 或 `$name`，预编译语句缓存解析结果；每次执行重新绑定参数并取得语句快照。事务暂不支持。DECIMAL/HUGEINT 和 UBIGINT 的行式结果以精确十进制文本返回。LIST/STRUCT 公开类型元数据，但当前 `Chunk` 没有读取嵌套元素值的方法。

SQL 已实现非递归 CTE、集合谓词、嵌套标量子查询、参数域去相关，以及显式/隐式 LATERAL 派生表关联。具体支持范围、拒绝边界和验证入口见[能力文档](docs/architecture/feature-contract.md)，不代表完整 DuckDB SQL 兼容或全部性能验收完成。

## 文档与开发

- [公共 API、资源默认值与使用边界](docs/public-api.md)
- [SQL、数据源和执行能力矩阵](docs/architecture/feature-contract.md)
- [相关子查询与 LATERAL 的实现和支持范围](docs/architecture/subquery-lowering.md)
- [架构约束](ARCHITECTURE.md)
- [项目测试规范与 DuckDB 对标方案](docs/testing-standard.md)
- [引擎优化与改进计划及诊断证据](docs/optimization-review-2026-11-06.md)
- [SQL 兼容性差异](compatibility/deviations.yaml)

仓库按 `pkg/gosql`（公共入口）、`internal`（引擎实现）、`cmd`（命令行工具）、`test`（跨模块测试）和 `examples`（示例）组织。外部项目应通过公共包访问引擎。

验证快速开始示例与公共包：

```sh
go test ./examples/quickstart ./pkg/gosql
```
