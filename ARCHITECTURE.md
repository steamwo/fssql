# Architecture

fssql 是 Go 实现的单节点、可嵌入 OLAP 查询引擎。本页记录贡献者必须保持的架构约束；0.1 封板后不再维护阶段流程文档。

## 执行模型

- 内部执行格式是自研 Vector/DataChunk ABI；Arrow 只用于外部交换，不作为内部执行格式。
- 查询从第一天使用 Push Pipeline。
- CPU Task Scheduler 与 Async I/O Scheduler 分离。
- Join、Aggregate、Sort 使用各自的外部算法，但共享 spill storage primitives。
- 核心 Hash Join / Group By 不使用 Go map。

## 内存

- Vector payload、StringHeap、Hash/Aggregate/Sort、Parquet decode、ReadAhead、Network buffers、Result encoding 等大块内存由 MemoryManager / BufferManager 管理。
- 任何可能无界增长的数据结构在增长前必须 Reservation。
- Go heap 只用于 AST、plan、metadata 和小型控制对象。

## 存储

- VFS 使用 capability model；禁止 silent whole-file Range fallback。
- Parquet 走 Controlled Parquet Engine，并直接解码到自研 Vector。
- FileSetPlanner + immutable ScanSet + SourceSnapshot 定义扫描输入。
- COPY 使用 Manifest + file-level resume + dataset commit。
- Catalog v1 为 MemoryCatalog + SQLiteCatalog。

## 语义与格式

- Stable SQL 语义默认对标 DuckDB；允许的差异记录在 `compatibility/deviations.yaml`。
- Protocol × Compression × Format 三者正交。
- Source Introspection 复用真正 Query Bind 的 Schema Resolution。

## 目录

```text
pkg/gosql            公共 Go API
internal/            引擎实现
cmd/                 命令行工具
test/                跨模块与互操作测试
examples/quickstart  最小可运行示例
docs/public-api.md   公共 API 与资源边界
```
