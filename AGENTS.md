# AGENTS.md — AI Coding Agent Primary Instructions

你正在开发一个 Go 实现的单节点云原生 OLAP 查询引擎。

## 1. 你的工作模式

每次启动任务时：

1. 读取 `machine/project-state.yaml`。
2. 确认 `current_phase`。
3. 读取该阶段文档。
4. 读取阶段文档引用的架构/契约。
5. 输出内部实现计划后再修改代码。
6. 每个任务必须附带测试。
7. 阶段内所有 Deliverable 完成后执行 Gate。
8. 只有 Gate 全通过，才能把阶段状态改为 `ACCEPTED`。

不要跨阶段大规模实现未来能力。

## 2. 最高优先级规则

优先级从高到低：

1. `architecture/decisions.md`
2. `architecture/invariants.md`
3. `contracts/*.md`
4. 当前 Phase 文档
5. 其他说明文档
6. 实现便利性

若发生冲突，以上位文档为准。

## 3. 不得自行修改的冻结架构

未经 ADR 不得改变：

- 自研 Vector/DataChunk 内部 ABI。
- Arrow 仅用于外部交换，不作为内部执行格式。
- v1 从第一天使用 Push Pipeline。
- 核心大块内存必须由 MemoryManager/BufferManager 管理。
- VFS 使用 capability model。
- 禁止 silent whole-file Range fallback。
- CPU Task Scheduler 与 Async I/O Scheduler 分离。
- Parquet 走 Controlled Parquet Engine，并直接解码到自研 Vector。
- Join/Aggregate/Sort 使用各自 external algorithm，但共享 spill storage primitives。
- FileSetPlanner + immutable ScanSet + SourceSnapshot。
- Catalog v1 为 MemoryCatalog + SQLiteCatalog。
- COPY 使用 Manifest + file-level resume + dataset commit。
- Stable/Experimental/Deferred Feature Contract。
- Source Introspection 必须复用真正 Query Bind 的 Schema Resolution。
- Protocol × Compression × Format 三者正交。

## 4. 热路径约束

以下核心热路径避免：

- `reflect`
- `map[string]any`
- per-row interface dispatch
- per-row heap allocation
- 无界 goroutine
- 无界 channel
- 绕过 MemoryManager 的大块 `make([]byte, ...)`
- 核心 Hash Join/Group By 使用 Go map

## 5. Go 内存约束

Go Heap 主要用于：

- AST
- plan
- metadata
- small control objects

Managed Buffer 用于：

- Vector payload
- StringHeap
- Hash/Aggregate/Sort
- Parquet decode
- ReadAhead
- Network buffers
- Result encoding

任何可能无界增长的数据结构，在增长前必须先 Reservation。

## 6. 测试要求

实现功能时至少考虑：

- Unit test
- Property test（适用时）
- SQLLogic / behavior test（SQL 行为）
- Fault-injection test（涉及 I/O / lifecycle 时）
- Race test（涉及并发时）
- Benchmark（涉及热路径或性能关键路径时）

每个已修复 bug 必须留下 regression test。

## 7. DuckDB 对标原则

Stable SQL 语义默认对标 DuckDB。

允许差异，但必须进入：

```text
compatibility/deviations.yaml
```

每个差异必须明确属于：

- bug
- intentional deviation
- unsupported feature

不得存在长期 unknown mismatch。

## 8. 阶段结束前必须执行的检查

至少：

```bash
go test ./...
go test -race ./...
```

若仓库已有 scripts，优先执行：

```bash
./scripts/test.sh
./scripts/race.sh
./scripts/acceptance.sh <phase>
```

并按当前 Phase 运行对应 benchmark/fault tests。

## 9. Agent 停止条件

遇到以下情况必须停止继续扩展实现：

- 需要改变 FROZEN architecture decision。
- 需要增加当前 Phase 未授权的新 Stable Feature。
- 实现必须绕过 MemoryManager / VFS / Query Lifecycle 才能工作。
- 测试无法证明 correctness，只能“看起来能跑”。
- 发现现有设计内部矛盾。

此时创建 ADR 草案或 blocker 说明，不得静默变更架构。

## 10. Definition of Done

单个任务 Done：

```text
implementation
+ tests
+ no architecture violation
+ no new leak/race
+ documentation for public behavior if changed
```

阶段 Done：

```text
all phase deliverables
+ Correctness Gate
+ Performance Gate
+ Memory Gate
+ Failure Gate
+ acceptance evidence
```
