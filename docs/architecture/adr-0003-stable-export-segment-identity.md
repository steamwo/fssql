# ADR-0003 — Stable Export Segment Identity for File-Level Resume

Status: ACCEPTED

## Context

Phase 4 同时要求：

- `ExportCoordinator + Manifest + WriterPool + Committer`；
- file-level resume；
- incomplete file redo；
- `Kill / Restart / Resume`；
- no duplicate completed files；
- no missing files；
- SingleFile / MultiFile / Partitioned export；
- bounded writer count and managed memory。

当前通用执行接口只传递 `DataChunk`：

```text
Source.Next(ctx) -> DataChunk
Pipeline workers
-> Sink.Consume(ctx, DataChunk)
```

`DataChunk` 和 Scheduler `Task` 都没有可持久化的 output segment identity。并行执行下，chunk 到达顺序、worker ID、goroutine ID、运行时 chunk 边界都不是 durable property。

因此以下做法不能用于可恢复多文件导出：

```text
first chunk  -> part-000001
second chunk -> part-000002
...
```

重启后调度顺序可能变化；保留旧的 `COMPLETE part-000001` 并跳过新的同名文件，可能造成重复或缺失数据。

这是 AD-009 `file-level resume` 缺失的 durable file identity 定义，需要通过 ADR 补全。

## Decision

### 1. Stable Export SegmentPlan

所有声明 **resumable** 的 MultiFile / Partitioned export 必须绑定一个 export-specific `SegmentPlan`。

`SegmentPlan` 至少包含：

```text
Strategy
Version
PlannerFingerprint
```

并作为 Manifest job identity 的一部分持久化。

Resume MUST 验证重启后的 `QueryFingerprint`、`SourceSnapshot` 和 `SegmentPlan` 与原作业一致；任一不一致都必须拒绝恢复。

### 2. Segment identity 必须独立于运行时调度

稳定 segment identity 不得依赖：

- worker ID；
- goroutine ID；
- chunk 到达顺序；
- runtime chunk boundaries；
- retry attempt number。

具体如何产生稳定 segment ID 属于 **Export planner implementation**，不是通用 Pipeline/Scheduler ABI。

通用 `DataChunk`、Pipeline `Sink`、Scheduler `Task` 本 ADR 均不增加 export-specific 字段。

### 3. Phase 4 初始策略：Explicit Stable SegmentPlan

Phase 4 首个实现采用 `EXPLICIT_V1`：

- Export planner 向 DatasetWriter 提供稳定 `segmentID`；
- `segmentID` 必须在同一 QueryFingerprint + SourceSnapshot + PlannerFingerprint 下可确定重建；
- DatasetWriter 不制造 segment ID，只验证和消费它；
- MultiFile / Partitioned 文件路径由稳定 `segmentID` 与稳定 partition value 组合得到；
- Manifest 中已 `COMPLETE` 的同一稳定文件在 resume 时直接跳过；
- `PLANNED` / `WRITING` / `FAILED` 文件按整文件重做。

若某个 Export planner 无法证明重启后能产生相同稳定 segment identity，则该 planner **不得声明 resumable MultiFile / Partitioned**。

SingleFile 不需要 segment planner：未完成文件整文件重做即可满足 v1 file-level resume。

### 4. 具体 segmentation 算法不冻结

本 ADR **不规定**以下任一算法为冻结架构：

- canonical row hash；
- local spool；
- worker-per-file；
- ordered row ordinal；
- source-file/row-group segment；
- 其他未来 planner。

这些都可以作为 Export planner 的实现策略单独评估，只要满足 Stable Segment Identity contract。

因此，批准本 ADR 不等于批准 row-hash 或 local spool。

### 5. Resume semantics

恢复流程为：

```text
load Manifest
-> validate QueryFingerprint
-> validate SourceSnapshot
-> validate SegmentPlan
-> convert stale WRITING files to redoable incomplete state
-> rerun using the same Export SegmentPlan
-> preserve COMPLETE files
-> redo incomplete files
-> commit only after every planned file is COMPLETE
```

`COMPLETE` 文件不可回退或覆盖。

任何旧 attempt 遗留的 `WRITING` 文件都视为 incomplete，恢复时必须整文件重做。

### 6. Dataset commit 不变

本 ADR 不改变 AD-009 的提交语义：

```text
remote staging complete files
-> conditional final manifest / success marker
-> COMMITTED dataset
```

S3 仍不得伪造目录 rename。

## Alternatives

### A. Runtime chunk ordinal

Rejected。并行调度与 chunk 边界在 restart 后不稳定。

### B. Worker/task ID

Rejected。row-to-worker assignment 在 join/aggregate/work stealing/retry/worker-count 变化后不稳定，同时会把 export recovery 泄漏进通用 Scheduler ABI。

### C. 强制 row hash + local spool

Not selected as frozen architecture。

它可以成为未来具体 planner，但会引入额外 hash semantics、volatile expression、nested value hashing、local temp lifecycle 与性能成本。当前不需要把这些实现细节冻结为 AD-009 的唯一答案。

### D. Require ORDER BY + row ordinal

Rejected as general contract。会收窄 COPY 行为并引入额外 SQL/ordering 语义。

## Consequences

Positive:

- 补全 AD-009 file-level resume 所需的 durable identity。
- 不修改 Own Vector / DataChunk ABI。
- 不修改 Pipeline / Scheduler 通用 ABI。
- 保持 Protocol × Compression × Format 正交。
- 允许未来为不同执行计划选择更适合的 deterministic segment planner。

Negative:

- Manifest 需要持久化 SegmentPlan identity。
- resumable MultiFile / Partitioned 只有在 planner 能证明 stable segment identity 时才能启用。
- Phase 4 需要新增 resume validation 与 stale incomplete-file recovery 测试。

## Compatibility Impact

不改变 SQL 查询结果语义。

不要求 DuckDB 的 COPY 文件命名或线程调度行为成为恢复契约；本项目的 file-level resume 是 AD-009 已有的额外工程保证。

Phase 4 尚未 ACCEPTED，因此 Manifest v1 可在本阶段加入 SegmentPlan 字段而不产生已发布格式迁移问题。

## Required Tests

至少包括：

1. 同一 QueryFingerprint + SourceSnapshot + SegmentPlan 可重复接受；任一 identity 漂移拒绝 resume。
2. `COMPLETE` 文件 resume 时不重写。
3. `WRITING` / `FAILED` 文件 resume 时整文件重做。
4. 重启恢复后无 duplicate completed file、无 missing file。
5. Cancel/failure 后 writer、reservation、client/session lease、incomplete multipart 归零。
6. SingleFile incomplete restart 全文件重做。
7. 非 stable planner 不得启用 resumable MultiFile / Partitioned。

## Affected Frozen Decisions

本 ADR 是对以下冻结决策的批准补充：

- AD-003 Execution Model — 通用 Pipeline DAG 保持不变；
- AD-009 COPY / Export — 补全 file-level resume 的 durable file identity；
- AD-014 Query Lifecycle — resume attempt 的资源仍归 QueryContext 管理；
- AD-021 Consistency Domains — Statement SourceSnapshot 与 Export Manifest Commit 仍分离；
- AD-031 Go Runtime / Buffer Ownership — 不引入无界未管理 buffer；
- I-07 Statement ScanSet/SourceSnapshot frozen；
- I-10 Lifecycle Protocol 继续强制执行。

## Approval

Approved by the project owner on 2026-11-01.

批准范围仅为 Stable Export SegmentPlan contract 与 `EXPLICIT_V1` 初始策略；row-hash、local spool、Pipeline ABI、Scheduler ABI 均未被本 ADR 批准为冻结实现。
