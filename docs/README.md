# Go 云原生 OLAP 查询引擎 — AI Agent 分阶段开发指导包

版本：V2.1-Agent-Ready  
状态：Architecture Frozen / Ready for Phase 0

本包的目标不是“给人看的设计说明”，而是给 AI coding agent 直接读取并按阶段执行。

## Agent 必须按此顺序读取

1. `AGENTS.md`
2. `machine/project-state.yaml`
3. `architecture/decisions.md`
4. `architecture/invariants.md`
5. `architecture/module-boundaries.md`
6. `architecture/feature-contract.md`
7. 当前阶段对应的 `phases/phase-N-*.md`
8. 当前阶段引用的 `contracts/*.md`

## 核心规则

- `architecture/decisions.md` 是最高优先级的架构事实源。
- 已标记 `FROZEN` 的决定，普通实现 PR 不得修改。
- 每次只实现一个阶段；前一阶段未 `ACCEPTED` 时，不得把下一阶段能力当稳定依赖。
- 阶段完成不是“代码写完”，而是“交付物 + Gate + 验收证据齐备”。
- 若实现与架构冲突，先停下并创建 ADR，不得静默改变设计。
- 所有性能目标必须有 benchmark 证据，所有故障安全目标必须有 fault-injection 证据。
- Stable SQL 默认以 DuckDB 语义兼容为目标。

## 阶段顺序

```text
Phase 0 Kernel PoC
  ↓
Phase 1 S3 + Parquet
  ↓
Phase 2 Join/Aggregate/Sort + Spill
  ↓
Phase 3 CSV/JSON/NDJSON + SFTP
  ↓
Phase 4 COPY / Export
  ↓
Phase 5 FTP/WebDAV + Production
  ↓
Phase 6 Feature Freeze / v1 Release
```

## 每阶段输出

Agent 在每阶段结束时必须生成：

```text
artifacts/
  phase-N/
    acceptance-report.md
    benchmark-report.md
    memory-report.md
    failure-report.md
    known-limitations.md
```

并更新：

```text
machine/project-state.yaml
```

只有 Acceptance Decision = `ACCEPTED` 才允许进入下一阶段。
