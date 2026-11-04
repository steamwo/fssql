# Architecture & Release Verification Contract

测试实施与判定统一遵循 [项目测试规范与 DuckDB 对标方案](../testing-standard.md)。发布必需项为 BLOCKED、NOT_RUN 或被跳过时，不满足本契约；测试规范中的建设目标不代表已获得验收证据。

## 每阶段 Gate

每个 Phase 都必须通过：

1. Correctness Gate
2. Performance Gate
3. Memory Gate
4. Failure Gate

## Feature Freeze

v1 Feature Complete 后进入 Feature Freeze。

只允许：
- bug fix
- security fix
- performance regression fix
- release blocker
- test / benchmark / documentation improvement

禁止新增 Stable Feature。

## v1 Stable 条件

```text
Stable Feature Complete
+ SQL Compatibility Verified
+ Correctness Verified
+ Forced-Spill Verified
+ Interop Verified
+ Failure Matrix Passed
+ Security Review Passed
+ Performance Baseline Established
+ Resource Envelope Documented
+ Operational Documentation Complete
```

## Acceptance Evidence

每阶段至少生成：
- acceptance-report.md
- benchmark-report.md
- memory-report.md
- failure-report.md
- known-limitations.md
