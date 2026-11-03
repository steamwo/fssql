# DuckDB 替换阶段 0 验收报告

日期：2026-11-03  
分支：`remediation/mem-001`  
计划：`docs/duckdb-replacement-plan.md`

## 结论

阶段 0 的仓库内基线已固化，但**尚不能判定阶段 0 完成**。原因是计划要求以调用方当前自编译加密扩展生成 AES-128/AES-256 `PARE` 样本并由现有读取路径验证；当前 `steamwo/fssql` 仓库及已连接的 GitHub 安装中没有该扩展源码、二进制、调用方仓库或加密样本。缺少事实基线时不以通用 Parquet 加密格式替代项目互操作要求。

## 已固化

- Stable v1 与迁移基线分开记录。Stable 差分继续使用 `test/compat/stable.slt`，DuckDB 参考版本固定为 `2.0.0`。
- `compatibility/duckdb-replacement-phase0.json` 建立业务兼容矩阵；每项记录输入类型、SQL 语料、预期 schema、预期结果、资源预算、实现状态、验收状态和证据。
- `test/corpus/duckdb-replacement-phase0/sql.json` 固定替换计划需要的 SQL 验收形状；由于调用方编译器不可访问，需要调用方精确 SQL 的条目明确标为 `caller_capture_required`，不冒充已抓取语料。
- `test/corpus/duckdb-replacement-phase0/types.json` 固定 UBIGINT、DECIMAL(38,9)、BLOB、TIMESTAMP_NS、TIMESTAMPTZ、NULL 等边界样本。
- `test/corpus/duckdb-replacement-phase0/encryption/manifest.json` 固定 `PARE`、`masterKeyID`、AES-128/AES-256、错误密钥拒绝的互操作门槛，并把缺失外部样本标为阻塞。
- `test/compat/duckdb_replacement_phase0_test.go` 校验版本基线、矩阵完整性、SQL 语料引用、风险状态及加密阻塞，防止把待实现/待验收项误报为支持。

## 风险复核

| 风险 | 已有实现 | 小规模测试 | 大文件验收 | 当前状态 |
|---|---|---|---|---|
| MEM-001 | CSV/NDJSON 有界串行 range/window 已存在 | 已有 | 修复后尚未重跑原 283/344 MiB 及更大样本，64/256 MiB 预算待验收 | Open |
| PERF-001 | Parquet projection/filter/limit pushdown 已存在 | 已有 | 修复后尚未重跑百万行物理 I/O | Open |
| SQL-SAMPLE-001 | 原生 sampling SQL/random/CAST/modulo 能力未完整实现 | 分层谓词与客户端 reservoir 是独立路径，不能代替 | 实现前不适用 | Open |

`compatibility/deviations.yaml` 已同步更新 MEM-001/PERF-001 描述，使其反映“实现/小测已有，但大规模验收未完成”，而不是继续描述修复前代码状态。

## 阶段 0 未满足项

1. 从实际调用方抽样、脱敏编译器和类型化导出流程抓取精确生成 SQL，并替换/确认 `caller_capture_required` 条目。
2. 固定自编译扩展版本或二进制摘要。
3. 用该扩展生成 AES-128、AES-256 `PARE` 样本。
4. 实现最小纯 Go 写出原型，并通过现有读取路径的正确密钥读取、错误密钥拒绝、`masterKeyID` 元数据识别。
5. MEM-001/PERF-001 的大规模复验属于后续阶段验收证据；在复验前保持 Open。

因此本提交完成了阶段 0 的仓库内可完成部分，并把外部依赖变成可审计的硬阻塞；没有通过跳过测试或降级格式宣称阶段 0 已通过。
