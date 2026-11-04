# DuckDB 对标性能优化记录（2026-11-04）

本轮按顺序由指定子代理实现，每项由主代理复核后继续下一项。原始输入为 `.cache/agent-verification/retest-optimized-2026-11-04/`，基准工具位于相邻仓库 `../fssql-bench`。

## 实施状态

| 顺序 | 范围 | 状态 | 验证与结论 |
| --- | --- | --- | --- |
| 1 | 统一结果消费与基准证据；最终有序输出与校验器 | 已完成 | pipeline/session/gosql 普通与 race 通过；基准 Go 和 5 个 Python 测试通过；8,193 行真实 Parquet smoke 三项摘要 PASS |
| 2 | VARCHAR TopN 与预算不足时的正确回退 | 已完成 | sort/session/gosql 普通与 race 通过；128KiB/4096 行 Consume 与 90KiB Emit 硬上限定向 race 通过 |
| 3 | 批量哈希、谓词、聚合与列复制热路径 | 已完成 | vector/hash/expression/sqltransform/aggregate/pipeline/gosql 七包最新普通与 race 均通过 |
| 4 | 低基数、字典分组与 Parquet 字典执行 | 已完成 | vector/parquet/aggregate/sqltransform/gosql 五包普通与 race 均通过 |
| 5 | JOIN 批量 probe、读取与输出物化 | 已完成 | join/hash/optimizer/planner/lowering/session/gosql 七包最新普通与 race 均通过 |
| 6 | 根据 profile 优化并行瓶颈并做集成验证 | 进行中 | 待 profile 和集成证据 |

## 原始证据的限制

- 现有 DuckDB harness 在 `fetchall()` 中计时，Go harness 只消费 chunk 行数；大结果集不能据此得出引擎性能倍数。
- 旧报告的 Arrow 时间来自硬编码常量，不能作为本轮同环境复测的替代品。
- 多项样本的 MAD/中位数超过项目测试标准的 10% 阈值，适合定位方向，不作为发布性能结论。
- ORDER BY 的结果必须验证顺序；不能通过改成无序摘要绕过不一致。

## 后续优化依据

- 实际 `user-1m.parquet` 的 `column1` 是 `BYTE_ARRAY / PLAIN_DICTIONARY`，元数据值域为 `T1` 至 `T5`；`column15` 是 `INT64 / PLAIN`。
- DuckDB 2.0 对该分组 SQL 的实际 EXPLAIN 为 `HASH_GROUP_BY`，不能假设本场景使用整数 perfect hash。
- 优化前现有 `BenchmarkHashChunkBigIntVarchar` 三次诊断测量为 269936、271813、262141 ns/op，均为 0 B/op、0 allocs/op。此为同机微基准起点，不代表 DuckDB 对比结果。

最终性能结论应遵循 [项目测试标准](testing-standard.md)，记录实际源码、数据、查询、配置和样本，区分微基准、API 端到端与正确性验证。

## 第 1 项：基准与有序输出

- `Pipeline.PreserveOrder` 只串行化最终可证明有序的 stage，上游依赖仍按配置并行。新增跨批 ORDER BY + OFFSET/LIMIT 与依赖实际 4 worker 的回归测试。
- 外部基准使用 DuckDB 2.0 的 `con.sql(sql).to_arrow_reader(2048)`，计时覆盖完整批消费与 reader.close；Go 使用 QueryChunks。报告说明两种 API 的绑定与收尾边界差异。
- 移除历史 Arrow 常量，显式指定结果文件；记录源码/二进制、数据和查询指纹。默认 2 次预热和 7 次测量，不兼容、波动超限或校验不通过的用例不进入有效倍数汇总。
- 小规模证据：`.cache/benchmark-contract-smoke/{fssql.json,duckdb.json,report.md}`。仅 1 次预热和 1 次测量，属于正确性 smoke，不能据此宣称性能等价。
- 可复用命令与脚本说明见相邻基准仓库的 `README.md`。
- 已保留阶段 1 的实际基准二进制 `.cache/performance-optimization-2026-11-04/baseline-stage1.exe`，SHA256 为 `55603d5d0b01ddcaae4765839b9605c1c2b3242229d2b377daf8a99d91a77a14`；来源证据为同目录 `baseline-provenance.json`。后续前后对比须使用这份实际构建来源，不能用运行时的当前 checkout 指纹冒充旧二进制来源。

## 第 2 项：VARCHAR TopN

- 变量长输入进入可溢写 TopN；超过候选预算或 managed 硬上限时，把前缀候选与未消费后缀交给已有外部排序。按失败行接续，避免重复或丢失数据。
- 回退后的 builder 逐行检查预算，分配失败时可先溢写已提交数据再重试当前行。消费者错误与已经开始投递的结果不会被重放。
- 小 K 的输出按 `min(2048, K)` 分配；外部回退只输出前 K 行。定宽预分配路径保持。
- 回退前为 builder 增长预留排序索引与溢写工作区；首次输出分配失败时允许缩小批次。单值或必要工作区本身放不下时仍返回真实资源错误。
- 32,768 行 VARCHAR、K=100 的同输入微基准（300ms、3 次，排除 fixture 构造）：顺序输入 TopN 中位 6.09ms、完整排序 100.74ms；打散输入分别为 8.52ms、104.97ms。约 25KB/op 对 4.02MB/op。这些是算子诊断值，不是 DuckDB 对标结果。

## 第 3 项：批内核与输入所有权转移

- 新增批量字节视图及按批选择的哈希、比较、聚合数值解码器，移除这些热路径中的逐值缓冲区获取。窄无符号整数的批哈希与溢写行哈希保持一致。
- Pipeline 在独占输入时可调用 `OwnedTransform`：纯列 Project 移动单次引用列，复制重复列；Filter 用拥有 child 的 dictionary selection 保留选中行。普通 Transform 和 borrowed TransformSink 继续遵守独立输出契约。
- 覆盖失败回滚、重复列独立性、借用边界，以及 NULL、NaN/负零、DECIMAL128、BLOB、纳秒时间戳、constant/nested dictionary 的哈希一致性。
- 同机短微基准：混合键 HashChunk 中位 95.3µs/2048 行（优化前 269.9µs，约 2.83 倍，仍 0 B/op、0 allocs/op）；Owned Project 移动 5.87µs 对复制 25.86µs。
- 全局聚合保留原通用 batch 路径，仅为 flat FLOAT/DOUBLE SUM/AVG 增加定型循环。相同输入的原 batch 对照中位 18.476µs，新路径 3.420µs，均 0 B/op、0 allocs/op；比较基线是原 batch，不是 scalar reference。
- 直接谓词路径分配从 11,816 降至 9,208 B/op、16 降至 11 allocs/op，耗时差异不显著，不宣称该用例加速。

## 第 4 项：字典扫描与低基数分组

- 小型 VARCHAR/BLOB Parquet 字典可保留为 owned dictionary 输出；上限 256 项、32KiB，且当前 dictionary 页须容纳整批。跨页、PLAIN、大字典或分配不足在消费索引前回到原 flat 路径。
- 单列字典分组只处理实际被引用的 entry，再映射到逐行 group ID，避免未引用项产生空组；支持 nested selection、重复 entry、NULL、NaN/正负零。多键、高基数或深度超过优化范围时保留通用路径。
- 输出生命周期独立于 cursor，相关回归覆盖关闭后读取、分配失败回退、跨页混合编码、过滤后无幻影组、浮点语义及 4 worker 强制溢写合并。
- 2048 行 COUNT+SUM 的三样本诊断中位数：5 组 flat/dictionary 为 595/125µs，256 组为 855/257µs；1000 组走通用回退，为 764/806µs，不宣称高基数加速。5 组字典路径约 528 B/12 alloc，对照 flat 248 B/7 alloc，CPU 收益伴随少量有界元数据开销。

## 第 5 项：JOIN 批量执行与必要输出

- resident partition 每批借用一次不可变记录读取器，批量探测 heads，以 active selection 多轮推进重复链，并按列压缩匹配选择；FULL 的 matched bitmap 每轮集中更新。
- 定宽 payload 直接批量复制，VARCHAR/BLOB 走批量 writer。完整宽度的可选 `RequiredOutput` mask 从 logical/physical 计划经 lowering 可选 setter 传入 runtime，不改变已有 Factory 接口或列索引；未使用列保留 compact constant NULL。resident 与 spill 路径都遵守 mask。
- 新增真正多 chunk、4 worker、双 build side、外/半/反连接、字典与常量 payload、空输出 mask、WHERE/ORDER BY 隐藏列和取消/配额路径的回归。溢写取消测试发现的局部 child PageList metadata 泄漏已修复。
- 同输入串行隔离微基准（300ms、3次，prehashed，同 emitter）：1000 个键无 payload 为 scalar/batch 584.3/473.4µs，全 payload 770.9/698.5µs；113 个键重复链分别 4157.6/2205.5µs、6789.7/4464.8µs。分配次数不变。这些是 probe 诊断结果，不是 API 或 DuckDB 性能结论。

## 集成验证安排（进行中，尚无最终结论）

- 在第 5 项通过后，单独采集 CPU、mutex、block profile，只优化有实际证据支持的剩余热点；profile 运行不能混入发布性能样本。
- 保留阶段 1 二进制和实际构建来源，最终二进制重新 build。使用同一查询、fixture、内存与 worker 配置，交替运行前后版本，至少 2 次预热和 7 个样本；性能测量期间不并行运行其他测试或微基准。
- DuckDB 使用新 Arrow harness 实测，不能复用旧常量；校验、稳定性不合格的用例明确标记，不纳入有效倍数。基准二进制运行时采集的当前 checkout 元信息不能替代保存的旧构建来源。
- 最后执行全量普通测试及相关 race，确认新字典、所有权、JOIN、TopN 和溢写路径的组合行为。用户删除的三个旧替换计划/阶段报告文档不在本轮修复范围内，保持删除状态。
