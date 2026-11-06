# JOIN 第二轮优化验证

本轮在上一轮九项查询优化完成后的工作区上继续优化 JOIN。百万行主场景的本地中位耗时从 **123.28ms 降到 70.41ms（1.75 倍）**，与同轮 DuckDB 的耗时比从 **11.35 倍降到 6.48 倍**。MinIO 主场景从 **258.21ms 降到 205.14ms（1.26 倍）**，约为 DuckDB 的 **1.08 倍**。

## 实现

参考 [DuckDB 的 perfect hash JOIN 实现](https://github.com/duckdb/duckdb/blob/main/src/execution/operator/join/perfect_hash_join_executor.cpp)中按整数范围直接寻址、验证唯一性后按批次选择匹配行的思路，结合现有 managed memory 和 hybrid spill 生命周期实现：

- 为单整数等值 INNER JOIN 增加可选直接索引。构建端必须全部驻留；以实际数据验证键范围、密度和唯一性，基数估计仅决定构建侧。支持有符号、无符号 8/16/32/64 位整数及 NULL 等值语义。
- 每个槽保存八字节分区号和行偏移引用，借用冻结后的构建数据，保留变长 payload。范围最多 1,048,576 项，且槽数组最多使用算子预算的 1/16；总驻留预算和全局 managed memory 限制仍然生效。配额不足时回退通用路径，其他分配错误正常返回。
- 重复键、稀疏范围、复合键、其他类型、非 INNER JOIN 及有溢写的构建端保留通用哈希语义。NULL 在普通等值条件下不匹配；NULL 等值条件下重复 NULL 也触发回退。
- 输出选择数组改由调用栈持有，消除每个 probe 批次约 64KiB 的两次堆分配；无共享对象池。定宽构建列的物化在批次入口验证偏移，随后逐列直接收集数据，避免每个值重复解析行布局。这两项也惠及通用哈希路径。

## 端到端计时

复用现有 `test/integration/duckdb_compare.py` 的配置和百万行 Parquet 数据：事实表 1,000,000 行，维表 1,000 行。本地文件与实际 MinIO 分别计时；两引擎均一个 worker/thread、64MiB 内存上限，fssql 原有算子预算 4MiB。查询包含解析、绑定、规划、执行及完整消费，数据库初始化和资源采样设置不计入查询耗时。

每项对优化前二进制、优化后二进制、DuckDB 随机交替采样，固定种子 20261107；各预热两次、测量五次，使用中位数。正式计时期间不运行其他回归或 profile。`samples.jsonl` 保留全部 126 个样本（含预热）、资源指标和样本序号；`summary.json` 保留 MAD 与二进制 SHA-256。

复测脚本为本目录的 `compare.py`。需保留 `.cache/join-optimization/before.test.exe`（上一轮版本，SHA-256 见 `summary.json`）、原有 `.cache/agent-verification/large-data` 数据及 MinIO 测试配置，并安装现有对比器所需的 DuckDB/Arrow 依赖。执行 `python docs/validation/2026-11-06-join-performance/compare.py` 会编译当前版本并把新样本写入 `.cache/join-optimization/`；`--profile` 单独执行四十次本地主场景并保存 CPU/分配 profile，不进入计时统计。

| 来源 | 场景 | 优化前 ms | 优化后 ms | DuckDB ms | 本轮加速 | 优化后 / DuckDB |
|---|---|---:|---:|---:|---:|---:|
| 本地 | inner_join | 123.28 | 70.41 | 10.86 | 1.75x | 6.48x |
| 本地 | inner_join_holes | 104.43 | 60.14 | 12.29 | 1.74x | 4.89x |
| 本地 | inner_join_duplicates | 170.70 | 149.38 | 32.38 | 1.14x | 4.61x |
| MinIO | inner_join | 258.21 | 205.14 | 190.24 | 1.26x | 1.08x |
| MinIO | inner_join_holes | 242.58 | 201.05 | 204.39 | 1.21x | 0.98x |
| MinIO | inner_join_duplicates | 322.31 | 301.73 | 210.25 | 1.07x | 1.44x |

主场景查询：

```sql
SELECT COUNT(*), SUM(d.weight)
FROM read_parquet('{data}') f
JOIN read_parquet('{dim}') d ON f.grp = d.grp;
```

`inner_join_holes` 将维表替换为 `SELECT * FROM read_parquet('{dim}') WHERE grp % 2 = 0`，使构建键域含空洞且一半事实行不匹配。`inner_join_duplicates` 将维表替换为同一维表的两次 `UNION ALL`，验证通用路径的计时，JOIN 输出基数为主场景的两倍。三项最终聚合结果均为一行，计时器检查完整消费和最终行数；结果内容另由下述独立测试验证。

主场景本地总分配中位数从 **77,437,440 字节降到 45,077,608 字节（减少约 42%）**；MinIO 从 79,315,272 降到 46,885,272 字节。所有正式 fssql 样本结束时 managed memory 和临时空间均归零。

## 正确性和生命周期

- `go test ./internal/... ./pkg/gosql ./examples/quickstart` 通过；最终补充用例和形状校验后，完整 hash/JOIN 包再次通过。
- `go test -race ./internal/execution/join ./internal/execution/hash -run 'Test(DenseInteger|ResidentBatch|BatchProbe|NullSafe|ExternalForcedSpill|SelectEqual)' -count=1` 通过。
- 新增独立嵌套循环参考结果，覆盖八种整数类型、BIGINT 最小/最大值、无符号最大值、跨符号范围、域内空洞与域外值、普通/NULL 等值、嵌套字典和常量 probe、双向 build、变长/NULL payload、重复键和重复 NULL、稀疏/全域范围、空表与全 NULL。
- 覆盖可选索引配额失败回退、非配额错误、构建中取消、下游取消、四 worker 读共享冻结索引、唯一键构建被迫溢写；确认错误和成功路径资源释放。
- 实际 MinIO 百万行 `TestMinIOLargeMatrix` 的 `inner_join`、`group_having` 通过完整类型化结果摘要校验；后者补充验证共享列物化代码。具体结果见 `correctness.jsonl`，managed/temp after 均为零。

## 剩余差距

本地主场景仍比 DuckDB 慢 6.48 倍。直接索引以外的通用路径本轮收益较小，重复键场景本地加速 1.14 倍、MinIO 加速 1.07 倍。当前没有将直接索引扩展到重复键、复合键、外连接或溢写分区。

profile 已确认主场景的通用哈希、分区重排和逐键比较开销被消除；剩余工作包括本地读取及文件身份检查、列物化和后续投影。这些数据仅代表当前 Windows、本地缓存和本机 MinIO 环境，不能据此宣称其他 JOIN 分布或部署环境已接近 DuckDB。
