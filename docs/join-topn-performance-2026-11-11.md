# JOIN / TopN 性能复测（2026-11-11）

本轮基线为 `d431702a11bdfaefd2a839a0ec3f6189c49713e5`。仅优化实际 profile 中的两个热点，尚未达到 DuckDB 的端到端性能。

## 实现

- VARCHAR TopN 按批借用字符串引用和 heap，缓存堆顶首排序键；替换候选后使缓存失效，首键相同时继续比较后续排序键。
- JOIN 批量键比较直接读取 flat 定宽数据，整批无 NULL 时跳过逐行 validity 查询；常量、字典和短输入向量保留原视图路径，浮点比较继续使用 SQL 语义。
- 增加字符串 TopN 的跨批、NULL 排序、ASC/DESC、空串、Unicode、长字符串、常量和嵌套字典回归，以及 JOIN 键比较的 flat/constant/dictionary、NULL、NaN、正负零和 Infinity 回归。

## 算子微基准

Windows/amd64，Intel Core i7-10510U，Go 1.27.3。构造 fixture 不计时。优化前后使用各自保留的测试二进制；无 profile 并发、无其他测试并发，按轮交替先后顺序，各 2 次预热、7 次测量，每次 `-test.benchtime=300ms`。下表为中位数，不代表 DuckDB 对比。

| 用例 | 优化前 | 优化后 | 耗时下降 | 前/后 MAD |
| --- | ---: | ---: | ---: | ---: |
| VARCHAR TopN，32,768 行、K=100、升序输入 | 3.994ms | 1.883ms | 52.9% | 4.1% / 9.8% |
| VARCHAR TopN，同规模、打散输入 | 4.621ms | 2.638ms | 42.9% | 1.1% / 2.7% |
| Resident probe，1000 个键、完整 payload | 355.6µs | 328.3µs | 7.7% | 5.9% / 6.4% |
| Resident probe，113 个键、重复链、完整 payload | 3.821ms | 3.292ms | 13.8% | 2.3% / 9.8% |

每次操作的分配次数保持不变：两项 TopN 为 352，JOIN 分别为 23、207。B/op 存在少量运行时噪声。JOIN 基准使用预哈希输入，并强制比较通用驻留 probe；唯一整数键在生产入口可走已有 dense index，不能把此表收益直接套用于该入口。

原始样本、命令、二进制 SHA256 和复测脚本：`.cache/join-topn-profile/{micro-comparison.json,compare_micro.py}`。优化前 profile 显示 VARCHAR TopN 的 `compareInputRow` 累计占 52.2%，`Buffer.Bytes` 占 25.6%（包含在调用链中，不能相加）；JOIN 的 `SelectEqual` 累计占 15.4%。profile 单独运行，不进入以上样本。

## DuckDB 端到端复测

复用相邻 `../fssql-bench` 的批消费 harness：fssql QueryChunks、DuckDB Arrow reader，完整消费计时、独立查询校验结果。数据为现有合成 `large-data/data.parquet`（200,000 行）和 `dim.parquet`；DuckDB 2.0.0、PyArrow 24.0.0，双方单 worker、256MiB 内存，各 2 次预热、7 次测量。两引擎顺序执行，未与其他测试并行；这次未按单个样本交替引擎，仍有时间漂移限制。

| 查询 | fssql | DuckDB | fssql / DuckDB | 结果摘要 | 前者/后者 MAD |
| --- | ---: | ---: | ---: | --- | ---: |
| INNER JOIN + COUNT/SUM | 85.3ms | 17.2ms | 4.97x | PASS | 1.9% / 5.1% |
| LEFT JOIN + COUNT | 109.7ms | 26.1ms | 4.21x | PASS | 3.0% / 1.7% |
| 数值 TopN，K=100 | 83.5ms | 18.1ms | 4.61x | PASS | 2.7% / 2.6% |
| VARCHAR TopN，K=100 | 349.8ms | 96.3ms | 不纳入有效倍数 | PASS | **15.4%** / 2.1% |

结果摘要包含 ORDER BY 顺序，但不替代完整类型差分。时间包含不同语言 API 的绑定与批导出成本，不能作为纯算子时间。VARCHAR 用例超过项目 10% 波动门槛，不能据此发布有效倍数。

查询、实际 fixture/source/binary 指纹、原始样本和报告保留于 `.cache/join-topn-profile/{queries.json,fssql.json,duckdb.json,report.md}`。

## 剩余热点与验证

另外单独运行两项端到端 CPU profile，各连续查询约 3 秒，不与测试或性能测量并行。INNER JOIN 的 `local.Driver.localPath` / `filepath.EvalSymlinks` 累计占 39.6%；数值 TopN 分别为 43.8% / 43.2%。Parquet `readRange` 累计占 38.7% / 55.6%，数值 `PackedTopN.Consume` 累计占 13.3%。这些调用链相互包含，不能相加，也不能直接换算为可实现的 wall time 收益。

因此，本机本地 Parquet 查询的剩余差距有显著的重复路径解析/范围读取成本，不能仅归因于 JOIN 哈希或 TopN 堆。后续优先验证读取句柄在查询中的复用，同时保持根目录校验和文件快照契约。本轮未修改存储层；这两项采样不代表远端存储或其他操作系统的热点。

profile 程序、CPU 样本和 flat 排名位于 `.cache/join-topn-profile/{profile_e2e.go,join-e2e.cpu,topn-e2e.cpu,join-e2e-top.txt,topn-e2e-top.txt}`；实际生产代码补丁保留为同目录 `production.patch`。

以下五包普通测试和完整 race 均通过：

```powershell
go test ./internal/execution/hash ./internal/execution/sort ./internal/execution/join ./internal/session ./pkg/gosql
go test -race ./internal/execution/hash ./internal/execution/sort ./internal/execution/join ./internal/session ./pkg/gosql
```

完整 API race 用时 309.6 秒；新增常量 NULL 分支另外通过定向 race。测试日志位于 `.cache/join-topn-profile/{tests.log,race.log,api-race-targeted.log}`。系统临时目录曾触发本地路径权限错误，将 `TEMP/TMP/GOTMPDIR` 指向仓库 `.cache/test-tmp` 后复测通过。未执行整个仓库的 `go test ./...`。

复现命令（仓库根目录；两个引擎串行执行）：

```powershell
$env:GOCACHE = Join-Path $PWD '.cache/go-build'
$env:TEMP = Join-Path $PWD '.cache/test-tmp'
$env:TMP = $env:TEMP
go -C ../fssql-bench build -o ../fssql/.cache/join-topn-profile/fssql-bench.exe .
./.cache/join-topn-profile/fssql-bench.exe -queries .cache/join-topn-profile/queries.json -workers 1 -config default -warmups 2 -runs 7 -engine-repo . -out .cache/join-topn-profile/fssql.json
./.cache/agent-verification/venv/Scripts/python.exe ../fssql-bench/duckdb_bench.py --queries .cache/join-topn-profile/queries.json --threads 1 --config default --warmups 2 --runs 7 --out .cache/join-topn-profile/duckdb.json
./.cache/agent-verification/venv/Scripts/python.exe ../fssql-bench/compare.py --fssql .cache/join-topn-profile/fssql.json --duckdb .cache/join-topn-profile/duckdb.json --out .cache/join-topn-profile/report.md
```
