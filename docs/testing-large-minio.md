# 大文件、SQL、资源与 Benchmark 验证

## 维度与数据

本套测试复用现有 MinIO，通过真正的 `Open → QueryChunks/Exec → S3 VFS` 路径执行。数据来自专用 `fssql-agent-test-` bucket；PostgreSQL 数据源尚未实现，PG 不作为引擎查询通过项。

主数据集包含 100 万行、6 列：ID、分组、正负整数、含 NULL 的整数、打乱顺序的排序键和 256 字节字符串。每个 Parquet row group 最多 32768 行。不同格式保存同一逻辑数据，便于独立交叉检查。

| 输入 | 大小 | 用途 |
| --- | ---: | --- |
| 无压缩 Parquet | 299,960,006 B | 列扫描、过滤、排序、聚合、Join、COPY、取消、并发 |
| 无表头 CSV | 283,038,719 B | 大文本扫描、LIMIT、内存预算对照 |
| NDJSON | 344,274,015 B | 大文本扫描、内存预算对照 |
| 31 个窄 Parquet 文件 | 24,037,651 B | glob、多文件统计 |
| JSON 数组及小维表 | 418,589 B | JSON 流、Join、子查询 |

场景覆盖：

- 窄列/宽列扫描、选择性过滤、空结果、LIMIT/OFFSET、表达式与 NULL。
- COUNT/SUM/AVG/MIN/MAX、GROUP BY/HAVING、DISTINCT、CTE、受支持的标量子查询。
- INNER/LEFT/RIGHT/FULL/CROSS/SEMI/ANTI Join，排序/聚合/Join 的内存执行与强制 spill 对照。
- 分组列上的确定性分层抽样、固定容量的客户端 reservoir 抽样。
- 原生 SQL 抽样、random、CAST、取模及子查询组合的实际支持边界。
- 四个查询并发、1/4 worker 对照、20 次连续查询。
- 取消、消费端失败、内存不足、临时空间不足、COPY 大结果回读。
- 默认 256MiB 预算下的大文本读取，以及 1GiB 下的首块诊断。

## 正确性与统计方法

`test/integration/minio_large_fixture.py` 分批生成文件，并用 DuckDB 2.0.0 计算完整参考结果。显式 ORDER BY 查询比较有序 SHA256；无序查询比较行 SHA256 的模 2^256 和，保留重复行计数。未排序 LIMIT 不假设返回特定子集，改为检查数量、ID 范围和每行 payload 的确定性内容。

Reservoir 抽样是结果消费端 Algorithm R，容量固定为 1000，使用跨 Python/Go 一致的固定随机序列对照完整结果。另有 128 个固定种子的唯一性、范围和分层覆盖 property test。它不代表引擎实现了 TABLESAMPLE 或 random()。

每个场景记录耗时、进程 CPU 时间、CPU 平均/峰值、RSS、Go heap、累计分配、GC、受管内存、临时文件和真实 HTTP 读取量。受管峰值来自 MemoryManager；RSS/heap/临时文件每 20ms 采样，CPU 峰值至少积累 100ms。瞬时峰值可能漏采，不能把采样值当作硬上限。CPU 100% 表示一个逻辑核；本机共 12 个逻辑核。

场景结束必须满足受管内存及查询临时文件归零，成功的 Range 请求必须返回 206，不允许静默无 Range 整文件 GET。该检查与“即使使用 Range 仍扫描全部数据”的性能缺口分开报告。

## 执行

需要 Python 的 boto3、DuckDB 2.0.0、PyArrow；凭据保存在被忽略的 `.cache/agent-verification/minio-test-env.json` 中，字段与 `FSSQL_MINIO_*` 环境变量同名。大文件与构建缓存也放在当前仓库的被忽略目录。

可复用的执行、对比和汇总脚本均位于 `test/integration/`，不依赖忽略目录中的脚本副本。依赖版本见 `test/integration/requirements.txt`。在与本次相同的 Windows + WSL 服务环境中，可使用下列命令：

```powershell
python -m pip install -r test/integration/requirements.txt
python test/integration/prepare_minio_services.py
python test/integration/minio_large_fixture.py
python test/integration/run_minio_large.py
python test/integration/run_minio_large.py --bench
python test/integration/run_minio_large.py --race
python test/integration/run_minio_large.py --workers --race
python test/integration/run_minio_large.py --defaults
python test/integration/run_minio_large.py --joins
python test/integration/profile_minio_large.py
python test/integration/benchmark_core.py
python test/integration/verify_large_regression.py
python test/integration/summarize_minio_large.py
```

`prepare_minio_services.py` 复用 `docker-minio-1`、`docker-postgres-1`，从容器配置读取凭据且不输出凭据；不会部署新服务。默认使用本机 9000/5432 端口，数据写入专用测试 bucket。执行脚本会覆盖同名本地报告；如需保留新一轮基线，请先备份上一轮 `artifacts/local-validation/2026-11-02/large/`。

```powershell
python test/integration/minio_large_fixture.py
$connection = Get-Content .cache/agent-verification/minio-test-env.json | ConvertFrom-Json
$connection.PSObject.Properties | ForEach-Object { Set-Item -Path "Env:$($_.Name)" -Value $_.Value }
$env:FSSQL_LARGE_MANIFEST = (Resolve-Path .cache/agent-verification/large-manifest.json).Path
$env:FSSQL_LARGE_REPORT = Join-Path $PWD 'artifacts/local-validation/2026-11-02/large/manual.jsonl'
go test -v -count=1 -timeout=30m -run '^TestMinIOLargeMatrix$' ./pkg/gosql
```

选择场景时设置 `FSSQL_LARGE_CASES`，名称用逗号分隔。Race 的运行时开销明显，使用有界的更长超时，保留相同结果断言：

```powershell
$env:FSSQL_LARGE_TIMEOUT = '10m'
$env:FSSQL_LARGE_CASES = 'global_aggregate,topn,aggregate_spill,join_spill,cancel,consumer_error,concurrent_queries'
go test -race -v -count=1 -timeout=30m -run '^TestMinIOLargeMatrix$' ./pkg/gosql
go test -race -v -count=1 -timeout=30m -run '^TestMinIOLargeWorkers4$' ./pkg/gosql
go test -v -count=1 -run '^TestMinIOLargeDefaultBudget$' ./pkg/gosql
```

## Benchmark 与 Profile

```powershell
go test -run '^$' -bench '^BenchmarkMinIOLarge$' -benchtime=1x -count=3 -benchmem -timeout=30m ./pkg/gosql
```

9 组 benchmark 每组 3 个样本；回调仅累计行数，排除逐行摘要校验。报告包含 ns/op、rows/s、实际网络字节、CPU、RSS、heap、受管峰值、临时文件及 Go 分配。资源采样本身也有开销。测试没有清除共享服务的文件缓存，属于本机 warm baseline；DuckDB 只作为正确性参考，不将本地 DuckDB 耗时与远端 MinIO 耗时直接比较。

另对一次窄列查询采集 CPU/heap profile。现有向量、Hash、Join、Aggregate、Sort、Distinct microbenchmark 独立运行，不能与远端 SQL 吞吐混为一项。

## 已发现的问题

大文本默认串行扫描要求整文件缓冲；Parquet 生产路径未接受列/谓词/LIMIT 下推；Top-N 本轮与全排序耗时及 spill 接近。错误清理通过不等于这些性能与能力门槛通过。缺口已记录到 `compatibility/deviations.yaml`，完整证据见[大负载报告](../artifacts/local-validation/2026-11-02/large/report.md)。
