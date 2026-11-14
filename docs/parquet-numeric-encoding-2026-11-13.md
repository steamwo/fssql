# Parquet 定宽数值批量编码（2026-11-13）

## 实现范围

- 复用 `Vector.BorrowFixedWidth` 的只读批量视图，加速 flat INTEGER/UINTEGER、BIGINT/UBIGINT、FLOAT/DOUBLE、DATE、各 TIMESTAMP 单位及 DECIMAL(10–18, scale)。这些类型的内部小端字节与 Parquet PLAIN 物理表示一致。
- 无 NULL 的前缀使用批量复制；含 NULL 时先计算当前页有效值数量，按精确输出大小预留受管缓冲，再紧凑复制有效值。全 NULL 页不扩容，输出与输入保持独立所有权。
- 常量、嵌套 dictionary、窄整数以及 DECIMAL32/128 沿用现有标量路径，保留扩宽、溢出检查和字节序转换。

没有增加依赖或公共选项；默认 Snappy、2,048 行/行组、512 行/页、统计和字典策略保持原值。

## 正确性验证

以下检查通过：

```powershell
go test -count=1 ./internal/export ./internal/format/parquet ./pkg/gosql
go test -race -count=1 ./internal/export
go test -race -count=1 ./pkg/gosql -run '^Test(ParquetDecimalPrecisionBoundaries|ParquetDictionaryArtifacts|WideTextToParquetArtifacts|ParquetCopyCompression|ParquetExportStatisticsPruneReads|ParquetStatisticsTypedInterop)$'

$env:FSSQL_REQUIRE_DUCKDB = '1'
$env:FSSQL_PYTHON = Join-Path $PWD '.cache/agent-verification/venv/Scripts/python.exe'
go test -count=1 -v ./test/compat -run '^TestDuckDB(FileInterop|ParquetTypeInterop)$'
```

新增编码回归覆盖 12 种类型配置 × 无/混合/全 NULL × flat/constant/嵌套 dictionary，逐字节检查 6 个页范围，包括跨 validity word、非零起始行和空范围。测试保留 unsigned、浮点 NaN payload/无穷/负零等位模式，并验证带前缀的缓冲扩容。低预算回归检查全 NULL、稀疏页、内存错误、恢复预算后的重试和资源归零。

PyArrow 24.0.0 独立读取 CSV/NDJSON × 未加密/AES-128-GCM/AES-256-GCM 的六个 4,101 行 × 64 列产物，逐值比较 schema、NULL 和全部列，并确认 Snappy。两个未加密产物的 SHA256 均为 `3249c68729160059d1b232813296907947b6e19e7dfa5acc23fc359b26f7c517`，与上一轮留存产物一致。加密使用公开合成测试密钥和本地 KMS 回调，不包含网络 KMS。

DuckDB 2.0.0 双向文件及 Parquet 类型互操作回归通过。没有执行全仓库测试、完整公共 API race 或完整 SQL 差分。

## 性能测量

基线生产代码为 `32ea30e712c3388a8ced977508cc414d431925a2`。新增微基准和测试后、修改生产代码前编译并保留基线二进制；两侧使用相同微基准及现有完整转换基准。

Windows/amd64，Intel Core i7-10510U，Go 1.27.3，GOMAXPROCS=8。微基准每次编码 512 行，输入为 2,048 行向量，交替取四个页范围；预算 1 MiB，复用 64 KiB 受管输出缓冲，排除 fixture 和缓冲构造。

完整转换为 100,000 行，复用原六列及 64 列固定 CSV/NDJSON fixture，默认 Snappy，Workers=1、256 MiB 受管内存、本地文件。计时覆盖 CopyTo 的绑定、读取解码、编码、压缩及文件提交，排除 fixture 生成、DB 启动和产物删除。每次检查行数、列数（宽表）、产物字节数和受管内存归零。

每个版本预热 2 轮，正式测量 7 轮，每轮交替版本先后顺序；所有性能场景串行运行，与正确性测试、编译和 profile 分开。每个样本使用 `-test.benchtime=1s`，未清除 OS 缓存。没有删除慢样本。以下为中位数；MAD/中位数超过 10% 的场景计时无效，不发布加速结论。

| 编码微基准（512 行） | 优化前 | 优化后 | 前/后 MAD | 耗时下降 |
| --- | ---: | ---: | ---: | ---: |
| BIGINT，无 NULL | 32.968 µs | 0.140 µs | 0.77% / 3.86% | 99.6% |
| BIGINT，约 10% NULL | 30.155 µs | 3.641 µs | 3.13% / 2.39% | 87.9% |
| BIGINT，约 90% NULL | 5.449 µs | 1.384 µs | 5.27% / 3.76% | 74.6% |
| DOUBLE，无 NULL | 33.570 µs | 0.144 µs | 1.85% / 1.81% | 99.6% |

四个微基准两侧均为 0 B/op、0 allocs/op；这些结果仅代表 PLAIN 值编码，不包含 definition levels、统计、压缩或完整查询开销。

| 完整转换（100,000 行） | 优化前 | 优化后 | 前/后 MAD | 耗时结论 |
| --- | ---: | ---: | ---: | --- |
| 六列 CSV | 0.547s | 0.474s | 3.75% / 7.09% | 下降 13.4% |
| 六列 NDJSON | 0.637s | 0.569s | 12.25% / 5.80% | 无效，仅诊断 |
| 64 列 CSV | 3.349s | 2.914s | 7.71% / 2.14% | 下降 13.0% |
| 64 列 NDJSON | 6.553s | 6.250s | 2.47% / 2.12% | 下降 4.6% |

完整转换累计堆分配保持接近：六列 CSV 72.082→72.082 MB、六列 NDJSON 94.206→94.208 MB、64 列 CSV 606.767→606.760 MB、64 列 NDJSON 676.419→676.422 MB；MB 按 1,000,000 字节计算。差异很小，不作为分配优化收益。

全部保留轮次的六列产物均为 2,369,002 字节，64 列产物均为 21,805,600 字节。三个有效转换计时场景未出现超过项目 10% 门槛的自身耗时回归；六列 NDJSON 仍缺有效计时结论，不能宣称完整转换性能验收通过。

累计堆分配不是峰值内存。本轮未测 heap/RSS 峰值、CPU 秒、临时 I/O、冷缓存、多 worker、远端存储或 Linux；没有采集 RAM 容量和磁盘型号。本轮不是 DuckDB 性能对比或完整发布性能验收。

## 证据与复现

本机证据位于 `.cache/parquet-numeric-2026-11-13/`：

- 前后测试二进制、生产源码快照、源码及二进制 SHA256、`production.patch`。
- `environment.json`、`run.py`、`commands.json`、原始 `round-*.log`、`samples.json` 和 `summary.json`。
- race、DuckDB 互读日志，以及 `check.py`、六份产物和 `interop.json`。

先将 `TEMP/TMP/GOTMPDIR` 与 `GOCACHE` 指向工作区可写目录。完整配对流程使用留存的 `run.py`；单轮入口为：

```powershell
go test ./internal/export -run '^$' -bench '^BenchmarkParquetPlainNative$' -benchtime=1s -count=1
go test ./pkg/gosql -run '^$' -bench '^BenchmarkTextToParquet$/^100000$/^(csv|ndjson)$/^snappy$' -benchtime=1s -count=1
go test ./pkg/gosql -run '^$' -bench '^BenchmarkWideTextToParquet$/^(csv|ndjson)$/^plain$' -benchtime=1s -count=1
```
