# Parquet 导出统计与读取裁剪（2026-11-13）

## 实现

导出的 Parquet 现在同时写入列块和数据页的统计信息：

- 支持的标量类型写入 `null_count`；有可用边界时写入现代 `min_value/max_value`，文件 footer 同时声明每个叶列的 `TYPE_ORDER`。普通和加密 footer 使用同一排序声明。
- 整数按有符号/无符号类型比较；DECIMAL 按精确系数比较，并按实际 Parquet 物理类型编码边界。字符串和 BLOB 使用字节顺序。
- 浮点写入 `nan_count`，含 NaN 的页或列块省略 min/max；零边界按 TYPE_ORDER 扩展为最小值 -0、最大值 +0。全 NULL 数据不写边界。
- 任一字符串超过 1,024 字节时，该统计范围省略边界，继续写 NULL 计数。不会截断值或拼出虚构边界。持久化列统计在复制边界前预留 metadata 预算，随 writer 关闭或失败清理释放。
- 每页只扫描一次计算统计，列统计由各页边界和计数合并，不再额外扫描整列。DECIMAL32/128 合并时使用 PLAIN 表示对应的精确比较规则。
- 读取端保留零长度二进制字段的存在性，并区分“空字符串边界”和“缺少边界”。

规范依据为 Apache 官方 [parquet.thrift 中的 Statistics、ColumnOrder 和 FileMetaData 定义](https://github.com/apache/parquet-format/blob/master/src/main/thrift/parquet.thrift)。保留现有 PLAIN 编码、压缩默认值、行组/页大小和公开 API；没有扩展读取端原本不支持的谓词类型。

## 正确性和互操作

以下检查通过：

```powershell
go test -count=1 ./internal/export ./internal/format/parquet ./pkg/gosql
go test -race -count=1 ./internal/export ./internal/format/parquet
go test -race -count=1 ./pkg/gosql -run '^Test(ParquetExportStatisticsPruneReads|ParquetStatisticsTypedInterop|WideTextToParquetArtifacts|ParquetCopyCompression)$'
```

新增回归覆盖精确整数/宽 DECIMAL 边界、unsigned、BLOB、空字符串、NaN、无穷、正负零、全 NULL、长值省略、跨页/行组所有权、统计预留及 footer 增长失败时的回收，以及普通/加密 metadata 编解码。

真实扫描回归从 8,192 个有序 BIGINT 中筛选末尾 256 行，逐值检查全部结果，并断言跳过 3 个行组、3 个页，只解码 1 个数据页。公共 API 另验证含 NaN 的比较不会因统计被错误裁剪，以及空字符串过滤结果。

PyArrow 24.0.0 独立验证通过：

- CSV/NDJSON × 未加密/AES-128-GCM/AES-256-GCM 六个 4,101 行 × 64 列产物，逐值比较 schema、NULL 和全部列，并检查 Snappy、解密和全部 **1,152 个列块**的 NULL 计数与 min/max。
- 另一个 4,101 行 × 5 列产物，检查 DECIMAL(38,4)、最大 UBIGINT、NaN、空/NULL/Unicode 字符串和超上限长字符串的值、类型及边界省略行为。

使用固定参考环境的 DuckDB 文件互操作与 Parquet 类型互操作回归也通过：

```powershell
$env:FSSQL_REQUIRE_DUCKDB = '1'
$env:FSSQL_PYTHON = Join-Path $PWD '.cache/agent-verification/venv/Scripts/python.exe'
go test -count=1 -v ./test/compat -run '^TestDuckDB(FileInterop|ParquetTypeInterop)$'
```

没有执行全仓库测试、完整公共 API race 或完整 SQL 差分。加密互读使用公开合成测试密钥和本地 KMS 回调，不是业务 KMS 验收。

## 前后测量

本轮基线包含[上一轮批量写入与 JSON 优化](text-conversion-optimization-2026-11-13.md)，生产代码为 `acb7a9f` 加上一轮保留的补丁。加入统计前已编译并保留基线二进制；两侧使用相同基准函数。

Windows/amd64、Intel Core i7-10510U、Go 1.27.3、GOMAXPROCS=8、Workers=1。本地文件，未清除 OS 文件缓存；WMI 拒绝访问，RAM 容量和磁盘型号未获取。各预热 2 轮、正式测量 7 轮，每轮交替两个版本的先后顺序，所有场景串行运行，不与测试或 profile 并行。每个样本使用 `-test.benchtime=1s`。

裁剪基准对每个版本自己导出的 8,192 行文件执行相同过滤，逐值消费全部 256 行，计时覆盖 scanner 创建、读取和关闭；排除文件生成、footer discovery、SQL 绑定与完整公共 API 开销，受管预算为 64 MiB。读取量是 VFS 范围返回的累计字节，包含重叠范围，不是冷磁盘物理 I/O。

写入基准复用现有六列/64 列、100,000 行 CSV/NDJSON fixture 和默认 Snappy，预算 256 MiB，计时范围与上一轮一致；fixture 生成、DB 启动和产物删除不计时。

以下为修正后实现的完整配对结果。MAD/中位数超过 10% 的场景，其计时无效，不能用来发布加速或无回归结论；MB 按 1,000,000 字节计算。

| 场景 | 优化前 | 优化后 | 前/后 MAD | 耗时结论 |
| --- | ---: | ---: | ---: | --- |
| 8,192 行过滤扫描，返回 256 行 | 23.924ms | 4.734ms | 4.27% / 7.20% | 下降 80.2% |
| 六列 CSV 转 Parquet | 1.014s | 0.812s | 26.29% / 6.14% | 无效，仅诊断 |
| 六列 NDJSON 转 Parquet | 1.148s | 1.041s | 9.71% / 9.50% | 下降 9.3% |
| 64 列 CSV 转 Parquet | 6.152s | 6.248s | 12.65% / 5.65% | 无效，仅诊断 |
| 64 列 NDJSON 转 Parquet | 12.975s | 12.869s | 6.33% / 7.02% | 下降 0.8%，无明显变化 |

过滤扫描每次 VFS 读取量从 **90,736 降至 4,615 字节（减少 94.9%）**；行组/页跳过数从 0/0 变为 3/3。累计堆分配从 582,423 降至 49,306 B/op，分配次数从 983 降至 255。

写入会增加元数据和少量累计分配：

| 转换场景 | 累计堆分配前→后 | 增幅 | 分配次数前→后 |
| --- | ---: | ---: | ---: |
| 六列 CSV | 74.50→74.93 MB | 0.59% | 309,101→312,928 |
| 六列 NDJSON | 96.62→97.06 MB | 0.45% | 358,890→362,705 |
| 64 列 CSV | 630.31→634.68 MB | 0.69% | 1,614,350→1,651,981 |
| 64 列 NDJSON | 699.97→704.35 MB | 0.62% | 1,804,268→1,841,900 |

产物大小在两种文本来源间一致：六列 **2,400,559→2,442,770 字节（+1.76%）**，64 列 **22,101,170→22,512,203 字节（+1.86%）**。增加部分包含页头和 footer 统计。

第一版实现分别计算整列与页统计，测得两个宽表写入场景约 10%–11% 的有效回归，未采用该版本作为最终实现。改为合并页统计后，重新编译并对全部五个场景完整执行 2 次预热、7 次配对测量，没有挑选旧样本或删除慢样本。旧版样本保留在证据根目录，最终结果使用 `merge/` 下的新样本。

最终三个有效耗时场景均未超过项目 10% 自身耗时回归门槛；**两个 CSV 写入场景仍缺有效耗时结论，不能宣称完整写入性能验收通过**。

累计 Go 堆分配不是峰值内存。本轮未测 heap/RSS 峰值、CPU 秒、临时 I/O、冷缓存、远端、多 worker 或 Linux；本轮不构成完整发布性能验收，也不是 DuckDB 性能对比。

## 证据与复现

本机证据位于 `.cache/parquet-stats-2026-11-13/`。最终实现与测量使用其 **`merge/` 子目录**，包含前后二进制、`environment.json`、`hashes.json`、生产补丁、`run.py`、精确 `commands.json`、原始 `round-*.log`、`samples.json`、`summary.json`，以及普通/race/互读/差分日志和 JSON；初版实现的测量和基线来源补丁保留在根目录。

先将 `TEMP/TMP/GOTMPDIR/GOCACHE` 指向工作区可写目录。完整配对测量运行上述 `run.py`，单轮入口为：

```powershell
go test ./pkg/gosql -run '^$' -bench '^BenchmarkParquetExportStatisticsPruning$' -benchtime=1s -count=1
go test ./pkg/gosql -run '^$' -bench '^BenchmarkTextToParquet$/^100000$/^(csv|ndjson)$/^snappy$' -benchtime=1s -count=1
go test ./pkg/gosql -run '^$' -bench '^BenchmarkWideTextToParquet$/^(csv|ndjson)$/^plain$' -benchtime=1s -count=1
```
