# Parquet 字符串字典编码（2026-11-13）

## 实现与边界

在上一轮[统计信息与裁剪](parquet-statistics-2026-11-13.md)的基础上，writer 为适合的 VARCHAR/BLOB 列块自动使用字典编码：

- 每个列块独立建字典，最多 256 项、32 KiB PLAIN 字典数据。小于 32 行、重复率不足或预估原始编码节省不足约 25% 的列块保持 PLAIN。
- 字典页在数据页之前写入，字典页使用 PLAIN，数据页使用现代 `RLE_DICTIONARY`。索引使用 RLE/bit-packed hybrid；正确处理零位宽、组尾补齐与 NULL，NULL 不占字典条目。
- 字典键借用不可变行组数据，哈希槽等元数据先预留预算，行索引通过 BufferManager 分配。高基数、大字典和可选分配预算不足时释放候选，不消耗或重排输入。
- 提交前准备字典页压缩、全部加密帧并检查数据页工作区余量。准备阶段的内存上限错误回退 PLAIN，恢复原页缓冲预算；目标写入错误继续传播并由原导出路径中止，不重放已写字节。
- Snappy/identity 与未加密/AES-128/AES-256 路径共用策略；字典页及页头分别使用模块 3/5 的 AAD，不带数据页序号。数据页序号仍从 0 开始。
- 字典按首遇顺序编号；随机哈希种子仅影响槽位，不影响未加密产物字节。min/max、NULL 计数和列排序声明继续来自原始值。

规范依据为 Apache 官方[字典与 RLE 编码](https://github.com/apache/parquet-format/blob/master/Encodings.md)及[模块加密 AAD](https://github.com/apache/parquet-format/blob/master/Encryption.md)。没有新增依赖、公共选项或数据类型；数值列保持原编码。

## 验证

以下检查通过：

```powershell
go test -count=1 ./internal/export ./internal/format/parquet ./pkg/gosql
go test -race -count=1 ./internal/export ./internal/format/parquet
go test -race -count=1 ./pkg/gosql -run '^Test(ParquetDictionaryArtifacts|WideTextToParquetArtifacts|ParquetCopyCompression|ParquetExportStatisticsPruneReads|ParquetStatisticsTypedInterop)$'
```

新增原生回归覆盖 1/2/256 项字典、257 项回退、UTF-8/BLOB、NULL、全 NULL 页、跨页索引补齐、跨行组重置、Snappy/identity、字典字节上限、取消、预算回退及目标页头/页体写入失败清理。测试逐值验证独立输入释放后的结果，并检查受管内存归零。

独立 PyArrow 24.0.0 校验了九个产物：六个 CSV/NDJSON 宽表产物，以及三个空字符串/全 NULL/整页 NULL/高基数边界产物，均覆盖未加密和两种 AES。全部值、类型、NULL、统计信息、字典/数据页偏移与编码选择一致；共验证 **108 个字典列块**，同时确认高基数列、全 NULL 列和末尾小行组保留 PLAIN。未加密 CSV/NDJSON 宽表产物逐字节一致。

另有一个包含精确 DECIMAL、最大 UBIGINT、NaN、空字符串和长字符串的产物通过 PyArrow 逐值及统计校验。DuckDB 2.0.0 逐值读取了三份未加密新产物；原有 `TestDuckDBFileInterop`、`TestDuckDBParquetTypeInterop` 也通过。DuckDB Python 进程退出时打印的 nanobind 引用计数警告保留在日志中。

加密验证使用公开合成测试密钥和本地 KMS 回调；本轮没有业务 KMS、Linux、全仓库测试或完整 SQL 差分验收。

## 性能测量

基线是上一轮最终统计实现的留存测试二进制，SHA256 为 `42ac7069f4881927c80fd352ec53b3c4ed0c811001e539f8ab3542302a5ead54`；本轮在相同代码基础上增加字典编码。两侧使用相同的现有六列/64 列文本转换基准和固定输入生成器，均为 100,000 行，默认 Snappy。

Windows/amd64、Intel Core i7-10510U、Go 1.27.3、GOMAXPROCS=8、Workers=1、256 MiB 受管内存，本地文件；未清除 OS 文件缓存。WMI 拒绝访问，RAM 容量与磁盘型号未获取。每个版本预热 2 次，正式测量 7 次，每轮交替版本先后顺序，场景串行，不与测试或 profile 并行。每个样本使用 `-test.benchtime=1s`。

计时覆盖完整 CopyTo 的解析绑定、读取解码、编码压缩及提交；排除 fixture 生成、DB 启动与产物删除。六列表只有 region 列适合字典；64 列表中八个低基数字符串列适合，另外八个高基数字符串列回退。

以下为中位数；MAD/中位数超过 10% 的场景计时无效。MB 按 1,000,000 字节计算。

| 场景（100,000 行） | 优化前 | 优化后 | 前/后 MAD | 耗时结论 |
| --- | ---: | ---: | ---: | --- |
| 六列 CSV | 0.698s | 0.718s | 3.70% / 5.35% | 增加 2.9% |
| 六列 NDJSON | 0.978s | 0.891s | 7.77% / 4.43% | 下降 8.9% |
| 64 列 CSV | 5.529s | 4.883s | 12.07% / 1.56% | 无效，仅诊断 |
| 64 列 NDJSON | 10.471s | 10.251s | 0.80% / 0.74% | 下降 2.1% |

三个有效耗时场景均在项目 10% 自身回归门槛内；没有删除慢样本，64 列 CSV 仍缺有效耗时结论，不能据此宣称所有场景提速或完整性能验收通过。

| 场景 | 累计堆分配前→后 | 分配量下降 | 分配次数前→后 | 次数下降 |
| --- | ---: | ---: | ---: | ---: |
| 六列 CSV | 74.93→72.08 MB | 3.80% | 312,923→214,352 | 31.5% |
| 六列 NDJSON | 97.06→94.21 MB | 2.93% | 362,701→264,111 | 27.2% |
| 64 列 CSV | 634.68→606.76 MB | 4.40% | 1,651,971→942,141 | 43.0% |
| 64 列 NDJSON | 704.34→676.42 MB | 3.96% | 1,841,878→1,132,039 | 38.5% |

两种文本来源的文件大小一致，且全部保留轮次相同：

- 六列：**2,442,770→2,369,002 字节，减少 3.02%**。
- 64 列：**22,512,203→21,805,600 字节，减少 3.14%**。

这些是低/高基数混合表的整体结果，不是全低基数数据的最佳值。字典路径按列批量借用字节并编码索引，减少了逐值字符串包装和页压缩缓冲分配；高基数列仍承担有限的候选检查开销，继续使用 PLAIN。

累计 Go 堆分配不是峰值内存；本轮未采集 heap/RSS 峰值、CPU 秒或临时 I/O。不将这些结果外推到远端、冷缓存、多 worker、百万行或网络 KMS，也不是 DuckDB 性能对比。

## 证据与复现

最终证据位于 `.cache/parquet-dictionary-2026-11-13/final/`：前后二进制、`environment.json`、`hashes.json`、生产补丁、`run.py`、精确命令、原始轮次日志、`samples.json`、`summary.json`，以及普通/race/互读/差分证据。初步检查日志保留在其父目录。

设置工作区可写的 `TEMP/TMP/GOTMPDIR/GOCACHE` 后，完整配对流程运行上述 `run.py`；单轮入口为：

```powershell
go test ./pkg/gosql -run '^$' -bench '^BenchmarkTextToParquet$/^100000$/^(csv|ndjson)$/^snappy$' -benchtime=1s -count=1
go test ./pkg/gosql -run '^$' -bench '^BenchmarkWideTextToParquet$/^(csv|ndjson)$/^plain$' -benchtime=1s -count=1
```
