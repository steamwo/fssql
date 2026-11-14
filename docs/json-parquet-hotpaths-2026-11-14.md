# JSON 投影与 Parquet 字符串编码优化（2026-11-14）

## 实现与范围

- JSON 在批次类型计划中预计算投影选择。根字段、STRUCT 子字段和 LIST 元素复用选择结果，不再逐值遍历投影路径；移除随解码器分配的路径数组。保留字段顺序、缺失值/NULL、未读字段结构校验和重复键检查。
- JSON/NDJSON runtime 直接读取绑定对象内部不可变 schema，避免每批调用公共 `Schema()` getter 复制字段。公开 getter 的复制行为保持原样；并行解码仍各自构造计划和工作区。
- Parquet 的 VARCHAR/BLOB PLAIN 编码借用现有 `BytesView`，先计算非 NULL 编码长度，再一次预留并直接写入整页。避免逐值 `Vector.Bytes` 临时分配和输出缓冲锁操作。支持 flat、constant、嵌套 dictionary、空值、空串、内联和外部字符串；保留尺寸溢出和受管预算检查。

未调整默认行组、页大小、Snappy、字典选择、加密、数值统计或存储读取路径。公共布局配置沿用[上一轮实现](public-api.md#structured-export)。

前置 CPU profile 帮助确定改动顺序：64 列 NDJSON 转换中，`ProjectedPathSet.Relevant` 累计占整个采样的 26.6%；原生 writer 采样中 `Buffer.Bytes` 累计占 17.0%，`collectParquetStatistics` 占 6.7%。采样包含 benchmark fixture/setup，调用链也有包含关系，不能直接换算为转换耗时收益或相加。分配采样和源码进一步定位了字符串 PLAIN 路径的逐值临时字节切片。

## 正确性与资源验证

以下检查通过：

```powershell
go test -count=1 ./internal/export ./internal/format/json ./pkg/gosql
go test -race -count=1 ./internal/format/json
go test -race -count=1 ./internal/export
go test -race -count=1 ./pkg/gosql -run '^Test(WideTextToParquetArtifacts|ParquetCopyCompression|EncryptedParquetCopyLayout|ParquetExportStatisticsPruneReads|ParquetStatisticsTypedInterop)$'
```

新回归覆盖带点的字面字段名、LIST→STRUCT→LIST 投影、未读字段的类型跳过、NULL 与嵌套元素偏移。字符串编码检查 VARCHAR/BLOB × flat/constant/constant NULL/嵌套 dictionary，包含空串、Unicode、零字节、非 UTF-8 字节、长值、分页范围和输入释放后的输出所有权。

精确预算用例验证：已有 3 字节前缀，加两条各 20 字节的非 NULL BLOB，完整编码恰好需要 51 字节受管页缓冲。额外占用 1 字节时预留失败且前缀不变；释放占用后可重试成功，输入和输出释放后受管内存归零。该数字是受管缓冲预算，不是进程 RSS。

PyArrow 24.0.0 独立校验 CSV/NDJSON × 未加密/AES-128-GCM/AES-256-GCM 六个产物：每份 4,101 行 × 64 列，跨 3 个行组，全部 schema、值、NULL 和 Snappy codec 一致。使用本地公开合成密钥，无网络 KMS。两份未加密文件仍为 901,690 字节，SHA256 均为 `3249c68729160059d1b232813296907947b6e19e7dfa5acc23fc359b26f7c517`，与前轮一致；加密文件各 940,875 字节。

未执行全仓库测试或完整公共 API race。

## 性能测量

基线为上一轮数值批量编码及公共布局配置完成后的工作区，基于提交 `32ea30e712c3388a8ced977508cc414d431925a2`，已在修改本轮生产代码前保留 `before-*.test.exe`。新增微基准在两侧使用同一实现。最终版本使用独立重编译的 `after-*.test.exe`；最初逐值 Append 的中间实现及其采样单独保存在 `v1/`，不与下表混合。

Windows/amd64、Intel Core i7-10510U、Go 1.27.3、GOMAXPROCS=8。每侧预热 2 轮、正式 7 轮，每轮交替引擎先后，场景串行运行；正式测量没有与测试、编译或 profile 并行。没有清除 OS 文件缓存。下表为正式样本中位数；MAD/中位数超过 10% 时标为无效，不发布加速比例。

JSON 微基准每次解码 1,024 行 BIGINT、显式投影全部字段，字段顺序与 schema 相反。字符串 PLAIN 微基准每次编码 512 行、约 10% NULL，循环使用 2,048 行输入，输出缓冲预先分配。两项均不计 fixture 构造。

原生 writer 为 100,000 行 × 64 列 BIGINT/DOUBLE/VARCHAR/BOOLEAN 混合输入，走本地 VFS、Snappy 与原子关闭；输入有独立受管预算，排除在 `writer-managed-peak-B` 外。完整转换为同规模的既有 CSV/NDJSON 合成输入，Workers=1、共享 256 MiB 预算、默认 2,048 行组/512 行页，覆盖解析绑定、读取解码、编码压缩和提交；fixture 生成、DB 启动与产物删除不计时。两类 fixture 不同，绝对耗时不能互相比。

| 场景 | 优化前 | 优化后 | 前/后 MAD | 耗时结论 |
| --- | ---: | ---: | ---: | --- |
| JSON，6 列显式投影 | 1.476ms | 1.048ms | 6.22% / 8.20% | 下降 29.0% |
| JSON，64 列显式投影 | 48.472ms | 11.812ms | 5.40% / 6.41% | 下降 75.6% |
| VARCHAR PLAIN，512 行 | 100.022µs | 16.746µs | 5.03% / 7.25% | 下降 83.3% |
| 原生 writer，2,048 行组 / 512 行页 | 1.895s | 1.860s | **11.05%** / 4.58% | 无效，不发布加速比例 |
| 原生 writer，8,192 行组 / 2,048 行页 | 1.312s | 1.333s | 7.72% / **16.08%** | 无效，不发布加速比例 |
| 64 列 CSV → Parquet | 4.095s | 4.315s | **12.29%** / 9.64% | 无效，不发布加速比例 |
| 64 列 NDJSON → Parquet | 8.197s | 3.966s | 4.34% / 9.29% | 下降 51.6% |

| 场景 | 累计堆分配前→后 | allocs/op 前→后 |
| --- | ---: | ---: |
| JSON，6 列 | 55,165→52,940 B | 45→45 |
| JSON，64 列 | 565,972→563,741 B | 398→398 |
| VARCHAR PLAIN，512 行 | 14,744→0 B | 460→0 |
| 原生 writer，2,048 / 512 | 417.83→394.79 MB | 904,897→184,898（下降 79.6%） |
| 原生 writer，8,192 / 2,048 | 257.45→234.41 MB | 769,772→49,771（下降 93.5%） |
| 64 列 CSV → Parquet | 606.77→583.72 MB | 942,179→222,174（下降 76.4%） |
| 64 列 NDJSON → Parquet | 676.42→652.71 MB | 1,132,053→411,942（下降 63.6%） |

两种原生布局的 writer 受管峰值在所有采样中均与基线相同：2,048 / 512 为 **3,806,530 字节**，8,192 / 2,048 为 **8,025,346 字节**。原生文件分别仍为 21,200,190 / 19,628,552 字节；两种完整文本转换均仍为 21,805,600 字节。

四个有效耗时场景均无耗时回归。CSV 和两个原生 writer 场景尚无有效耗时结论，不能把累计分配减少直接当作它们的加速证明，也不能标为全部场景性能验收通过。

以上不是 DuckDB 对比、冷缓存、百万行、Linux、远端或多 worker 性能结论。B/op 是累计 Go 堆分配，不是 heap/RSS 峰值；未测进程峰值、CPU 秒或临时 I/O。正式计时产物检查行数、文件字节数和受管内存归零，独立逐值互读使用上述 4,101 行样本。

## 仍待处理

- scanner 级类型计划/字段索引缓存尚未引入。本轮先消除已定位的逐值投影匹配；没有增加常驻解码器池或跨批 LIST 计数状态。
- 数值 min/max、NULL 定义级别与值编码的扫描尚未合并。需要针对变化后的热点评估，保留 NaN、正负零、DECIMAL 及稀疏 NULL 的预算语义。
- JoinOrder 尚未接入实际重写。`gosql.Open` 的 engine 配置没有注入 `Statistics`，现有 `JoinOrder` 只有测试调用；需要先接入绑定快照对应的真实统计，再完成连接谓词和输出列序的重映射，并保留外连接/相关输入等边界。仅调用当前候选排序不能视为已完成生产优化。

## 证据与复现

本机证据保存在 `.cache/remaining-opt-2026-11-14/`：

- `run.py`、`commands.json`、`round-*.log`、`samples.json`、`summary.json`：最终版本完整配对样本与统计。
- `environment.json`、`hashes.json`、`source-hashes.json`、`source/`、`tracked-production.patch`：环境、二进制和最终源码。补丁相对 Git HEAD，包含前轮尚未提交的数值派发；新增源文件另存于快照。
- `record.py`、`verification.json`：样本数量、二进制身份、输出字节数、受管峰值与零分配检查。
- `before-json.cpu.pprof`、`before-json.allocs.pprof`、`before-writer.cpu.pprof`：单独采集的诊断 profile，不计入正式样本。
- `check.py`、`interop-final/`、`interop.json`：最终六份独立互读产物与验证。
- `v1/`、`parquet_plain_bytes-v1.go`、`interop/`：中间实现证据，仅作诊断。

将 `GOCACHE`、`TEMP/TMP/GOTMPDIR` 指向工作区 `.cache`，并设置 `GOMAXPROCS=8` 后，配对重测可运行 `run.py`；单轮入口为：

```powershell
go test ./internal/format/json -run '^$' -bench '^BenchmarkWideProjectedDecodeRecords$' -benchtime=1s
go test ./internal/export -run '^$' -bench '^BenchmarkParquetPlainBytes$' -benchtime=1s
go test ./internal/export -run '^$' -bench '^BenchmarkParquetLayout$/^64$/^(r2048_p512|r8192_p2048)$' -benchtime=1s
go test ./pkg/gosql -run '^$' -bench '^BenchmarkWideTextToParquet$/^(csv|ndjson)$/^plain$' -benchtime=1s
```
