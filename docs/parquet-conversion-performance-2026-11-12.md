# CSV / NDJSON 转 Parquet 性能（2026-11-12）

Parquet 默认页压缩已改为 Snappy，覆盖 SQL COPY、Go `CopyTo`、writer factory 和直接 writer；显式 `none` / `identity` 继续生成未压缩页，文本格式的默认压缩仍为 `identity`。SQL 在最终 FORMAT 确定后应用默认值，包括 `ENCRYPTED_PARQUET`。

测量基于 `d431702a11bdfaefd2a839a0ec3f6189c49713e5` 加当前工作区改动，表示当前端到端性能，不是本次补丁的前后回归对比。环境为 Windows 10.0.26320 / amd64、Intel Core i7-10510U、Go 1.27.3、8 个逻辑 CPU、`GOMAXPROCS=8`、`Workers=1`、受管内存上限 256 MiB。本地文件输入输出，无加密；按热缓存流程测量，未清空 OS 缓存。磁盘型号和 RAM 容量未采集。

基准：[BenchmarkTextToParquet](../pkg/gosql/text_conversion_benchmark_test.go)。两种输入包含相同的六列：BIGINT id、高基数 account、32 种 region、精确四分之一数值 DOUBLE、BOOLEAN active、含中文/逗号/引号且 10% NULL 的 note。生成器固定，无随机种子。CSV 显式指定 HEADER 和列类型，NDJSON 使用类型推断。10 万行输入分别为 6,217,927 / 11,657,890 字节；100 万行分别为 63,178,937 / 117,578,900 字节。

计时包含 `CopyTo` 的解析/绑定、文本读取解码、向量处理、Parquet 编码/压缩和原子文件提交；不包含 fixture 生成、数据库启动、文件删除及校验。默认 writer 使用 2,048 行/行组、512 行/页和 PLAIN 编码。两个预热轮次后保留七轮样本，每个样本由 Go benchmark 累计计时至少 1 秒；轮次串行，场景顺序固定。下表为中位数，MB 均按 1,000,000 字节计算，吞吐按物理输入大小计算。

| 行数 | 来源 | 压缩 | 耗时 | MAD / 中位数 | 行/秒 | 输入 MB/s | 产物 MB |
| ---: | --- | --- | ---: | ---: | ---: | ---: | ---: |
| 100,000 | CSV | Snappy（默认） | 0.714s | **10.02%** | 140,148 | 8.71 | 2.40 |
| 100,000 | CSV | identity | 0.733s | 3.76% | 136,373 | 8.48 | 6.59 |
| 100,000 | NDJSON | Snappy（默认） | 0.881s | 3.22% | 113,506 | 13.23 | 2.40 |
| 100,000 | NDJSON | identity | 1.039s | **12.85%** | 96,252 | 11.22 | 6.59 |
| 1,000,000 | CSV | Snappy（默认） | 5.562s | 9.33% | 179,806 | 11.36 | 24.00 |
| 1,000,000 | CSV | identity | 5.826s | 3.58% | 171,657 | 10.85 | 65.87 |
| 1,000,000 | NDJSON | Snappy（默认） | 7.031s | 2.61% | 142,223 | 16.72 | 24.00 |
| 1,000,000 | NDJSON | identity | 8.278s | **13.49%** | 120,796 | 14.20 | 65.87 |

粗体场景超过项目 10% 波动门槛，测量无效，仅保留诊断值，不用来发布速度倍数。未删除慢样本。百万行默认 Snappy 的两个场景均在波动门槛内；在这组数据上，CSV 的行吞吐高于 NDJSON。不同来源的输入字节数不同，不能用 MB/s 排名代替行吞吐比较。

百万行 Snappy 产物固定为 23,996,755 字节，identity 为 65,872,837 字节，减少 63.6%。Snappy 没有表现出明显的端到端速度负担，但现有波动和固定测量顺序不足以证明稳定提速。百万行 CSV / NDJSON 的 Snappy 累计 Go 堆分配分别约 1.41 / 1.56 GB、602 / 611 万次；这是每次转换的累计分配量，**不是峰值内存**。每轮完成后受管内存均归零；Go heap/RSS 峰值、CPU 秒和临时 I/O 未测量。

验证：`go test ./internal/export ./internal/sql/binder ./internal/session ./pkg/gosql` 通过；随后新增的 CSV/NDJSON 公共入口用例通过 `go test ./pkg/gosql -run '^TestParquetCopyCompression$' -count=1`。回归覆盖默认/显式 Snappy、none/identity、SQL FORMAT 覆盖、实际页 codec、NULL/空串/Unicode 回读和内存释放。独立 PyArrow 校验以另行生成的 10 万行同内容 fixture 检查 CSV/NDJSON × Snappy/identity 四个文件的完整值、schema 和全部行组 codec；百万行 benchmark 每轮检查导出行数、字节数及受管内存归零。

本报告未测量 DuckDB 性能对比、冷缓存、远端存储或多 worker，不能将这些数值直接外推到上述场景，也不作为完整发布性能验收。64 列与加密的补充测量见 [宽表加密转换报告](wide-parquet-encryption-performance-2026-11-12.md)。

本机证据位于 `.cache/parquet-conversion/`：`environment.json` 保存版本和二进制/生成器 SHA256，`warmup-*.log` / `measured-*.log` 保存全部轮次，`summary.json` 保存原始样本和统计，`fixtures.json` / `interop.json` 保存独立格式校验及文件摘要。首次调试的 `smoke*.log` 不计入正式样本。

复现单轮（先将 TEMP/TMP/GOTMPDIR 指向可写目录）：

```powershell
go test ./pkg/gosql -run '^$' -bench '^BenchmarkTextToParquet$' -benchtime=1s -count=1
```

本轮完整顺序由 `.cache/parquet-conversion/run.ps1` 记录：先编译测试二进制，再逐轮启动二进制执行全部场景，共 9 轮，前 2 轮仅预热。没有与其他测试、profile 或 fixture 校验并行运行。
