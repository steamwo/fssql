# 64 列 CSV / NDJSON 转加密 Parquet（2026-11-12）

本轮补测宽表与加密组合，没有修改生产代码。基准为 [BenchmarkWideTextToParquet](../pkg/gosql/wide_text_conversion_benchmark_test.go)，使用 **64 列 × 100,000 行**，全部采用默认 Snappy，比较未加密、AES-128-GCM 和 AES-256-GCM。加密使用现有 Parquet Modular Encryption 路径，覆盖数据页、页头和页脚；密钥由本地回调提供，不包含网络 KMS 延迟。

环境沿用六列表测量：Windows 10.0.26320 / amd64、Intel Core i7-10510U、Go 1.27.3、8 个逻辑 CPU、GOMAXPROCS=8、Workers=1、受管内存限额 256 MiB，本地文件、热缓存流程，未重置 OS 缓存。源码为 `d431702a11bdfaefd2a839a0ec3f6189c49713e5` 加当前工作区改动。RAM 容量和磁盘型号未采集。

CSV / NDJSON 包含相同的 640 万个单元格：16 列 BIGINT、16 列 DOUBLE、16 列 VARCHAR、16 列 BOOLEAN。c00 保持非 NULL，其余各列 10% NULL；DOUBLE 为精确四分之一数值，字符串混合高基数账号与低基数中文、逗号和引号。固定生成器无随机种子。CSV 显式指定表头和类型，NDJSON 使用类型推断。输入分别为 56,627,747 / 97,907,491 字节。这与前轮六列表的值分布不同，不能仅按列数或单元格数推算性能倍数。

计时包含 `CopyTo` 的解析/绑定、读取解码、Parquet 编码、Snappy、加密及原子提交；不包含数据生成、数据库启动、文件删除和独立校验。writer 为默认 2,048 行/行组、512 行/页、PLAIN 编码。各场景串行、固定顺序，每个样本至少累计计时 1 秒，2 轮预热后各保留 7 个完整样本。一次进程 CPU 诊断采样干扰了 `measured-4.log` 所在整轮，因此保留原始日志、排除该整轮并以完整的 `measured-9.log` 重测替代；其他慢样本全部保留。最终统计轮次为 2、3、5、6、7、8、9，详细原因保存在 `exclusions.json`。

**本轮五个场景的 MAD/中位数超过项目 10% 门槛，测量无效；这些中位数只作本机诊断参考。仅 NDJSON + AES-256-GCM 的单场景波动满足门槛，不能据此认定整个宽表加密性能验收通过。** MB 按 1,000,000 字节计算，吞吐按物理输入大小计算。

| 来源 | 加密 | 中位耗时 | MAD / 中位数 | 行/秒 | 输入 MB/s | 输出 MB | 测量状态 |
| --- | --- | ---: | ---: | ---: | ---: | ---: | --- |
| CSV | 无 | 5.405s | 13.94% | 18,502 | 10.48 | 22.10 | 无效，仅参考 |
| CSV | AES-128-GCM | 5.855s | 20.36% | 17,081 | 9.67 | 22.92 | 无效，仅参考 |
| CSV | AES-256-GCM | 6.992s | 19.34% | 14,301 | 8.10 | 22.92 | 无效，仅参考 |
| NDJSON | 无 | 12.827s | 12.28% | 7,796 | 7.63 | 22.10 | 无效，仅参考 |
| NDJSON | AES-128-GCM | 12.952s | 19.34% | 7,721 | 7.56 | 22.92 | 无效，仅参考 |
| NDJSON | AES-256-GCM | 12.532s | 7.76% | 7,980 | 7.81 | 22.92 | 单场景有效 |

目前不能发布稳定的加密耗时增幅，也不能据此对 AES-128 / AES-256 的速度排名。文件大小不受该时间波动影响：未加密 Snappy 为 **22,101,170 字节**，两种 AES 均为 **22,920,162 字节**，增加 **818,992 字节（3.71%）**。两种来源的产物大小一致。

每次转换的累计 Go 堆分配：CSV 未加密/加密约 1.294 / 1.355 GB，NDJSON 约 1.363 / 1.424 GB，AES 两种密钥长度接近。这是累计分配，**不是峰值内存**。各轮导出行数、64 列 schema 数量、输出字节数及受管内存归零断言全部通过；未采集 Go heap/RSS 峰值、CPU 秒和临时 I/O。

正确性使用独立的 4,101 行 × 64 列样本，跨 3 个行组及多个页面，覆盖 CSV/NDJSON × 未加密/AES-128/AES-256 六个产物。`TestWideTextToParquetArtifacts` 通过；随后 PyArrow 24.0.0 使用本地测试 KMS 回调解析公开合成测试密钥，成功读取/解密全部六个文件，并逐值比较全部列、NULL 和类型，验证全部行组使用 Snappy。证据为 `.cache/parquet-conversion-wide/interop.json`。这不包含业务 KMS 或调用方自编译加密扩展验收；10 万行正式计时产物检查行数、列数、字节数和受管内存释放，没有逐个做全量独立解密。

本机证据位于 `.cache/parquet-conversion-wide/`：`environment.json` 保存配置及二进制/生成器 SHA256，`warmup-*.log` / `measured-*.log` 保存原始轮次，`exclusions.json` 记录受干扰整轮及替代轮次，`summary.json` 保存保留样本与统计，`tests.log` / `interop.log` 保存正确性执行结果。未与其他测试或 profile 并行执行正式转换。

复现单轮（先将 TEMP/TMP/GOTMPDIR 指向可写目录）：

```powershell
go test ./pkg/gosql -run '^$' -bench '^BenchmarkWideTextToParquet$' -benchtime=1s -count=1
go test ./pkg/gosql -run '^TestWideTextToParquetArtifacts$' -count=1
```

完整正常测量流程见 `.cache/parquet-conversion-wide/run.ps1`：编译后执行 9 轮，前 2 轮预热。本次另加第 9 号轮替换受诊断采样干扰的第 4 号轮。未测量百万行宽表、冷缓存、远端存储、多 worker、网络 KMS 或 DuckDB 性能对比；当前数据不作为容量或延迟承诺。
