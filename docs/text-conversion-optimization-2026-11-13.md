# 文本转 Parquet 与宽 JSON 解码优化（2026-11-13）

## 改动范围

- Parquet writer 复用现有 `vector.CopySelectionInto`，按列批量复制输入；保留 flat、constant、嵌套 dictionary、NULL、精确数值及字符串拥有权语义。
- 每列的多页编码复用原始大小的受管页缓冲。长值导致扩容后仍释放该缓冲，不让大页容量挤占后续页的预算；压缩缓冲和行组向量继续沿用原生命周期。
- JSON 解码为超过 8 列的根对象建立批内字段索引，先预留 metadata 预算，再创建索引。预算不足时回退线性查找；小 schema 和嵌套对象保留原路径。字段匹配仍区分大小写，转义后的重复键仍报错。

没有修改默认 Snappy、2,048 行/行组、512 行/页或公开 API。

## 正确性与资源验证

以下检查通过：

```powershell
go test -count=1 ./internal/export ./internal/format/json ./pkg/gosql
go test -race -count=1 ./internal/export ./internal/format/json
go test -race -count=1 ./pkg/gosql -run '^Test(WideTextToParquetArtifacts|ParquetCopyCompression)$'
```

新回归检查了输入释放后的独立所有权、flat/constant/嵌套 dictionary、NULL、宽数值、NaN/Infinity/负零、字符串及二进制值；JSON 覆盖宽 schema、字段乱序、未知字段、Unicode 转义、重复键、嵌套对象和低预算回退。失败和成功路径均检查受管内存释放。

独立 PyArrow 24.0.0 校验通过 CSV/NDJSON × 未加密/AES-128-GCM/AES-256-GCM 的六个产物。每个产物为 4,101 行 × 64 列，跨 3 个行组，逐值比较 schema、NULL 和全部列，并检查所有行组使用 Snappy。加密校验使用本地公开合成测试密钥，不含网络 KMS。未加密产物 SHA256 与前轮证据一致。

没有执行全仓库测试或完整公共 API race。

## 性能测量

生产代码基线为 `acb7a9f48f13131c04c2ba1731ad9cb16efead71`；修改生产代码前已编译并保留基线二进制。优化版为该提交加本次两处生产代码改动。新增 JSON 微基准在两侧使用相同实现。

Windows/amd64，Intel Core i7-10510U，Go 1.27.3，GOMAXPROCS=8；转换使用 Workers=1、256 MiB 受管内存、本地文件和默认 Snappy。六列和 64 列输入均为 100,000 行，使用现有固定生成器，详见前轮[六列报告](parquet-conversion-performance-2026-11-12.md)与[宽表报告](wide-parquet-encryption-performance-2026-11-12.md)。WMI 拒绝访问，未获取 RAM 容量和磁盘型号。

两个版本每轮交替先后顺序，各预热 2 轮、正式测量 7 轮；所有场景串行运行，不与测试或 profile 并行。fixture 生成、DB 启动和产物删除不计时。转换计时覆盖 CopyTo 的解析绑定、读取解码、编码压缩和文件提交。未清除 OS 文件缓存。

宽表每样本执行一次完整转换，均超过 1 秒。初轮短场景使用 300ms 微基准或单次转换，仅保留作诊断；JSON 与六列场景另按 `-test.benchtime=1s` 完整配对补测。没有删除慢样本，也没有把两次采样拼成同一场景的正式样本。

以下为中位数。MAD/中位数超过 10% 的场景，其耗时测量无效，不发布加速比例。MB 按 1,000,000 字节计算。

| 转换场景（100,000 行） | 优化前 | 优化后 | 前/后 MAD | 耗时结论 | 累计堆分配前→后 | 分配量下降 |
| --- | ---: | ---: | ---: | --- | ---: | ---: |
| 六列 CSV | 1.045s | 0.831s | 15.10% / 11.02% | 无效，仅诊断 | 141.74→74.50 MB | 47.4% |
| 六列 NDJSON | 1.260s | 1.127s | 9.97% / 8.70% | 下降 10.5% | 163.82→96.62 MB | 41.0% |
| 64 列 CSV | 7.215s | 5.774s | 11.28% / 4.94% | 无效，仅诊断 | 1,294.00→630.31 MB | 51.3% |
| 64 列 NDJSON | 15.309s | 11.715s | 5.30% / 9.81% | 下降 23.5% | 1,363.33→699.98 MB | 48.7% |

转换累计分配次数中位数分别为：六列 CSV 602,718→309,109，六列 NDJSON 652,214→358,877，64 列 CSV 3,090,647→1,614,360，64 列 NDJSON 3,279,565→1,804,266。产物大小保持一致：六列 2,400,559 字节，64 列 22,101,170 字节。

JSON 微基准每次解码 1,024 行，仅包含 BIGINT 字段，字段顺序与 schema 相反；fixture 构造不计时。它验证字段匹配路径，不代表混合类型的完整转换耗时。

| JSON 列数 | 优化前 | 优化后 | 前/后 MAD | 结论 | B/op 前→后 | allocs/op 前→后 |
| --- | ---: | ---: | ---: | --- | ---: | ---: |
| 6 | 1.530ms | 1.551ms | 5.07% / 8.20% | 增加 1.3%，无明显变化 | 55,169→55,167 | 45→45 |
| 64 | 37.965ms | 16.346ms | 3.76% / 3.86% | 下降 56.9% | 562,378→565,967 | 393→398 |

宽 JSON 的字段索引带来约 3.6 KB/批、5 次/批的额外 Go 堆分配；索引 metadata 在分配前预留预算，批结束时释放。四个有效耗时场景没有超过项目 10% 自身耗时回归门槛；两个 CSV 场景仍缺有效耗时结论，不能标为完整性能验收通过。

累计 Go 堆分配不是峰值内存；本轮未测 heap/RSS 峰值、CPU 秒或临时 I/O。每次转换检查行数、产物字节数和受管内存归零；正式计时产物未逐个交给外部读取器全量校验，独立逐值互读使用上述 4,101 行样本。本轮不代表 DuckDB 对比、百万行、Linux、远端存储、多 worker、冷缓存或网络 KMS 性能。

## 证据与复现

本机证据位于 `.cache/conversion-opt-2026-11-13/`：

- `environment.json`、`hashes.json`、`production.patch`：环境、前后二进制和源码 SHA256、生产代码补丁。
- `run.py`、`run_short.py`、`commands.json`、`short-commands.json`：执行顺序和精确命令。
- `round-*.log`、`short-round-*.log`、`samples.json`、`short-samples.json`、`summary.json`、`short-summary.json`、`final-summary.json`：完整样本和统计。正式结果使用后一次短场景样本及初轮宽表样本。
- `tests.log`、`race.log`、`api-race.log`、`interop.json`、`check.py`：正确性和独立互读证据。

先将 `TEMP/TMP/GOTMPDIR` 与 `GOCACHE` 指向工作区可写目录。完整成对流程运行上述脚本；单轮场景入口为：

```powershell
go test ./internal/format/json -run '^$' -bench '^BenchmarkWideDecodeRecords$' -benchtime=1s -count=1
go test ./pkg/gosql -run '^$' -bench '^BenchmarkTextToParquet$/^100000$/^(csv|ndjson)$/^snappy$' -benchtime=1s -count=1
go test ./pkg/gosql -run '^$' -bench '^BenchmarkWideTextToParquet$/^(csv|ndjson)$/^plain$' -benchtime=1x -count=1
```
