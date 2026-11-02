# 百万行真实 MinIO 测试与 Benchmark 报告

**结论：大 Parquet 的主要 SQL 结果、spill 与资源清理通过；大 CSV/NDJSON 的默认内存路径失败，查询下推和 Top-N 性能存在明显缺口，不能判定大文件能力完整通过。**

## 环境与数据

- Windows / amd64，Go 1.24.2，Intel i5-12400F，12 个逻辑核。
- Windows 本机运行引擎，访问既有 WSL Docker MinIO；PG 仅只读连接验证和资源旁观，未宣称引擎支持 PG 数据源。
- DuckDB 2.0.0 是结果参考，PyArrow 24.0.0 生成无压缩、无 dictionary 的 Parquet。每个 row group 最多 32768 行。
- 同一逻辑事实表 1,000,000 行，6 列，包含 NULL、负数、1000 个分组、乱序排序键和 256 B payload。
- 原始对象合计 951,728,980 B（907.64 MiB）；Parquet 299,960,006 B，CSV 283,038,719 B，NDJSON 344,274,015 B。
- 本机 warm baseline，未清空共享服务缓存；未并行执行其他测试来干扰 benchmark。共享主机的外部负载未隔离。

## 测试覆盖与结果

共 52 个独立场景：正常功能通过 34，预期错误/清理验证 6，显式未支持 7，真实失败 5。

预期错误包括取消、消费端停止、内存/临时空间配额，以及高内存预算下读取首块后主动停止的两个诊断场景。首块诊断不代表完整文本扫描通过。

汇总按同名场景的最后复核结果统计；原始日志均保留。CAST 等边界探测保留相同 SQL 和实际错误，不计为功能支持。初轮 USING SAMPLE 的极短运行触发统计器除零，修复统计器后单独复核；没有修改引擎来改变该结果。

| 场景 | 结果 | 返回行数 | 耗时 s | CPU % | RSS MiB | 受管峰值 MiB | 临时文件峰值 MiB | 实读 MiB | GC |
| --- | --- | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: |
| scan_narrow | pass | 1000000 | 6.171 | 66.3 | 26.7 | 2.9 | 0.0 | 286.5 | 511 |
| scan_wide | pass | 1000000 | 6.472 | 70.7 | 27.7 | 3.0 | 0.0 | 286.5 | 723 |
| projection_limit | pass | 100 | 6.212 | 68.7 | 32.6 | 3.0 | 0.0 | 286.5 | 635 |
| filter_range | pass | 1000 | 6.137 | 58.3 | 31.1 | 2.9 | 0.0 | 286.5 | 492 |
| filter_empty | pass | 0 | 5.977 | 67.4 | 28.5 | 2.9 | 0.0 | 286.5 | 500 |
| expressions_nulls | pass | 10000 | 5.914 | 65.3 | 29.1 | 3.0 | 0.0 | 286.5 | 499 |
| global_aggregate | pass | 1 | 6.256 | 71.7 | 29.0 | 2.9 | 0.0 | 286.5 | 500 |
| group_having | pass | 1000 | 6.100 | 69.9 | 29.7 | 3.1 | 0.0 | 286.5 | 480 |
| distinct | pass | 1000 | 6.227 | 70.0 | 31.6 | 3.0 | 0.0 | 286.5 | 483 |
| sort_spill | pass | 1000000 | 11.994 | 84.0 | 39.5 | 7.6 | 2.7 | 286.5 | 355 |
| sort_in_memory | pass | 1000000 | 13.214 | 80.9 | 71.5 | 24.9 | 0.0 | 286.5 | 127 |
| topn | pass | 100 | 11.982 | 82.8 | 41.0 | 7.6 | 2.7 | 286.5 | 359 |
| offset | pass | 100 | 11.920 | 82.3 | 40.7 | 7.6 | 2.7 | 286.5 | 363 |
| cte | pass | 1000 | 5.953 | 61.7 | 30.5 | 3.1 | 0.0 | 286.5 | 489 |
| scalar_aggregate_unsupported | unsupported | 0 | 0.010 | — | 21.1 | 0.0 | 0.0 | 0.0 | 0 |
| scalar_subquery | pass | 999 | 6.124 | 75.5 | 29.7 | 2.9 | 0.0 | 286.5 | 495 |
| inner_join | pass | 1 | 7.401 | 79.0 | 31.4 | 4.1 | 0.0 | 286.5 | 837 |
| left_join_nulls | pass | 1 | 7.031 | 80.7 | 32.5 | 4.7 | 0.0 | 286.5 | 830 |
| semi_join | pass | 1 | 6.925 | 78.3 | 33.1 | 3.6 | 0.0 | 286.5 | 787 |
| anti_join | pass | 1 | 7.120 | 65.2 | 36.0 | 4.0 | 0.0 | 286.5 | 829 |
| aggregate_spill | pass | 100000 | 6.750 | 66.4 | 33.3 | 4.0 | 1.6 | 286.5 | 437 |
| aggregate_in_memory | pass | 100000 | 6.447 | 55.0 | 61.6 | 20.5 | 0.0 | 286.5 | 99 |
| derived_table_unsupported | unsupported | 0 | 0.001 | — | 21.4 | 0.0 | 0.0 | 0.0 | 0 |
| join_spill | pass | 1 | 11.837 | 61.8 | 33.6 | 4.1 | 0.3 | 572.9 | 790 |
| join_in_memory | pass | 1 | 11.461 | 56.3 | 50.9 | 13.0 | 0.0 | 572.9 | 361 |
| csv_scan | fail | 0 | 0.016 | — | 22.0 | 1.0 | 0.0 | 2.0 | 3 |
| csv_limit_wide | fail | 0 | 0.017 | — | 21.7 | 1.0 | 0.0 | 2.0 | 3 |
| csv_large_memory_first_chunk | expected_error | 2048 | 0.490 | 95.7 | 295.2 | 270.6 | 0.0 | 271.9 | 4 |
| ndjson_scan | fail | 0 | 0.018 | — | 22.1 | 1.0 | 0.0 | 2.0 | 3 |
| ndjson_large_memory_first_chunk | expected_error | 2048 | 0.588 | 103.7 | 354.1 | 329.0 | 0.0 | 330.3 | 4 |
| json_array | pass | 500 | 0.020 | — | 22.6 | 0.8 | 0.0 | 1.1 | 2 |
| multi_file | pass | 1 | 1.570 | 57.7 | 26.4 | 0.7 | 0.0 | 23.0 | 90 |
| modulo_sample | unsupported | 0 | 0.054 | — | 25.0 | 2.9 | 0.0 | 1.8 | 2 |
| arithmetic_sample | unsupported | 0 | 0.001 | — | 9.0 | 0.0 | 0.0 | 0.0 | 0 |
| reservoir_sample | pass | 1000 | 12.188 | 78.6 | 43.2 | 9.4 | 2.1 | 286.5 | 323 |
| native_sample_unsupported | unsupported | 0 | 0.001 | — | 22.3 | 0.0 | 0.0 | 0.0 | 0 |
| random_function_unsupported | unsupported | 0 | 0.004 | — | 22.4 | 0.0 | 0.0 | 0.0 | 0 |
| cancel | expected_error | 0 | 0.050 | — | 25.2 | 3.0 | 0.0 | 1.8 | 2 |
| consumer_error | expected_error | 0 | 0.052 | — | 25.0 | 3.0 | 0.0 | 1.8 | 2 |
| memory_limit | expected_error | 0 | 0.023 | — | 23.7 | 0.5 | 0.0 | 0.3 | 0 |
| temp_quota | expected_error | 0 | 0.073 | — | 29.0 | 2.9 | 0.0 | 1.8 | 2 |
| concurrent_queries | pass | 4 | 10.278 | 126.0 | 48.4 | 10.1 | 0.0 | 1145.7 | 1090 |
| repeat_queries | pass | 1 | 118.185 | 69.5 | 35.9 | 3.0 | 0.0 | 5728.6 | 10036 |
| copy_large | pass | 100000 | 8.869 | 53.7 | 44.3 | 11.6 | 0.0 | 312.8 | 273 |
| stratified_sample | pass | 1000 | 5.945 | 63.3 | 27.1 | 2.9 | 0.0 | 286.5 | 499 |
| using_sample_unsupported | unsupported | 0 | 0.001 | — | 19.7 | 0.0 | 0.0 | 0.0 | 0 |
| scan_workers4 | pass | 1000000 | 6.594 | 53.1 | 26.5 | 3.1 | 0.0 | 286.5 | 512 |
| csv_scan_default256 | fail | 0 | 0.025 | — | 17.4 | 1.0 | 0.0 | 2.0 | 3 |
| ndjson_scan_default256 | fail | 0 | 0.018 | — | 14.7 | 1.0 | 0.0 | 2.0 | 3 |
| right_join | pass | 1 | 5.914 | 72.1 | 27.0 | 2.9 | 0.0 | 286.5 | 494 |
| full_join | pass | 1 | 5.790 | 64.2 | 28.8 | 3.0 | 0.0 | 286.5 | 486 |
| cross_join | pass | 1 | 5.704 | 59.2 | 28.1 | 2.9 | 0.0 | 286.5 | 500 |

## 关键发现

1. **大文本读取存在功能阻塞（MEM-001）**：CSV/NDJSON 默认串行扫描申请整文件输入缓冲，64 MiB 和默认 256 MiB 预算均无法扫描本轮对象。提高到 1 GiB 后，返回首块前已占用约 271/329 MiB 受管内存；此诊断主动停止，没有把大内存单块成功当作流式能力通过。
2. **Parquet 下推未生效（PERF-001）**：窄列、LIMIT 100、选择性过滤及空结果仍读取约 300,375,678 B、约 3487 次 Range。生产 adapter 源码明确将 Projection/Filter/Limit 留给 residual operators，因此输入 payload 列仍被解码。
3. **Top-N 未见收益**：Top-100 与全排序落盘的中位耗时、实读量及临时文件接近；本轮不能将其当作有效的生产 Top-N 优化。
4. **峰值有界但分配量大**：窄列扫描的 benchmark 累计分配约 1.34 GiB/次；pprof 约 84% 分配来自 MemoryManager 下面的 DefaultAllocator，StringHeap.PutBytes/Buffer.Resize 与未裁剪字符串列相关。它们仍受内存记账管理，但频繁分配触发 GC。
5. **并行收益有限**：1/4 worker 的窄列吞吐接近；四查询并发结果正确，但每个查询仍完整扫描源文件，网络量相应放大。
6. **能力边界明确**：TABLESAMPLE、USING SAMPLE、random()、显式 CAST 和 BIGINT `%` 目前不支持；聚合外层标量子查询和 FROM 派生表也超出当前有限子查询实现。受支持的普通标量子查询、CTE Join、分层谓词抽样和消费端 reservoir 分别验证。

已将资源/性能缺陷和未支持边界写入 [compatibility/deviations.yaml](../../../../compatibility/deviations.yaml)。本轮未新增 Stable feature 或隐藏错误。

## 结果正确性与清理

- DuckDB 独立计算完整结果。ORDER BY 比较有序 SHA256；无序查询比较保留重复计数的行摘要和。
- 未排序 LIMIT 只验证合法子集、行数与 payload；不会把另一合法子集误判为错误。
- 排序、聚合、Join 分别对照内存执行与低预算 spill，确实观测到临时文件，结果均匹配参考。
- Reservoir 通过完整有序百万行流抽取 1000 项，与独立 Python 固定种子参考一致；另有 128 种子 property test。
- 所有已记录场景结束时，受管内存和查询临时文件均为零；包括内存不足、临时配额、取消和消费端错误。
- 连续 20 次查询结束均归零；累计 Go 分配约 26.6 GiB、GC 约一万次。内存释放正确，分配效率仍需改进。
- 100000 行含宽字符串的 COPY 写入真实 MinIO，随后回读全部 id/value/payload，完整行摘要与 DuckDB 参考一致。

## End-to-end Benchmark（三次样本）

回调只计数，不做逐行 SHA 校验。表中 CPU 100% 代表一个逻辑核，并非整台 12 核机器的 100%。RSS 包含 Go runtime/连接池，不能等同于受管内存；顺序 benchmark 的 RSS 也受前序场景保留页影响。

| 场景 | n | 中位 s | 最小/最大 s | CPU % | 受管峰值 MiB | 累计分配 MiB/op | 实读 MiB/op |
| --- | ---: | ---: | ---: | ---: | ---: | ---: | ---: |
| scan_narrow | 3 | 6.375 | 5.838 / 6.930 | 52.3 | 2.9 | 1372.2 | 286.5 |
| scan_wide | 3 | 6.341 | 6.298 / 6.642 | 63.5 | 3.0 | 1864.6 | 286.5 |
| filter_range | 3 | 6.097 | 6.012 / 6.692 | 54.3 | 2.9 | 1373.8 | 286.5 |
| sort_spill | 3 | 12.136 | 11.878 / 12.206 | 79.0 | 7.6 | 1495.2 | 286.5 |
| sort_in_memory | 3 | 13.107 | 13.093 / 13.542 | 83.9 | 24.9 | 1428.8 | 286.5 |
| topn | 3 | 12.064 | 11.999 / 12.104 | 79.7 | 7.6 | 1503.0 | 286.5 |
| aggregate_spill | 3 | 7.098 | 7.067 / 7.374 | 62.9 | 4.0 | 1533.5 | 286.5 |
| join_spill | 3 | 12.470 | 12.267 / 12.508 | 57.2 | 4.1 | 2849.2 | 572.9 |
| scan_workers4 | 3 | 6.246 | 5.889 / 6.265 | 62.0 | 3.0 | 1371.7 | 286.5 |

## Race、microbenchmark 与回归

| Race 场景 | 最终结果 | 耗时 s |
| --- | --- | ---: |
| global_aggregate | pass | 26.084 |
| topn | pass | 155.074 |
| aggregate_spill | pass | 46.991 |
| join_spill | pass | 40.810 |
| cancel | expected_error | 0.093 |
| consumer_error | expected_error | 0.087 |
| concurrent_queries | pass | 23.640 |
| scan_workers4 | pass | 14.827 |

Race 初轮 Top-N 因测试器 2 分钟上限被取消；资源清理通过，未出现数据竞争报告。提高到有界的 10 分钟后只复测同一场景，不放宽结果断言。保留初轮和重测日志，race 耗时不用于生产 benchmark 对比。

microbenchmark 使用既有向量、Hash、Join、Aggregate、Sort、Distinct 与 Native API，100ms × 3 次。

| 包/benchmark | 样本数 | 中位 ns/op | 中位 B/op | 中位 allocs/op |
| --- | ---: | ---: | ---: | ---: |
| core-aggregate / BenchmarkHashAggregateBigInt | 3 | 764168.0 | 218193 | 61 |
| core-aggregate / BenchmarkExternalAggregateForcedSpill | 3 | 5462795.0 | 953139 | 3242 |
| core-distinct / BenchmarkExternalDistinct | 3 | 64087550.0 | 11189884 | 40903 |
| core-expression / BenchmarkNumericAdd2048 | 3 | 99046.0 | 25880 | 14 |
| core-hash / BenchmarkVectorHashInt64 | 3 | 36946.0 | 0 | 0 |
| core-hash / BenchmarkHashTableLookup | 3 | 42.3 | 0 | 0 |
| core-hash / BenchmarkVectorHashVarchar | 3 | 118426.0 | 0 | 0 |
| core-join / BenchmarkExternalHashJoinForcedSpill | 3 | 9644900.0 | 1347022 | 6178 |
| core-join / BenchmarkInnerHashJoin2048 | 3 | 1701654.0 | 507585 | 146 |
| core-native / BenchmarkNativeQueryChunks | 3 | 343703.0 | 18696 | 174 |
| core-sort / BenchmarkExternalSortForcedSpill | 3 | 8996654.0 | 684452 | 4855 |
| core-sort / BenchmarkTopN | 3 | 3026264.0 | 275819 | 4365 |

core-status.json：`{"native": 0, "expression": 0, "hash": 0, "join": 0, "aggregate": 0, "sort": 0, "distinct": 0}`。

regression-status.json：`{"correctness": 0, "race": 0, "vet": 0, "architecture": 0, "duckdb": 0, "duckdb-race": 0, "linux-compile": 0, "fixture-python": 0}`。

## 共享服务资源

以下为 Docker stats 约 5 秒采样的整个既有容器占用，包含其缓存及其他潜在客户，不能归因于单条查询，也不能与 Windows 引擎 RSS 直接相加。

| 容器 | 样本 | CPU 峰值 % | 内存峰值 MiB |
| --- | ---: | ---: | ---: |
| docker-minio-1 | 189 | 288.4 | 999.2 |
| docker-postgres-1 | 189 | 0.3 | 43.5 |

## 证据与验证边界

- [完整原始矩阵](normal.log)、[抽样边界复核](recheck.log)、[四 worker](workers.log)、[默认内存预算](defaults.log)、[RIGHT/FULL/CROSS Join 补充](join-extra.log)、[COPY 全行内容校验](copy-full.log)。
- [三次 end-to-end benchmark](bench.log)、[CPU 热点](cpu-top.txt)、[累计分配热点](alloc-top.txt)、[存活 heap 热点](heap-live-top.txt)。
- [初轮 race](race.log)、[Top-N race 重测](race-topn-retry.log)、[四 worker race](workers-race.log)。
- [结构化汇总](summary.json)、[指标 CSV](metrics.csv)、[数据与 SQL 描述](manifest.json)、[运行方法](../../../../docs/testing-large-minio.md)。
- CPU/heap/临时文件采样存在开销；RSS 与临时文件瞬时峰值可能低估，受管内存峰值来自精确记账。小于 100ms 的 CPU 百分比不作为可比较数据。
- 不使用 race 的 RSS/耗时作为正常运行基线；pprof 和普通基线独立运行。
- 本轮大数据覆盖整型、浮点聚合、NULL 和宽字符串；没有执行大规模 DECIMAL/LIST/STRUCT、压缩编码全矩阵、任意规模极限、长时间 soak 或真实断电注入。仓库已有 unit/fault/interop 测试不等于这些维度的大文件规模验证；不宣称完整 Release Gate 通过。
- 当前专项结论为 FAIL：大文本读取与性能缺口尚未修复。项目阶段元数据未修改。
