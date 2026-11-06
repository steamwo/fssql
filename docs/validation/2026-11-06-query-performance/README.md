# 九类查询性能优化与 DuckDB 对照

本次针对 `inner_join`、`group_having`、`multi_file`、`scan_narrow`、`global_aggregate`、`offset`、`sort_in_memory`、`sort_spill`、`topn` 做端到端优化。本地九项中位数加速约 1.65～2.72 倍；真实 MinIO 主要收益来自排序。仍未追平 DuckDB：本地 INNER JOIN 仍慢约 11.7 倍，远端扫描与多文件查询基本持平，不能声称全部差距已消除。

## 实现与依据

- 本地范围读取复用扫描期间的只读句柄，Parquet footer 的 trailer/metadata 也共用句柄，减少 Windows 下反复执行 `EvalSymlinks` 和打开文件的成本。根目录校验仍在打开前完成；读前、读后、路径替换及预期身份检查保留。VFS 通过可选接口接入，S3 等未实现该接口的驱动保留原路径。
- 定长整数排序键一次编码为带 NULL 顺序的字节键，使用受管工作区进行稳定基数排序；有符号极值、无符号、HUGEINT、DECIMAL、DESC 和多键均由回归测试与原 SQL 比较器对照。浮点及变长键保留原比较路径。排序预算包含归一化键和双索引；提前预留空间以吸收 builder 倍增扩容，不提高内存上限。
- TopN 固定候选区、多路归并页面和索引区在拥有者生命周期内借用底层切片，避免每次比较或逐行读取时获取 `Buffer` 锁，释放时清空借用。
- 聚合增加 BIGINT flat 批次归约，以及有界单整数键精确缓存。缓存碰撞仍比较完整键与 NULL 状态；仅使用外排算子预算的至多 1/16，小预算自动保留原路径，并纳入受管内存与 spill 决策。
- JOIN 固定宽度键在批次入口解析列布局，探测阶段按批次检查取消。分区哈希表定位混入哈希高位，避免 radix 分区已固定的低位再次造成槽位集中；完整哈希、完整键比较及重复键链均保留。

设计参考 DuckDB 的[排序键归一化说明](https://duckdb.org/2025/09/24/sorting-again)与[文件句柄读取实现](https://github.com/duckdb/duckdb/blob/main/src/storage/external_file_cache/caching_file_system.cpp)。本次实现沿用 fssql 的受管内存、对象身份和清理约束，没有调用 DuckDB 执行 fssql 查询，也没有针对 fixture 的数值或路径硬编码结果。

## 比较口径

- 数据：既有 100 万行 fixture，同一份本地 Parquet 与真实 MinIO S3 对象；多文件场景使用同一份 `parts/*.parquet`。
- 环境：Go 1.24.2、DuckDB Python 2.0.0、Windows/amd64、Intel Core i5-12400F。
- 配置：单 worker/thread；通常为 64 MiB 查询内存、4 MiB spill 策略预算；`sort_in_memory` 为 256 MiB 查询内存、128 MiB spill 策略预算。没有通过增大线程或内存取得收益。
- 保存优化前测试二进制；按固定随机种子交替运行优化前、优化后和 DuckDB。每种来源/查询/引擎先预热 2 次，再记录 3 个样本；共 162 个有效计时样本。
- fssql 完整消费 Chunk，DuckDB 完整消费 2048 行 Arrow batch；结果读取只计行数，排除逐行摘要成本。计时包括解析、绑定、规划与执行，不含 DB/连接创建和 fixture 生成。
- 正确性另跑真实 MinIO 百万行矩阵，对完整结果摘要、排序、内存和临时存储回收作独立校验。

[全部原始计时样本](samples.jsonl)、[精确中位数与 MAD](summary.json)、[百万行正确性结果](correctness.jsonl)。二进制 SHA256 保存在 `summary.json` 中，优化前指本次修改前保存的工作区构建，并非对未提交改动重新解释为某个 Git 提交。

## 结果

耗时单位为毫秒；加速倍数是“优化前 / 优化后”，DuckDB 比值是“优化后 fssql / DuckDB”，数值越接近 1 越好。

| 来源 | 场景 | 优化前 | 优化后 | DuckDB | 加速 | DuckDB 比值 |
| --- | --- | ---: | ---: | ---: | ---: | ---: |
| local | scan_narrow | 218.50 | 110.62 | 13.58 | 1.98x | 8.15x |
| local | sort_spill | 1252.69 | 556.91 | 139.13 | 2.25x | 4.00x |
| local | sort_in_memory | 1425.12 | 524.27 | 137.53 | 2.72x | 3.81x |
| local | topn * | 164.10 | 92.60 | 17.64 | 1.77x | 5.25x |
| local | offset | 337.21 | 175.37 | 29.81 | 1.92x | 5.88x |
| local | global_aggregate | 198.59 | 92.44 | 13.41 | 2.15x | 6.89x |
| local | group_having | 208.88 | 125.57 | 13.44 | 1.66x | 9.34x |
| local | inner_join | 207.36 | 125.68 | 10.74 | 1.65x | 11.70x |
| local | multi_file | 133.82 | 69.20 | 8.44 | 1.93x | 8.20x |
| s3 | scan_narrow | 458.81 | 473.77 | 225.10 | 0.97x | 2.10x |
| s3 | sort_spill | 1449.36 | 802.50 | 335.67 | 1.81x | 2.39x |
| s3 | sort_in_memory | 1614.22 | 783.78 | 272.87 | 2.06x | 2.87x |
| s3 | topn | 345.10 | 335.50 | 211.71 | 1.03x | 1.58x |
| s3 | offset | 508.09 | 415.46 | 226.24 | 1.22x | 1.84x |
| s3 | global_aggregate | 379.01 | 328.40 | 191.44 | 1.15x | 1.72x |
| s3 | group_having | 387.84 | 371.54 | 193.98 | 1.04x | 1.92x |
| s3 | inner_join | 321.31 | 269.44 | 188.91 | 1.19x | 1.43x |
| s3 | multi_file | 237.18 | 237.24 | 84.16 | 1.00x | 2.82x |

`*` 本地 TopN 的 DuckDB 样本 MAD/中位数约 12%，超过原比较器的 10% 稳定性门槛；保留全部样本并标注波动，不把该比值当成稳定保证。其他来源/场景/引擎组合均低于 10%。三次样本只证明本机本轮观测，不代表跨平台或冷缓存结论。

## 验证

```sh
go test ./internal/... ./pkg/gosql ./examples/quickstart
go test -race ./internal/storage/local ./internal/execution/aggregate ./internal/execution/join ./internal/execution/sort -run 'Test(RangeReader|ExternalAggregateWorker|IntegerGroupCache|NormalizedKeys|PartitionedHash|Resident|SortCancel)' -count=1
python test/integration/duckdb_compare.py --samples 3 --warmups 2 --sources local,s3 --only scan_narrow,global_aggregate,multi_file,group_having,inner_join,offset,sort_in_memory,sort_spill,topn --tag optimize-final
```

单元回归、相关 race 检查通过。真实 MinIO 九项完整摘要全部一致，所有场景查询后受管内存和临时存储归零；`sort_spill` 实际临时存储峰值为 21,011,040 字节，`sort_in_memory` 为 0。

本轮还发现旧二进制在当前 Windows 环境的目录采样同样把强制外排峰值记录为 0，导致原 `needs_spill` 断言失败。现使用精确临时存储配额峰值补充目录采样，仍要求强制外排实际落盘，未移除或放宽该断言。旧版计时样本的目录峰值不能据此解释成未发生 spill。

## 仍有差距

本地 INNER JOIN 仍约为 DuckDB 的 11.7 倍。远端窄扫描未加速，多文件远端场景也基本持平。本次没有改变 S3 请求合并、远端预取或网络并行策略；这些 IO 路径仍需要独立 profile 与正确性验证。当前优化只覆盖已验证的类型与布局，浮点/变长排序仍使用既有比较器，不将结果外推到所有 SQL、数据分布、冷缓存或其他操作系统。
