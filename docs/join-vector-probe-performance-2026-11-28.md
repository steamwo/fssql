# JOIN vector probe、输出复用与全局聚合融合

日期：2026-11-28。基线：`00f4f56295a5d89619ed64eea4030cae3daa8c8b`。

本次修复内存 `HashJoin` 每批 probe 编码临时 tuple collection、重复创建输出 vectors 的问题，并将复用和聚合批量消费接入生产 `StableExternalHashJoin`。生产 equi JOIN 在基线中已经具备输出列 mask、批量 hash/compare 和部分唯一整数 direct-index 路径，不能把内存 JOIN 的全部提速归因到生产 SQL。

## 实现范围

- 内存 JOIN 的单 BIGINT/UBIGINT key 直接借用 flat、constant 或 dictionary vector，计算 semantic hash 并比较 build key；保留重复键链和普通 equality 的 NULL 不匹配语义。其他 key 使用已有 vector hash/comparer，无须创建 probe tuple collection。
- 内存 JOIN 和 CROSS runtime 接收 required-output mask，保持原 schema 下标，以紧凑 NULL constant 代替未使用列，跳过对应复制和 materialization。生产 equi JOIN 原有 mask 继续生效。
- Query-local pool 复用 emitter、匹配选择数组、comparer metadata、输出 vectors 和 VARCHAR heap。选择数组先预留 managed memory；空闲输出在 JOIN 关闭时释放，活动 lease 归还时释放。
- Resident 和 spill emitter 均复用同步 borrowed-consume 输出。Ownership-taking 或支持 owned-input move 的下游保留既有转移路径，避免覆盖保留的 chunk，也避免阻断 projection 的列移动优化。
- 无 GROUP BY 的 COUNT(*)、COUNT(expr)、SUM 直接消费 borrowed match batches。纯列 aggregate pre-projection 在 lowering 时重映射到 JOIN schema，避免隐藏批量接口。保留 NULL 扩展、重复键 multiplicity、整数溢出及浮点累加顺序。
- BIGINT flat hash 和 build BIGINT SUM 在批次入口选择专用 kernel。通用 RowLayout、复杂表达式、分组聚合和 spill 聚合 materialization 继续承担 fallback。
- 保留 PackedTopN 和 VARCHAR 生产路径。字符串正确性测试扩展到高基数、长公共前缀、有序/逆序、多 ties、NULL，以及 K=1/100/257。

当前融合仍枚举每个匹配，尚未按 build key 预聚合 COUNT/SUM；后者是另一层优化。

## 同机诊断微基准

AMD EPYC 9V74，Linux amd64，Go 1.22.12，`GOMAXPROCS=1`。容器 CPU quota 为 2 核、内存限额 8 GiB，benchmark memory manager 为 256 MiB。每项先运行两次 warmup，再测七个独立进程，每进程 `benchtime=200ms`。测试和其他 benchmark 未与正式测量并发执行。

每次操作 probe 2,048 行，build 1,000 行，左右各两列 BIGINT。向量在计时前创建，build、cache warmup、finalize 和 cleanup 均不计时。probe key 为 `row % 2000`，因此 INNER 每批输出 1,048 行，LEFT 输出 2,048 行；这四项输出全部四列，没有使用列裁剪或聚合融合。

| 路径 | 基线中位数 µs/batch | 优化后 µs/batch | 提速 | B/op 前 → 后 | allocs/op 前 → 后 |
|---|---:|---:|---:|---:|---:|
| 内存 INNER | 413.189 | 64.937 | 6.36× | 96,336 → 0 | 31 → 0 |
| 内存 LEFT | 513.418 | 94.567 | 5.43× | 96,336 → 0 | 31 → 0 |
| 生产 Stable INNER | 45.507 | 34.960 | 1.30× | 39,936 → 0 | 23 → 0 |
| 生产 Stable LEFT | 142.724 | 106.435 | 1.34× | 68,272 → 0 | 28 → 0 |

中位绝对偏差（µs，前 → 后）：内存 INNER 17.056 → 1.954；内存 LEFT 23.895 → 7.989；Stable INNER 1.106 → 1.092；Stable LEFT 2.655 → 2.048。原始样本保留了波动，没有删除 outlier。

另一个内存 INNER 微基准用 `row % 1000` 保证全部匹配，消费 `COUNT(*) + SUM(build payload)`。两种模式都使用本次 direct probe、列裁剪和 buffer 复用，仅对比 materialized 与 fused aggregate 消费：

| 消费模式 | 中位数 µs/batch | B/op | allocs/op |
|---|---:|---:|---:|
| Materialized，已裁剪到 payload | 81.199 | 0 | 0 |
| Fused match batch | 64.544 | 0 | 0 |

融合减少约 20.5% 耗时。每个 measured batch 验证结果所需的期望贡献为 COUNT=2,048、SUM=1,000,128，计时前的一次 cache warmup 也纳入最终结果校验。

**这些是 fssql 自身算子诊断结果，不是上传报告中 100M seed-repeat workload 的复跑，也不提供新的 DuckDB 耗时比。** `0 B/op` 描述 cache warmup 后这些固定宽度、同步消费场景的稳态 probe，不能解释为整个查询、字符串输出、spill 或 ownership-taking result cursor 都没有分配。

原始记录：[before.txt](benchmarks/join-vector-probe-2026-11-28/before.txt)、[after.txt](benchmarks/join-vector-probe-2026-11-28/after.txt)、[全部样本与中位数](benchmarks/join-vector-probe-2026-11-28/measurements.json)、[环境与源码指纹](benchmarks/join-vector-probe-2026-11-28/environment.json)。

## 复现与验证

快速诊断：

```sh
GOMAXPROCS=1 go test ./internal/execution/join -run '^$' \
  -bench '^(BenchmarkJoinVectorPipeline|BenchmarkJoinGlobalAggregate)$' \
  -benchmem -benchtime=200ms -count=7
```

以上快速命令在同一进程重复。复现记录中的独立进程采样时，先用 `go test -c` 构建测试 binary，再对每个精确 sub-benchmark 分别执行九次，排除前两次。例如：

```sh
go test -c -o /tmp/fssql-join.test ./internal/execution/join
GOMAXPROCS=1 /tmp/fssql-join.test -test.run='^$' \
  -test.bench='^BenchmarkJoinVectorPipeline$/^memory$/^inner$' \
  -test.benchmem -test.benchtime=200ms -test.count=1
```

基线需创建上述 commit 的独立 worktree，将当前 `vector_pipeline_bench_test.go` 复制到相同包后构建；这个 benchmark 文件不依赖新增 API。聚合融合的两项均在当前实现内对比，不存在基线 commit 的融合结果。

验证命令：

```sh
go test -count=1 ./...
go test -race -count=1 ./...
go run ./tools/architecture-check
python tools/check-duckdb.py --pr
```

新增资源测试检查 warmed buffer/字符串 heap 不增长、部分 batch 复用、ownership 保留与 move 路径、分配失败、关闭活动 lease 和最终 managed memory 归零。聚合融合对照 materialized 输出，覆盖 INNER/LEFT/RIGHT/FULL/SEMI/ANTI、重复键、NULL 和空 build。DuckDB typed corpus 新增十二项，包含 NULL-safe equality、浮点/DECIMAL、字符串 COUNT、HUGEINT 溢出和复杂表达式/分组回退。
