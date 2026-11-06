# UNION ALL 长链性能修复

## 原因与修复范围

CPU profile 显示，修复前约 60.5% 的采样时间位于 `SinkTakesChunkOwnership`、`unionBranchSink.TakesChunkOwnership` 与 `TransformSink.TakesChunkOwnership`。左结合 UNION 链为每个中间结果增加透传 Project，行数据经过多层 Sink；各层又重复向下查询所有权，放大了执行开销。问题并不主要来自解析或绑定。

修复在 Binder 完成逐层类型推导之后进行：Planner 按原分支顺序收集同 schema 的 UNION 输入，跳过 Binder 生成的纯透传包装，再构造平衡二叉树。1025 项计划的最大深度由 2050 层降至 14 层以内；Project 总数由 2049 降至 1026，保留根输出投影和各 SELECT 分支投影。

仅在列名、类型、可空性完全一致时重关联。CAST、过滤、聚合、DISTINCT、排序、LIMIT/OFFSET、物化和命名 CTE 边界保留；不承诺混合类型或复杂查询边界有相同收益，也不改变它们的既有语义。

## 同机端到端基准

环境：Go 1.24.2、Windows/amd64、Intel Core i5-12400F。查询形状为 `SELECT 0 AS value UNION ALL SELECT 1 AS value ...`，每个分支返回一行整数；计时包含解析、绑定、规划、执行及结果消费，DB 创建和 SQL 构造不计时。测试使用 64 MiB 引擎受管内存上限。

修复前为保存的旧测试程序单次结果；修复后为独立运行三次的中位数。修复前 100/500/1000 项同时采集 CPU profile，2049/4097 项未开启 profile。数据是本机观测值，不作为固定时间门槛。

| UNION 项数 | 修复前 | 修复后（三次中位数） |
| --- | ---: | ---: |
| 100 | 8.12 ms | 2.37 ms |
| 500 | 229.02 ms | 11.27 ms |
| 1000 | 1468.41 ms | 22.91 ms |
| 2049 | 10372.45 ms | 45.96 ms |
| 4097 | 76823.51 ms | 93.20 ms |

4097 项每次查询的 Go 分配总量从约 4.22 GB 降至约 56.68 MB，分配次数从约 7629 万降至约 50.75 万。分配总量是 `B/op` 累计值，不是峰值驻留内存或引擎受管内存。

复现当前基准：

```sh
go test ./pkg/gosql -run '^$' -bench '^BenchmarkUnionAllChain$' -benchtime=1x -count=3
```

## 回归验证

- `TestUnionAllPlanDepthAndOrder`：约束 1025 项的计划深度、投影数量与分支顺序，不使用耗时断言；已验证在原实现上失败。
- `TestUnionAllLongChain`：4097 项的完整有序消费、LIMIT、排序/OFFSET、回调失败回收及 Materialize 所有权；快照关闭后受管内存回到 0。
- `TestUnionAllReassociationBoundaries`：保留 BIGINT→FLOAT→DOUBLE 的中间舍入、NULL、重复行、派生表修饰符、共享 CTE 和既有转换错误。
- 完整相关包测试通过：`internal/planner`、`internal/optimizer`、`internal/execution/lowering`、`internal/execution/pipeline`、`internal/session`、`pkg/gosql`、`examples/quickstart`。
