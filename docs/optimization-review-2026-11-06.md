# fssql 引擎优化与改进计划（2026-11-06）

日期：2026-11-06

范围：本仓库 HEAD da17401 及分析时未提交的迁移修复、ZSTD 读取变更。本文负责引擎实现、公共 API 和引擎验收；edge-agent-go 的配置、业务流程和 HTTP 回放由调用方文档维护。本次仅整理文档，未执行新的实现或验收。已复现、profile 证据、静态风险和未验收能力分别记录，建议不表示已经取得性能收益。

## 本轮实际验证

环境：Windows amd64，Go 1.27.3，CGO_ENABLED=0，Intel i7-10510U（4 核/8 线程）。使用 gosql 公共入口、单 worker、256 MiB 受管内存、64 MiB spill 预算。合成源固定 100,000 行，包含 BIGINT id、5 个一字节字符串分组、约 78 字节 Unicode 文本。源逐批生成，不从网络读取。结果回调完整消费但只统计行数，未逐值校验；这是分配诊断，不是结果正确性或 DuckDB 性能对标。

每场景两次预热、七次测量，测量覆盖 QueryChunks 的规划、执行、完整消费和清理；两次采样间执行 runtime.GC。下表为七样本中位数，分配次数与分配字节数分别取中位数。

| 查询场景 | 分配次数 | 累计 Go 分配 MiB | 耗时中位 ms | 耗时 MAD/中位数 |
| --- | ---: | ---: | ---: | ---: |
| 直接文本投影 | 2,237 | 30.52 | 69.43 | 12.03% |
| LOWER 文本转换 | 603,497 | 104.51 | 300.89 | 4.22% |
| 公共 HMAC-SHA256 UDF | 1,503,449 | 168.56 | 629.60 | 15.05% |
| 按 grp、id 排序 | 5,014 | 39.82 | 472.05 | 12.38% |
| ROW_NUMBER，按 grp 分区、id 排序 | 410,397 | 135.88 | 674.87 | 9.94% |

全部样本返回 100,000 行，查询结束的受管占用为 0。查询执行的工作不同，不能按表中耗时求“算子加速比”。投影、HMAC、排序的耗时波动超过项目 10% 门槛，不作为正式性能结论。累计分配不是峰值内存或 RSS。

另用隔离进程各执行 HMAC、窗口场景并采集 allocs profile。profile 包含各自进程中的预热和测量，是采样统计，不与某一条样本的精确分配数直接相等：

- 窗口：partitionKey 调用链占采样分配对象约 96.4%；但按字节计算，DefaultAllocator 占约 86.8%，Buffer.Resize 累计调用链占约 73.8%。应同时处理逐行小对象和大缓冲增长，不应只按对象数判断内存成本。
- HMAC：executeScalarFunction 调用链覆盖几乎全部采样对象；HMAC/SHA256 初始化、hex 编码、内部/公共 ScalarValue 转换都是可见来源。

诊断程序、最终样本、汇总和 profile 文本已归档到 [验证证据目录](validation/2026-11-06-optimization-review/README.md)。证据来自本轮文档整理前的诊断，复制归档不代表重新运行。早期包含夹具字符串构造分配的 samples.jsonl 未纳入正式归档，最终表格使用 clean-samples.jsonl。原始二进制和 pprof 保留在本机 edge-agent-go 的 .cache/fssql-optimization-audit/，不属于本仓库版本化产物。

## 优先级与建议

### FOPT-01 / P0：统一 CSV 推断与执行的引号/NULL 语义（已复现）

输入 quoted-empty.csv：

```csv
v
1
""
```

```sql
SELECT v FROM read_csv('quoted-empty.csv', header=true, allow_quoted_nulls=false);
```

fssql 自动推断为 BIGINT，读取时报 csv: row 2 column 1: invalid BIGINT ""。显式 columns={'v':'VARCHAR'} 返回 ['1','']。本机 DuckDB 2.0.0 自动推断为 VARCHAR 并返回相同两值，证据见 [csv-duckdb-reference.json](validation/2026-11-06-optimization-review/csv-duckdb-reference.json)。

根因：internal/format/csv/bind.go 把编码的 NULL 选项转为普通 NullLiteral 再交给 InferSchema；infer.go 的 observeKind 仅收到解码字节，失去字段是否带引号的信息。执行 decode.go 保留 quotedField，并按 allow_quoted_nulls 区分，引起 schema 与执行不一致。

建议：推断和执行共用字段 NULL 判定规则，推断保留 quoted 状态；加入自动/显式 schema、引号空串/无引号空字段、数字/日期/纯文本、CRLF/EOF 的契约回归。提供 columns 的显式类型路径不等同于这里的自动推断失败；本项验收必须覆盖自动推断与执行的一致性。

### FOPT-02 / P0：清理失败的错误可见性与重试（静态风险，原占用根因未确定）

internal/query/context.go 的 Context.Close 在资源清理之前设置 closed=true，随后执行 cleanup、Temp.Close 和 Memory.Close。若临时文件删除失败，下一次 Close 会因为 closed=true 直接返回 nil，不能重试未完成的清理，也没有保留第一次清理错误。

此前 edge-agent-go 联调中实际观察到 Windows 临时目录共享占用；本轮没有再次复现，也不能据此把根因归于 Context.Close。

建议：分开“拒绝新工作”和“资源清理完成”的状态，保留清理错误，明确重试或后台回收契约；加入首次 sharing violation、二次解除占用、取消/早关、并发 Close 的故障注入测试。验收必须包含文件句柄、临时目录和配额释放，不只检查 managed_after=0。

### FOPT-03 / P1：窗口键批量比较，去掉逐行临时值（profile 已确认）

internal/execution/expression/window_execution.go 的 partitionKey 每行 make([]ScalarValue)，并经 scalarFromVector/Vector.Bytes 取值和复制；字符串还再次复制，只有切换分区时才真正需要保存自己的上一分区键。

建议：复用已有 BorrowFixedWidth/BorrowBytes，按 batch 比较相邻分区；只在分区改变或跨 chunk 保留最后键时复制。保留 NULL、NaN、DECIMAL、字典/常量、跨批分区、多个窗口和错误清理语义。先对现有 ROW_NUMBER/SUM 等路径减分配，再考虑分组 TopK、兼容窗口排序复用。不能只为 rn<=K 提前截断而改变原业务种子、排序或配额语义。

### FOPT-04 / P1：LOWER/MD5/日期/HMAC 的批量内核与 UDF 桥接（profile 已确认一部分）

executeScalarFunction 每行分配参数切片，scalarFromVector 对字符串复制；公共 UDF 桥接又创建公共参数并转换字符串，返回后再次转换成内部字节。HMAC 工厂每行 hmac.New、Sum(nil)、hex.EncodeToString，重复初始化密钥状态并产生中间结果。

已有 string_batch.go 优化 length/strpos/concat，说明可复用现有模式，无需重写 Vector/Pipeline。优先补 LOWER、MD5、DATE_TRUNC 等迁移高频函数，以及兼容现有 Eval 的可选 batch UDF 能力。

HMAC 可让每个执行器/worker 持有独立 hash 状态并 Reset，在有界输出缓冲中直接 hex 编码。不能并发共享一个 hash 实例，不能跨 DB/租户混用密钥。普通第三方 UDF 的参数拥有权、NULL 策略和调用语义也不能为了借用字节而悄悄改变。

### FOPT-05 / P1：受管缓冲增长、元数据计量与物化 I/O（代码已确认，收益需测量）

Buffer.Resize 总是新建完整缓冲并复制旧内容；StringHeap 从 64 字节逐次倍增。窗口 profile 中大部分累计字节来自这条链路。Resize 只把 reservation 调整到新长度，复制期间新旧 backing 同时存活，真实瞬时占用高于最终容量记账。

建议先做已知长度/批次字节数的受控预留，减少增长和复制；必要时采用有总量上限且计入资源预算的复用，不应直接使用无界 sync.Pool。明确新旧缓冲同时存活期间的峰值预算契约。

runtime_window.go、runtime_cte.go 还使用普通 []PageID 保存随输入增长的页号，CTE 发布/读取时复制整份列表；内部已经有受管 PageList，应复用，补齐 metadata 的资源限制。

OVER() 全局窗口和 CTE 物化直接走 SerializeChunk -> WritePage -> ReadPage -> DeserializeChunk，缺少按预算先驻留、必要时 spill 的选择。可以借鉴现有 Materialized/typed source 的有界驻留策略。一次性 CTE、重复 CTE、全局聚合窗口分别规划，避免每个阶段独立落盘；必须保留来源只打开一次和非确定表达式只求值一次。

### FOPT-06 / P1：Parquet 写出批量化和 row group 策略（代码已确认）

默认 row group 为 2,048 行，page 为 512 行。百万行约 489 个 row group；宽表会产生更多 column chunk、page header 与 footer metadata。WriteChunk 按行、按列调用 copyParquetScalar；公共 CopyOptions 没有 row-group 字节/行数策略。

建议增加受内存限制的字节目标，允许配置行数上限，使用批量定宽/字符串复制路径；对文件大小、导出吞吐和下游读取请求数一起测量，不能简单无限加大 row group。

flushRowGroup 对所有文件检查 int16 ordinal。默认行数设置下，静态推算单文件最多 32,768 个完整 row group，即 67,108,864 行；这里没有实际生成该规模文件。应明确超大文件与原生加密的 ordinal 边界，提前验证、滚动分片，或在格式允许的未加密路径省略可选 ordinal，避免工作到文件末尾才失败。

当前工作区已新增 ZSTD 解码，不应把读取 ZSTD 再列为完全缺失。仍应做实际 DuckDB/PyArrow 生成文件的扫描互通，以及截断、错误尺寸、内存限额测试；写出选项当前仍仅 identity/snappy。

### FOPT-07 / P2：按 stage 并行，防止“提高 Workers 却没收益”（代码已确认）

requiresSerialExecution 发现状态窗口或要求稳定 RNG 顺序的抽样时，把整张计划的 workers 设为 1；这同时影响本来可以并行的上游扫描/表达式，而非只约束有序阶段。

公共 Open 构造的 StableQuerySourceFactory 默认 CSVScan.EnableParallel=false，Workers 不会自动变成 CSV 并行扫描。内部 parallel CSV planSafeOffsets 又有全文件摘要扫描和后续边界扫描，因此不能简单开关并行就假设更快。

建议先分开并行上游和有序阶段；对 CSV 采用有界的边界/解码流水线，分别测 1/2/4 worker 的真实读取字节、请求数、吞吐和内存。固定种子抽样仍需保持规定的 RNG 事件顺序。

### FOPT-08 / P2：生产扫描验收与查询观测接口

已有 PERF-001/MEM-001 记录仍保留生产远程 Parquet、超过预算的远程 CSV/NDJSON 验收缺口；loopback 正确性与物理 I/O 结果不能代替生产服务测量。Nov 4 的批量哈希、低基数字典、JOIN、TopN 已完成部分优化，不能把它们重新列成尚未实现。

建议公开兼容的逐查询观察接口：阶段耗时、输入/输出行数、排序数、物化/spill 字节、实际 I/O 字节与请求数、受管峰值及来源策略。配合外部采样 RSS/Go heap/GC。内部已有 profile 数据结构，应由引擎在公共 API 中受控暴露查询级观察能力；调用方负责接入 HTTP、上传与任务观测，不在引擎内实现业务计量。

unknown function 的明确拒绝保留；按调用方实际使用的 SQL 补齐函数/语法，并持续使用固定参考版本差分。事务、通用相关子查询、更多协议不作为当前迁移吞吐的优先工作。

## 实施顺序与验收责任

| 顺序 | 任务 | 当前证据状态 | 引擎验收重点 |
| --- | --- | --- | --- |
| 1 | FOPT-01 CSV 语义 | 已复现，尚未修复 | 自动/显式 schema 与固定 DuckDB 参考逐值比较 |
| 1 | FOPT-02 清理失败 | 静态风险；历史 Windows 占用根因未定 | 故障注入、重试、错误保留、句柄/目录/配额释放 |
| 2 | FOPT-03 窗口键 | profile 已确认分配热点 | 结果不变、跨批语义、分配下降、失败清理 |
| 2 | FOPT-04 批量标量/UDF | HMAC/LOWER 分配已测，其他内核需逐项测量 | NULL/编码/密钥隔离、参数拥有权、批量与原实现一致 |
| 3 | FOPT-05 缓冲/物化 | 静态路径与窗口缓冲 profile 已确认 | 内存峰值、metadata 限额、spill 字节、单次求值 |
| 3 | FOPT-06 Parquet writer | 静态路径与边界推算，未测优化收益 | 独立格式互通、吞吐/文件大小/下游读取、边界提前拒绝 |
| 4 | FOPT-07 阶段并行 | 静态执行路径已确认 | 1/2/4 worker 一致性、种子顺序、真实 I/O 和资源 |
| 4 | FOPT-08 引擎观测/远程验收 | 既有 PERF-001/MEM-001 尚有未验收范围 | 可选观察接口、真实服务数据、固定参考完整结果 |

表格中的“已测”“已复现”只描述分析证据，不表示优化已实现或发布验收完成。每项实现独立验证，不以改期望值绕过契约。

每个优化保留同语义旧实现对照或固定 DuckDB golden，检查 schema、NULL/空串、精确 Decimal、时间、种子、重复行和顺序。覆盖 64/256/512 MiB、1/2/4 worker、同一 DB 并发查询及独立 DB 的资源包络。诊断 profile 与正式性能采样分开执行。

本次文档整理未运行新测试。分析轮也未执行新的全量测试、Linux、race、真实数据库/MinIO 或 Java 加密消费端。此前全量通过记录属于当时的工作区快照，不替代后续修改的回归。

## 相关文档与调用方边界

- [既有性能优化记录](performance-optimization-progress-2026-11-04.md)：批量哈希、字典、JOIN、TopN 的已有工作
- [扫描验收记录](validation/2026-11-06-scan-acceptance/report.md)：生产远程验收的证据和剩余范围
- [迁移能力任务](edge-agent-go-duckdb-migration-tasks.md)：兼容能力的既有状态与限制
- [测试规范](testing-standard.md)：正式性能与差分门槛
- [edge-agent-go 接入优化计划](../../../zwyb_file/edge-agent-go/docs/fssql-optimization-review.md)：调用方配置、对象处理、预算、观测与 HTTP 回放；此链接依赖当前本机相邻仓库布局

引擎优化任务的主记录仅维护于本文；调用方文档通过 FOPT 编号引用其依赖，不复制引擎实现清单。
