# 2026-11-06 深度 DuckDB 差分验证

## 环境

| 项目 | 值 |
| --- | --- |
| fssql 分支 | `remediation/mem-001` |
| 参考引擎 | DuckDB 2.0.0（Python 绑定） |
| Go | go1.24.2 windows/amd64 |
| Python | 3.14.5 |
| OS | Windows 10 Pro 10.0.19045 |
| 时区 | 参考会话固定 UTC；fssql 语义按 UTC 验证 |
| MinIO | 本机 WSL Docker 容器 `docker-minio-1`，`http://127.0.0.1:9000`，path-style + HTTP |

逐项用例清单见 [cases.md](cases.md)；逐用例两侧值与错误记录见 [evidence.jsonl](evidence.jsonl)（262 条，0 mismatch）。

## 执行命令

```sh
FSSQL_REQUIRE_DUCKDB=1 FSSQL_PYTHON=python go test -count=1 -v ./test/compat \
  -run 'TestDuckDB(DifferentialCorpus|DeepDifferential|FileInterop|ParquetTypeInterop)$'
```

MinIO 需要额外注入 `FSSQL_MINIO_ENDPOINT`、`FSSQL_MINIO_BUCKET`、`FSSQL_MINIO_ACCESS_KEY`、`FSSQL_MINIO_SECRET_KEY`，然后运行：

```sh
FSSQL_REQUIRE_DUCKDB=1 FSSQL_PYTHON=python go test -count=1 -v ./test/compat \
  -run '^TestDuckDBMinIODifferential$'
```

## 结果摘要

| 套件 | 范围 | 结果 |
| --- | --- | --- |
| `TestDuckDBDifferentialCorpus` | `stable.slt` 144 条既有差分语料 | PASS |
| `TestDuckDBDeepDifferential` | 88 条新增深度用例 | PASS |
| `TestDuckDBFileInterop` | CSV/JSON/NDJSON/Parquet 双向互操作 | PASS |
| `TestDuckDBParquetTypeInterop` | DECIMAL/UBIGINT/TIMESTAMP_NS/BLOB 双向 Parquet | PASS |
| `TestDuckDBMinIODifferential` | 真实 MinIO 双向 CSV/NDJSON/Parquet + S3 扫描/过滤/聚合/多文件/缺失对象 | PASS |

深度 SQL 用例覆盖：

- 窗口：分区 `ROW_NUMBER`、显式 `ROWS` frame、全局 `COUNT/SUM OVER ()`、多窗口排序隔离；
- 采样：reservoir 行数/百分比、bernoulli 0/100%、`TABLESAMPLE`、`REPEATABLE` 成员；
- 连接：INNER/LEFT/RIGHT/FULL/CROSS/SEMI/ANTI、多键重复键、NULL 键、`IS NOT DISTINCT FROM`、`USING` 合并键；
- 聚合：NULL/空输入、分组/HAVING、DISTINCT 聚合、HUGEINT SUM；
- 函数：length/substr/strpos/replace/repeat/lower/hex/md5/floor/mod/least/greatest/coalesce/nullif/`||`、strftime/strptime/date_trunc/timezone；
- 类型：DECIMAL 舍入、HUGEINT/UBIGINT、DATE/TIMESTAMP、BOOLEAN↔数值、BLOB `\xHH` 字面量；
- 谓词：三值逻辑、BETWEEN、Unicode LIKE/ESCAPE、UNION ALL、CTE 重复引用；
- 子查询：标量、EXISTS、IN、相关 OR、相关 LIMIT、LATERAL 空聚合；
- 跨批次：4097 行（2 × 2048 + 1）上的窗口、聚合、排序、DISTINCT 与采样；
- 错误：UTINYINT 溢出、非法整数文本、除零、HUGEINT 溢出、非法 BLOB 转义，两侧都必须失败。

文件互操作覆盖：

- fssql 导出 CSV/JSON/NDJSON/Parquet → DuckDB 读回；
- DuckDB 导出 CSV/NDJSON/Parquet → fssql 读回；
- 两侧读回值同时与同一份 typed fixture 的规范值比较；
- typed Parquet 额外覆盖 `DECIMAL(18,2)`、`DECIMAL(38,6)`、`UBIGINT`、`TIMESTAMP_NS`、`BLOB`，投影为精确文本/十六进制后比较。

MinIO / S3 覆盖：

- fssql 写 6 个 S3 对象（CSV/NDJSON/typed Parquet/两个 part Parquet/4097 行 Parquet），DuckDB httpfs 与 fssql 分别读回；
- DuckDB 写 3 个 S3 对象（CSV/NDJSON/typed Parquet），fssql 读回；
- 同一份 S3 Parquet 上执行 COUNT/SUM、范围过滤、分组聚合、TopN；
- `read_parquet('s3://.../parts/*.parquet')` 多文件扫描；
- 缺失对象两侧都必须失败；
- 所有对象位于唯一 `deep-<timestamp>` 前缀，结束后通过 boto3 清理。

## 本轮发现与修复

1. `substr` 的 0/负起点语义与 DuckDB 不一致。
   - 原实现 `substr('abc',0,2)` 返回 `ab`；DuckDB 返回 `a`。
   - 已按 DuckDB 语义修复：起点 0 表示第一个字符之前，负起点从末尾倒数，越过开头的长度自然截短。
   - 回归测试：`TestSubstrDuckDBBoundaries`。
2. `BOOLEAN` 与数值互转未实现。
   - `CAST(TRUE AS INTEGER)`、`CAST(1 AS BOOLEAN)` 等明确报错。
   - 已补齐：true=1、false=0；数值 0 为 false，非 0（含负数、浮点、DECIMAL）为 true。
   - 回归测试：`TestBooleanNumericCastDuckDBSemantics`。
3. VARCHAR→BLOB 未解码 `\xHH` 转义。
   - 原实现把 `CAST('A\x00\xFF' AS BLOB)` 当成字面文本；DuckDB 解码为字节 `41 00 FF`。
   - 已按 DuckDB 语义只解码 `\xHH`，裸反斜杠、`\n`、不足两位十六进制均报错。
   - 回归测试：`TestBlobLiteralDuckDBSemantics`。

## 未纳入或仍开放的边界

- `HUGEINT` Parquet 导出仍被明确拒绝（`HUGEINT Parquet mapping is not defined without changing logical identity`）；本轮 typed Parquet 用 `DECIMAL(38,6)` 覆盖宽精确数值。
- DuckDB 2.0 的 `COPY ... (FORMAT JSON)` 实际输出逐行 NDJSON；反向互操作使用 NDJSON 对应 fssql 的 `read_ndjson`。
- DuckDB 不支持 `length(BLOB)`，该投影未放入跨引擎用例；fssql 自身的 `length(BLOB)` 由单元测试覆盖。
- 早期大 `UNION ALL` 线性链呈明显非线性：约 100 项 12ms、500 项 345ms、1000 项 1.7s、2049 项 12.3s、4097 项 76.6s。后续 CPU profile 将主要热点定位到执行阶段嵌套 Sink 的所有权查询及重复中间投影；同 schema 的纯透传 UNION 子树已改为平衡规划，4097 项端到端基准降至约 93ms，见 [性能修复记录](union-all-performance.md)。跨批次差分用例仍使用 `batch_rows()`，将批次语义与查询规模性能分别验证。
- 本轮为单机 Windows、本地内存/文件；未覆盖 S3、MinIO、加密 Parquet 实际互读和大规模性能门槛。

## 逐用例证据

`evidence.jsonl` 每行包含：`suite`、`case`、`sql`、`fssql_values`、`duckdb_values`、`fssql_columns`、`duckdb_columns`、`fssql_error`、`duckdb_error`、`match`。本次运行统计：

| suite | 记录数 | match |
| --- | ---: | ---: |
| `stable-slt` | 144 | 144 |
| `deep-sql` | 88 | 88 |
| `file-interop` | 7 | 7 |
| `parquet-types` | 2 | 2 |
| `minio-s3` | 21（9 写入 + 12 查询） | 21 |
| 合计 | 262 | 262 |

生成方式：

```sh
FSSQL_DIFF_EVIDENCE=<path>/evidence.jsonl FSSQL_DIFF_VERBOSE=1 \
FSSQL_REQUIRE_DUCKDB=1 FSSQL_PYTHON=python \
go test -count=1 -v ./test/compat -run 'TestDuckDB(DifferentialCorpus|DeepDifferential|FileInterop|ParquetTypeInterop|MinIODifferential)$'
```

`FSSQL_DIFF_VERBOSE=1` 会在测试日志中逐用例打印 `fssql = [...]` 与 `duckdb = [...]`。

## 复现与清理

所有用例通过 `go test` 运行，临时文件位于系统临时目录并在测试结束时清理。参考引擎版本由 `compatibility/deviations.yaml` 固定为 2.0.0；runner 会拒绝版本不匹配。
