# 深度对比用例清单

## 1. 既有 Stable 语料（144 条）

| 分区 | 条数 | 内容 |
| --- | ---: | --- |
| 基线 SELECT/表达式/类型 | 45 | 常量、别名、算术、CAST、DECIMAL/HUGEINT/unsigned、NULL、LIKE/BETWEEN/IN、EXISTS、UNION ALL、DISTINCT、JOIN USING 等 |
| `SQL.SUBQUERY.DOMAIN` | 19 | 参数域去相关：非等值/OR、LIMIT/OFFSET、嵌套引用、聚合/HAVING、派生表/CTE/UNION/JOIN |
| `SQL.SUBQUERY.RELATION` | 18 | 关系树中的相关子查询、连接、分页、DISTINCT、集合谓词 |
| `SQL.SUBQUERY.SCOPE` | 20 | 聚合归属、分组/HAVING、外层聚合、空集语义 |
| `SQL.SUBQUERY.LATERAL` | 24 | 显式/隐式 LATERAL、链式引用、聚合、空集、拒绝边界 |
| `SQL.SUBQUERY.NESTED` | 18 | 多层标量依赖、按内到外准备、投影裁剪 |

完整 SQL 见 `test/compat/stable.slt`。

## 2. 新增深度 SQL 用例（88 条）

| ID | 对比内容 |
| --- | --- |
| WINDOW-001 | 分区 `ROW_NUMBER()` |
| WINDOW-002 | 分区 `SUM` 显式 `ROWS BETWEEN UNBOUNDED PRECEDING AND 1 PRECEDING` |
| WINDOW-003 | 全局 `COUNT(*) OVER ()` / `SUM(...) OVER ()` |
| WINDOW-004 | 同一查询多个窗口、不同排序键 |
| WINDOW-005 | 过滤后的全局窗口表达式 |
| SAMPLE-001 | `reservoir(3 ROWS)` 计数 |
| SAMPLE-002 | `reservoir(60 PERCENT)` 计数 |
| SAMPLE-003 | `bernoulli(100 PERCENT)` 计数 |
| SAMPLE-004 | `bernoulli(0 PERCENT)` 计数 |
| SAMPLE-005 | `reservoir(3 ROWS) REPEATABLE(42)` 成员 |
| SAMPLE-006 | `TABLESAMPLE reservoir(2 ROWS)` 成员 |
| JOIN-001 | INNER JOIN |
| JOIN-002 | LEFT JOIN |
| JOIN-003 | RIGHT JOIN |
| JOIN-004 | FULL JOIN |
| JOIN-005 | CROSS JOIN 计数 |
| JOIN-006 | SEMI JOIN |
| JOIN-007 | ANTI JOIN |
| JOIN-008 | FULL JOIN USING 合并键与两侧限定键 |
| JOIN-009 | 多键、重复键 JOIN |
| JOIN-010 | `IS NOT DISTINCT FROM` NULL 安全 JOIN |
| AGG-001 | 全局聚合含 NULL |
| AGG-002 | 空输入聚合（COUNT/SUM/AVG/MIN/MAX） |
| AGG-003 | GROUP BY + HAVING |
| AGG-004 | `COUNT(DISTINCT)` / `SUM(DISTINCT)` |
| AGG-005 | 分组聚合与 SUM 宽整数 |
| AGG-006 | AVG/SUM 结果类型 |
| AGG-007 | HUGEINT SUM |
| FUNC-001 | `length`（Unicode/空串） |
| FUNC-002 | `substr`（Unicode） |
| FUNC-003 | `strpos` |
| FUNC-004 | `replace` |
| FUNC-005 | `repeat` |
| FUNC-006 | `lower` |
| FUNC-007 | `hex(VARCHAR)` |
| FUNC-008 | `md5` |
| FUNC-009 | `floor` 正负值 |
| FUNC-010 | `mod` 正负操作数 |
| FUNC-011 | `least` / `greatest` |
| FUNC-012 | `coalesce` / `nullif` |
| FUNC-013 | `||` 与 NULL 传播 |
| FUNC-014 | `strftime` 微秒格式 |
| FUNC-015 | `strptime` 往返 |
| FUNC-016 | `date_trunc('month')` |
| FUNC-017 | `timezone('UTC', TIMESTAMP)`（会话 UTC） |
| FUNC-018 | `substr` 起点 0、负起点、越界长度 |
| FUNC-019 | `repeat('')`、非重叠 `replace` |
| FUNC-020 | emoji `length`、`hex(BLOB)` |
| FUNC-021 | 闰日 `strftime`、epoch 前后 `date_trunc` |
| TYPE-001 | DECIMAL→整数，中点远离零舍入 |
| TYPE-002 | HUGEINT/UBIGINT 最大边界 |
| TYPE-003 | HUGEINT→BIGINT/DECIMAL |
| TYPE-004 | 有符号/无符号宽度转换 |
| TYPE-004B | BOOLEAN→INTEGER/BIGINT/UTINYINT |
| TYPE-004C | 数值（含负数/浮点）→BOOLEAN |
| TYPE-005 | DATE/TIMESTAMP→VARCHAR |
| TYPE-006 | BLOB `\xHH` 字面量 |
| PRED-001 | IN/NOT IN + NULL 三值逻辑 |
| PRED-002 | BETWEEN / NOT BETWEEN + NULL |
| PRED-003 | LIKE / ESCAPE / Unicode |
| PRED-004 | IS [NOT] DISTINCT FROM |
| PRED-005 | UNION ALL 保留重复并排序 |
| PRED-006 | CTE + UNION ALL |
| PRED-007 | `NULL AND TRUE` / `NULL OR FALSE` / `NOT NULL` |
| PRED-008 | LIKE 空串、emoji 与 `_` |
| SUB-001 | 非相关标量子查询 |
| SUB-002 | 相关 EXISTS |
| SUB-003 | 相关标量 `count(*)` |
| SUB-004 | LEFT JOIN LATERAL 相关派生表 |
| SUB-005 | CTE 重复引用 JOIN |
| SUB-006 | 相关 IN + OR 谓词 |
| SUB-007 | 相关 ORDER BY + LIMIT 1 |
| SUB-008 | LATERAL 空聚合（COUNT=0、SUM=NULL） |
| BATCH-001 | 4097 行 count |
| BATCH-002 | 跨批次分区 ROW_NUMBER 边界（0/1/2047/2048/2049/4096） |
| BATCH-003 | 跨批次 GROUP BY |
| BATCH-004 | 跨批次 ORDER BY + LIMIT/OFFSET |
| BATCH-005 | 跨批次 DISTINCT |
| BATCH-006 | 跨批次 reservoir 采样 |
| BATCH-007 | 跨批次窗口 SUM frame 边界 |
| NUM-001 | 整数乘/除与浮点除 |
| NUM-002 | 负数 floor/mod |
| ERR-001 | UTINYINT 溢出 |
| ERR-002 | 非法 INTEGER 文本 |
| ERR-003 | BIGINT 除零 |
| ERR-004 | HUGEINT 溢出 |
| ERR-005 | BLOB 不足两位的 `\x` 转义 |
| ERR-006 | BLOB 裸反斜杠转义 |

## 3. 本地文件双向互操作

| 方向 | 格式 | 内容 |
| --- | --- | --- |
| fssql → DuckDB | CSV | header、引号、换行、Unicode、NULL |
| fssql → DuckDB | JSON | JSON 数组 |
| fssql → DuckDB | NDJSON | 逐行 JSON |
| fssql → DuckDB | Parquet | 普通列 + snappy |
| DuckDB → fssql | CSV | header、引号、换行、Unicode、NULL |
| DuckDB → fssql | NDJSON | 逐行 JSON |
| DuckDB → fssql | Parquet | 普通列 + snappy |
| fssql → DuckDB | Parquet typed | DECIMAL(18,2)、DECIMAL(38,6)、UBIGINT、TIMESTAMP_NS、BLOB |
| DuckDB → fssql | Parquet typed | 同上 |

两侧读回值同时与同一份 typed fixture 的规范值比较。

## 4. 真实 MinIO / S3（12 条查询 + 9 个写入对象）

测试对象位于每次运行唯一的 `s3://<bucket>/deep-<timestamp>/` 前缀，结束后用 boto3 删除。

| ID / 对象 | 方向 | 内容 |
| --- | --- | --- |
| fssql 写 6 个对象 | fssql → S3 | simple.csv、simple.ndjson、typed.parquet、parts/part1-2.parquet、batch.parquet |
| DuckDB 写 3 个对象 | DuckDB → S3 | simple.csv、simple.ndjson、typed.parquet |
| `fssql-csv` | fssql 写 → 两侧读 | S3 CSV 显式 schema、7 列、含换行/Unicode/NULL |
| `fssql-ndjson` | fssql 写 → 两侧读 | S3 NDJSON 逐行读取 |
| `fssql-typed-parquet` | fssql 写 → 两侧读 | S3 Parquet 精确类型（DECIMAL/UBIGINT/TIMESTAMP_NS/BLOB） |
| `fssql-multi-file` | fssql 写 → 两侧读 | `read_parquet('s3://.../parts/*.parquet')` 多文件 |
| `duckdb-csv` | DuckDB 写 → 两侧读 | DuckDB 生成的 S3 CSV |
| `duckdb-ndjson` | DuckDB 写 → 两侧读 | DuckDB 生成的 S3 NDJSON |
| `duckdb-typed-parquet` | DuckDB 写 → 两侧读 | DuckDB 生成的 S3 typed Parquet |
| `batch-aggregate` | 同一 S3 对象 | 4097 行 Parquet 的 COUNT/SUM(id)/SUM(amount) |
| `batch-filter` | 同一 S3 对象 | id 2048..2051 的范围过滤 |
| `batch-group` | 同一 S3 对象 | 5 个分组 COUNT/SUM |
| `batch-topn` | 同一 S3 对象 | `ORDER BY id DESC LIMIT 5` |
| `missing-object` | 两侧错误 | 缺失 S3 对象必须都失败 |

## 5. 结果

- 既有语料 144/144 PASS；
- 深度 SQL 88/88 PASS；
- 本地文件双向互操作 PASS；
- typed Parquet 双向互操作 PASS；
- MinIO S3 12/12 查询 PASS，9 个写入对象 PASS；
- 发现并修复 `substr` 边界、BOOLEAN↔数值 CAST、VARCHAR→BLOB `\xHH` 三处语义差异。
