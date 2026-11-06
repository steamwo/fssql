# Feature Contract

本文记录当前实现范围，区分冻结的 v1 基线、后续扩展和实验能力。公共 Go 入口以 [Public API](../public-api.md) 为准；内部模块存在不代表外部应用可以配置使用。

“已实现”指有代码及对应测试入口，不等于所有规模、协议和性能门槛均已完成发布验收。验收状态与证据按[测试规范](../testing-standard.md)和[发布契约](release-contract.md)记录；SQL 差异按[差异登记](../../compatibility/deviations.yaml)管理。参考引擎版本为 DuckDB 2.0.0。

## STABLE v1 基线

冻结基线包括 SELECT/FROM/WHERE、GROUP BY/HAVING、ORDER BY/LIMIT/OFFSET、DISTINCT、INNER/LEFT/RIGHT/FULL/CROSS/SEMI/ANTI JOIN、COUNT/SUM/AVG/MIN/MAX、非递归 CTE 和有限非相关子查询。

控制与元数据语句包括 CREATE EXTERNAL TABLE、CREATE VIEW、CREATE SECRET、COPY TO、DESCRIBE 和 information_schema.columns。内建表函数包括 read_parquet、read_csv、read_json、read_ndjson、glob、parquet_schema 和 parquet_file_metadata。

下表补充当前分支扩展的具体边界，不将“完整 DuckDB SQL 兼容”作为笼统能力声明。

## 当前 SQL 能力矩阵

| 能力 | 当前实现范围 | 边界与验证入口 |
| --- | --- | --- |
| 查询与聚合 | 基线算子；无 FROM 聚合使用一条隐式输入行，WHERE FALSE 保留空集聚合语义 | [基础 SQL](../../pkg/gosql/common_sql_test.go)、[数值测试](../../pkg/gosql/numeric_completion_test.go) |
| 精确数值 | 有符号/无符号整数、HUGEINT、DECIMAL；整数溢出检查，DECIMAL 转整数按中点远离零舍入 | [数值边界](../../pkg/gosql/numeric_completion_test.go)、[DECIMAL](../../pkg/gosql/decimal_cast_test.go)；宽整数不通过浮点数中转 |
| 集合与谓词 | IN/NOT IN 列表和单列 SELECT、EXISTS/NOT EXISTS、BETWEEN、Unicode LIKE、UNION ALL | NULL/空集合有用例；UNION 去重、INTERSECT、EXCEPT、量化 ANY/ALL 未实现；[用例](../../pkg/gosql/common_sql_test.go) |
| DISTINCT 聚合 | 单参数 COUNT/SUM/AVG/MIN/MAX(DISTINCT expr) | COUNT/SUM/AVG 可共享相同参数的去重输入；不同参数可能增加排序和临时 I/O；[测试](../../pkg/gosql/subquery_design_test.go) |
| JOIN | 基线连接、USING 合并键及两侧限定列；可哈希键先缩小候选，其他条件保留计算 | 无可用键时用交叉积/过滤，CROSS build 输入驻留受管内存；[连接输出](../../pkg/gosql/join_output_test.go)、[关系树关联](../../pkg/gosql/subquery_relation_test.go) |
| 非相关标量嵌套 | 按内到外依赖顺序准备常量；每层零行 NULL、多行报错 | EXISTS 已裁去的投影依赖不执行；[嵌套和清理](../../pkg/gosql/nested_scalar_test.go) |
| 相关子查询 | 等值快速路径及参数域去相关；非等值/OR、跨层引用、全局聚合/HAVING、派生表/CTE/UNION/JOIN ON | [参数域](../../pkg/gosql/subquery_domain_test.go)、[关系传播](../../pkg/gosql/subquery_relation_test.go) |
| 聚合归属 | 纯外层聚合交回所属查询块计算，再传回子查询；分组和行数独立检查 | 同层嵌套聚合、未分组列仍报错；[归属测试](../../pkg/gosql/subquery_scope_test.go) |
| LATERAL 派生表 | 显式与自动左侧引用；逗号/CROSS/INNER/LEFT、链式引用、局部聚合、UNION ALL、列别名 | JOIN 优先于逗号；实际依赖的 RIGHT/FULL/SEMI/ANTI、归属于左输入的聚合按 DuckDB 拒绝；[测试](../../pkg/gosql/lateral_test.go) |
| 相关分页 | ORDER BY 后按参数域应用常量 LIMIT/OFFSET；DISTINCT 先于分页 | 非常量相关 LIMIT/OFFSET 按 DuckDB 拒绝；[跨批次验证](../../pkg/gosql/subquery_domain_test.go) |
| 采样 | TABLESAMPLE 单输入、USING SAMPLE 整个 FROM；reservoir 行数/百分比、Bernoulli 行采样、system 批采样、种子 | 独立源先采样再关联过滤可用；采样算子需接收关联参数时按 DuckDB 拒绝；[用例](../../pkg/gosql/sql_compatibility_test.go) |
| 窗口 | ROW_NUMBER；COUNT(*)/SUM 的 OVER ()，或显式 ROWS BETWEEN UNBOUNDED PRECEDING AND 1 PRECEDING | COUNT/SUM 无显式 frame 时仅支持 OVER ()；一般 RANGE/GROUPS/frame 未实现；[测试](../../pkg/gosql/window_sampling_test.go) |
| SELECT 参数 | database/sql 的 ?、连续编号 $1/$2、sql.Named 对应 $name，预编译语法复用 | 不混用样式；参数不能替换标识符；DDL/COPY 参数和事务未实现；[测试](../../pkg/gosql/database_sql_statement_test.go) |

相关子查询和 LATERAL 的完整实现说明见[子查询 lowering](subquery-lowering.md)。表函数参数仍按固定参数绑定，不包含按 LATERAL 参数动态调用文件读取器。

## 关联聚合示例

以下查询无需外部数据源，展示 LATERAL 聚合的匹配、空集 COUNT 和 NULL SUM：

```sql
WITH orders AS (
    SELECT 1 AS customer_id, 10 AS amount
    UNION ALL SELECT 1, 20
    UNION ALL SELECT 2, 5
), customers AS (
    SELECT 1 AS id UNION ALL SELECT 2 UNION ALL SELECT 3
)
SELECT c.id, totals.n, totals.total
FROM customers c
LEFT JOIN LATERAL (
    SELECT count(*) AS n, sum(o.amount) AS total
    FROM orders o WHERE o.customer_id = c.id
) totals ON TRUE
ORDER BY c.id
```

结果为 `(1, 2, 30)`、`(2, 1, 5)`、`(3, 0, NULL)`。使用 database/sql 时 SUM 的宽整数结果以精确十进制文本返回，见[类型映射](../public-api.md#row-value-representation)。

## 公共集成、数据源与资源

| 能力 | 公共入口 | 边界 |
| --- | --- | --- |
| 原生 / database/sql | gosql.Open、QueryChunks、Exec；OpenSQLDB | Chunk 为回调借用对象；行式结果通过 Rows.Close/上下文结束消费 |
| 本地文件 / S3 | Config.LocalFiles、FileURI、Config.S3 | 显式配置；本地路径受 RootDir 限制；spill 目录配置不启用本地数据访问 |
| Go 数据源 / UDF | Config.TableFunctions、BatchReader/BatchWriter、Config.ScalarFunctions | 表函数固定 schema、零参数；Go 回调管理自身外部资源和共享状态 |
| 导出 / 加密导出 | SQL COPY 或 DB.CopyTo；CopyOptions、ExportResult、Config.ParquetKeys | CSV/JSON/NDJSON/Parquet；加密为导出接口，不代表有公共通用加密读取密钥注入接口 |
| 类型保真 | 无符号整数、BLOB、HUGEINT/DECIMAL、日期和时间类型 | UBIGINT/HUGEINT/DECIMAL 的行式值使用精确文本；LIST/STRUCT 当前仅公开类型元数据 |
| Catalog / 元数据 | 同一 DB 实例共享 MemoryCatalog；DESCRIBE、parquet_schema、parquet_file_metadata | SQLiteCatalog、InspectSource、HTTP 服务构造器及 SFTP/FTP/WebDAV 为内部实现，无对应公共 Config 注入入口 |
| 内存 / 临时存储 | Config 内存、spill、worker/queue 配置，DB.MemoryUsed | 统计受管内存而非 RSS；不保证所有计划都可任意规模落盘或并行 |

## EXPERIMENTAL 与验收边界

Parquet Page Index 的读取/裁剪、页跳过及早期批过滤已有实现和针对性测试：

- [公开查询过滤](../../pkg/gosql/parquet_filter_test.go)
- [页索引](../../internal/format/parquet/page_index_test.go)、[页跳过](../../internal/format/parquet/page_skip_test.go)
- [索引故障注入](../../test/fault/parquet_page_index_test.go)、[跳页失败路径](../../test/fault/parquet_page_skip_test.go)

这些优化仅对可证明安全的类型、谓词和索引生效；不支持的表达式继续执行残余过滤。不能据此宣称任意文件均会少读、所有投影都可跳页，或大文件物理 I/O 验收已完成。性能与资源缺口继续保留在差异登记中。

ATTACH 自动发现、Parquet Bloom Filter、复杂 JSON 结构并行、多文件完整 schema 校验和深度源诊断仍按实验范围管理。

## DEFERRED

一般窗口函数/frame、递归 CTE、完整 DuckDB 子查询语法与算子覆盖、UPDATE/DELETE/MERGE、原生持久化表、事务、Iceberg/Delta、ORC/Avro、PostgreSQL Wire、原生远程协议、远程 spill、分布式执行和动态二进制扩展 ABI 尚未纳入当前完整支持范围。

## 验证入口

```sh
go test ./examples/quickstart ./pkg/gosql ./test/compat -count=1
```

DuckDB 差分默认可跳过；正式运行需安装与 `test/integration/requirements.txt` 一致的参考依赖，设置 `FSSQL_REQUIRE_DUCKDB=1`，通过 `FSSQL_PYTHON` 指向可用 Python，再执行 `go test ./test/compat -count=1`。未执行的差分、外部服务或大规模性能测试不能记为通过。

固定 SQL 语料见 [stable.slt](../../test/compat/stable.slt)。子查询用例族包括 `SQL.SUBQUERY.DOMAIN`、`RELATION`、`SCOPE`、`LATERAL`、`NESTED`，覆盖结果值、NULL、重复行和已登记边界；发布验收仍需满足测试规范中的其余要求。
