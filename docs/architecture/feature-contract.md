# Feature Contract

## STABLE v1

SQL：
- SELECT / FROM / WHERE
- GROUP BY / HAVING
- ORDER BY / LIMIT / OFFSET
- DISTINCT
- INNER / LEFT / RIGHT / FULL / CROSS / SEMI / ANTI JOIN
- COUNT / SUM / AVG / MIN / MAX
- non-recursive CTE
- limited non-correlated subquery
- CREATE EXTERNAL TABLE
- CREATE VIEW
- CREATE SECRET
- COPY TO
- DESCRIBE
- information_schema.columns

Table Functions：
- read_parquet
- read_csv
- read_json
- read_ndjson
- glob

Introspection：
- DESCRIBE table
- DESCRIBE query
- InspectSource
- parquet_schema
- parquet_file_metadata

## EXPERIMENTAL

- ATTACH auto-discovery
- Parquet Page Index pruning
- Parquet Bloom Filter
- advanced JSON structural parallelism
- full multi-file schema validation
- deep source diagnostics

## DEFERRED

- Complete window-function coverage and general window frames
- Recursive CTE
- full correlated subquery decorrelation
- UPDATE / DELETE / MERGE
- native persistent table storage
- Iceberg / Delta
- ORC / Avro
- PostgreSQL Wire
- Native remote wire protocol
- remote spill
- distributed execution
- dynamic binary extension ABI

## Implemented SQL extensions

- TABLESAMPLE / USING SAMPLE with reservoir, Bernoulli and batch-based system sampling
- random(), explicit numeric CAST and BIGINT modulo
- FROM derived tables and non-correlated scalar subqueries combined with aggregates/HAVING
- database/sql SELECT parameters and reusable parsed statements
- Global aggregate SELECT without FROM, checked integer arithmetic and DECIMAL-to-integer CAST
- IN lists/subqueries, BETWEEN, LIKE, EXISTS, UNION ALL, single-argument aggregate DISTINCT and JOIN USING
- Equality fast paths and parameter-domain correlation for EXISTS/IN/scalars, non-equality predicates, constant per-parameter LIMIT/OFFSET, nested expression references and grouped outer outputs
- Parameter propagation through derived tables/CTEs, UNION ALL and JOINs, subqueries in ON, and outer-owned aggregate calls
- Explicit/implicit LATERAL derived SELECT inputs with INNER/CROSS/LEFT joins, and nested non-correlated scalar dependencies
- Limited window functions: ROW_NUMBER, COUNT(*) and SUM; supported forms are validated explicitly by the parser

These additions do not imply full DuckDB SQL compatibility. Dynamic table-function arguments, quantified ANY/ALL, general window frames and transactions remain unsupported. Correlation pushed through SAMPLE, non-constant correlated row limits and disallowed dependent join types are rejected as in DuckDB; see [subquery lowering](subquery-lowering.md) for the complete scope.
