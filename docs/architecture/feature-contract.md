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

- Window Functions
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
