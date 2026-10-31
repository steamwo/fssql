# Source Introspection Contract

## Stable Public Interfaces

SQL：
```sql
DESCRIBE table_name;
DESCRIBE SELECT * FROM read_parquet(...);
```

Go：
```go
conn.InspectSource(ctx, SourceSpec{...})
```

Server：
```text
POST /v1/sources/inspect
```

Parquet：
```text
parquet_schema(...)
parquet_file_metadata(...)
```

## Internal Rule

必须复用：

```text
FileSetPlanner
→ TableFunction.Bind
→ SchemaResolver
```

禁止独立维护第二套 schema inference。

必须保证：

```text
DESCRIBE schema == actual Query Bind schema
```

## SchemaSource

- EXPLICIT
- CATALOG
- FILE_METADATA
- SAMPLE_INFERRED

## Confidence

- EXACT
- HIGH
- SAMPLED
- UNKNOWN

## Full Validation

全量多文件 schema 校验为 Experimental，不作为普通 DESCRIBE 默认行为。
