# ADR-0007 — Stable `CREATE EXTERNAL TABLE` SQL Grammar

Status: **PROPOSED**

Date: 2026-11-02

## Context

ADR-0006 / AD-044 preserves the frozen v1 Stable Feature Contract and reopens Phase 5 to complete already-declared Stable capabilities before Feature Freeze.

`architecture/feature-contract.md` declares `CREATE EXTERNAL TABLE` as Stable v1, but the repository currently does not define its public SQL grammar. This is materially different from other Stable statements in the same remediation:

- `DESCRIBE` has an explicit project contract in `contracts/source-introspection.md` and a DuckDB compatibility target.
- `COPY ... TO` has a DuckDB compatibility target and existing Export architecture.
- `CREATE SECRET` has a DuckDB compatibility target and existing SecretManager/Security architecture.
- `CREATE EXTERNAL TABLE` is a project-specific Stable capability; DuckDB does not provide an equivalent generic statement whose grammar can be adopted without a project decision.

Choosing a grammar affects the Stable public SQL contract. The implementation agent must not invent it silently.

The grammar must also preserve the frozen internal path:

```text
SQL statement
→ table-function call
→ FileSetPlanner
→ TableFunction.Bind
→ SchemaResolver
→ immutable source binding / Catalog metadata
```

It must not introduce a second `FORMAT`/`LOCATION` schema-inference path that bypasses the real table-function Bind, because AD-042 / I-11 require introspection and query binding to share the same schema resolution.

## Option A — `USING <table_function_call>`

Define the v1 Stable syntax as:

```sql
CREATE EXTERNAL TABLE <qualified_name>
USING <stable_read_table_function_call>;
```

Examples:

```sql
CREATE EXTERNAL TABLE events
USING read_parquet('s3://bucket/events/*.parquet');

CREATE EXTERNAL TABLE logs
USING read_ndjson('sftp://host/logs/*.ndjson');
```

For v1, `<stable_read_table_function_call>` is limited to source-reading table functions that produce a relation:

```text
read_parquet
read_csv
read_json
read_ndjson
```

`glob` remains a Stable table function but is not a valid external-table storage binding because its result is a list of paths rather than the schema/data source represented by the external table.

Semantics:

1. The `USING` call is parsed as the same table-function call AST used in `FROM`.
2. Creation executes the real table-function Bind path exactly once for the statement snapshot.
3. The resulting bound schema and canonical source binding are persisted as Catalog metadata.
4. Credentials/tokens are never persisted in Catalog metadata; secret references/scopes remain Security/SecretManager concerns.
5. Querying the catalog object reuses the table-function/source binding contract; it must not independently infer schema.
6. Unsupported/invalid table-function options fail creation rather than being stored as opaque SQL for later reinterpretation.

Properties:

- Makes the external nature explicit without pretending a native persistent table exists.
- Directly composes with AD-020 TableFunction and AD-028 FileSetPlanner.
- Preserves Protocol × Compression × Format orthogonality because protocol remains in the URI and format remains the selected table function/options.
- Avoids a second `LOCATION ... FORMAT ...` binding system.
- Does not add a new Stable feature; it defines the syntax of an already-frozen Stable feature.

## Option B — `AS <table_function_call>`

Define:

```sql
CREATE EXTERNAL TABLE <qualified_name>
AS <stable_read_table_function_call>;
```

Example:

```sql
CREATE EXTERNAL TABLE events AS read_parquet('s3://bucket/events/*.parquet');
```

Properties:

- Compact.
- Still permits reuse of real TableFunction.Bind.
- `AS` is easily confused with CTAS (`CREATE TABLE ... AS SELECT ...`) and suggests materialization/native persistence, which is Deferred in v1.

## Option C — `LOCATION ... FORMAT ...`

Define a storage-style grammar such as:

```sql
CREATE EXTERNAL TABLE events
LOCATION 's3://bucket/events/*.parquet'
FORMAT PARQUET;
```

Properties:

- Familiar from some warehouse systems.
- Introduces a second public binding vocabulary that must map format/options back to TableFunction.Bind.
- Risks duplicating format/schema resolution and weakening AD-042/I-11.
- Requires additional syntax for CSV/JSON options, compression, and future source arguments.

## Decision recommendation

**Recommend Option A.**

Reasoning:

1. It maps one-to-one to the already-frozen TableFunction architecture rather than inventing a parallel source configuration system.
2. It makes clear that the object is external metadata, not a native persistent table.
3. It allows Catalog creation, query binding, `DESCRIBE`, and `InspectSource` to share the same bound source/schema path.
4. It avoids changing the Stable feature list and does not introduce a future-format abstraction prematurely.

## Compatibility policy

`CREATE EXTERNAL TABLE` is an intentional project extension. It must be recorded in the Phase 6 compatibility matrix as an extension rather than a DuckDB mismatch.

All SQL semantics inside the referenced table-function call remain subject to the existing Stable table-function contracts and DuckDB-compatible behavior where applicable.

## Acceptance impact

Until this ADR is resolved:

```text
ADR-0006 remediation work item #6 is blocked only at CREATE EXTERNAL TABLE grammar.
No implementation may choose a Stable public grammar silently.
```

Other statements with an existing DuckDB/project syntax contract may continue to use those contracts, but the dependency-ordered remediation should not claim Stable SQL surface complete until this decision is accepted and implemented.

## Architecture invariants

This proposal changes no execution invariant by itself. Any accepted option must preserve:

- I-05 No Silent Range Fallback
- I-06 Storage Orthogonality
- I-07 Query Snapshot
- I-10 Lifecycle
- I-11 Introspection Consistency

and must not introduce format/protocol implementation types into Catalog, Planner, Query, or Core ABI.
