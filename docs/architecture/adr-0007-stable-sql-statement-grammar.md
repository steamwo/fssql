# ADR-0007 — Stable v1 SQL Statement Grammar and Security Mapping

Status: **ACCEPTED**

Date: 2026-11-02

## Decision

Option A is accepted.

The v1 Stable feature list remains unchanged. This ADR freezes the minimum public grammar required to implement the already-declared Stable statements, preserves DuckDB-compatible shapes where equivalent statements exist, and makes the project-specific Catalog/Security mappings explicit.

## Context

ADR-0006 / AD-044 preserves the frozen v1 Stable Feature Contract and requires the missing Stable SQL surface to be implemented before Phase 6 Feature Freeze.

The current Stable contract names the following statements but does not define their public grammar:

```text
CREATE EXTERNAL TABLE
CREATE VIEW
CREATE SECRET
COPY TO
DESCRIBE
```

The repository currently has no SQL grammar contract for these statements. The existing parser only accepts SELECT/WITH. This is not merely an implementation detail: SQL grammar is a public Stable interface and must not be invented by an implementation PR after Architecture Freeze.

AD-025 says Stable SQL targets DuckDB semantic compatibility. Current DuckDB documentation provides public syntax for `CREATE SECRET`, `COPY ... TO`, `CREATE VIEW`, and `DESCRIBE table/query`. However, the project's Stable `CREATE EXTERNAL TABLE` capability is a project-specific catalog abstraction and cannot be derived from a DuckDB statement contract.

There is also a security mismatch that requires an explicit mapping rather than syntax copying:

- the existing `SecretManager` requires an ACL scope with authenticated subjects and optional scheme/host restrictions;
- DuckDB's secret scope model is path/prefix-oriented and does not define this project's authenticated-subject ACL;
- silently defaulting a SQL-created secret to wildcard subjects would weaken AD-015;
- silently inventing a caller identity or persistence behavior would create hidden public semantics.

The existing implementation boundaries relevant to this decision are:

- `catalog.ExternalTable`: object name, bound columns, stable table-function name, canonical source;
- `FunctionCatalog`: Stable `read_parquet`, `read_csv`, `read_json`, `read_ndjson`, `glob` names, while real schema Bind remains adapter-owned;
- AD-042 / I-11: introspection and catalog schema must reuse the actual table-function Bind/schema-resolution path;
- `SecretManager`: in-memory scoped secrets with explicit Subjects/Schemes/Hosts ACL and no persistent-secret contract;
- AD-009 Export: `COPY TO` must lower to existing ExportCoordinator/Manifest/Writer/Committer boundaries rather than define a second writer path.

## Accepted Option A — Minimal v1 grammar, DuckDB-compatible where an equivalent exists

No `OR REPLACE`, `IF NOT EXISTS`, persistent secret, arbitrary secret provider, generic DDL option, or additional statement is implied unless explicitly listed below.

### A.1 CREATE VIEW

Stable v1 grammar:

```sql
CREATE VIEW <qualified_name> AS <select_statement>
```

Semantics:

- query is parsed/bound using the same query Binder used for execution;
- Catalog stores the original SQL plus the bound output schema;
- view creation is a Catalog Transaction;
- recursive/correlated semantics outside the Stable query contract remain unsupported.

### A.2 CREATE EXTERNAL TABLE

Stable v1 project grammar:

```sql
CREATE EXTERNAL TABLE <qualified_name>
USING <stable_table_function>(<string_literal>)
```

Examples:

```sql
CREATE EXTERNAL TABLE events
USING read_parquet('s3://bucket/events/*.parquet');

CREATE EXTERNAL TABLE logs
USING read_ndjson('https://example.test/logs/*.ndjson');
```

Semantics:

- `<stable_table_function>` must resolve to a Stable Table Function in FunctionCatalog;
- v1 requires exactly one source argument because the current frozen FunctionCatalog v1 arity is one for these readers;
- table schema is **not** independently specified in the DDL grammar;
- creation must execute the real FileSetPlanner → TableFunction.Bind → SchemaResolver path and persist the resulting bound schema in Catalog;
- the Catalog stores the canonical source and stable table-function identity, never credential material;
- source snapshot identity used by later statements remains statement/query-owned, not Catalog Transaction-owned;
- `glob()` is a Stable table function but is not valid as an external table data reader unless a real schema-producing TableFunction.Bind adapter exists for it.

This grammar intentionally avoids an inline column list because a second DDL schema declaration could diverge from actual Bind and violate I-11 unless a separate explicit validation contract were defined.

### A.3 CREATE SECRET

Stable v1 grammar:

```sql
CREATE SECRET <name> (
    TYPE <identifier>,
    <credential_key> <string_literal>
    [, <credential_key> <string_literal> ...]
    [, SUBJECT <string_literal> ...]
    [, SCHEME <string_literal> ...]
    [, HOST <string_literal> ...]
)
```

Binding/security rules:

- `TYPE` is required and is metadata used by the secret adapter; it is not automatically a credential value;
- credential keys are normalized identifiers and values are string literals;
- at least one `SUBJECT` is required in v1; there is no implicit wildcard ACL;
- zero or more `SCHEME` and `HOST` entries map directly to `SecretScope` restrictions;
- `SUBJECT '*'` is allowed only when explicitly written and remains subject to server/native authorization policy;
- SQL-created secrets are temporary/in-memory only because persistent secret storage is not part of the frozen v1 contract;
- `CREATE PERSISTENT SECRET`, provider chains, environment discovery, and secret-directory semantics are not Stable v1;
- SQL text containing credentials must never be emitted to audit/profile/error payloads beyond redacted statement metadata;
- credential values must be copied into SecretManager and must not be stored in Catalog.

The option-list form intentionally resembles DuckDB's `CREATE SECRET (...)` surface, but the ACL fields are project-specific because AD-015 requires explicit subject/endpoint authorization.

### A.4 COPY TO

Stable v1 grammar:

```sql
COPY <qualified_name> TO <string_literal> [ ( <copy_option> [, ...] ) ]

COPY ( <select_statement> ) TO <string_literal> [ ( <copy_option> [, ...] ) ]
```

Rules:

- only `TO` is Stable v1; `COPY FROM` is not implied;
- source may be a catalog relation/view or a Stable SELECT query;
- destination is a URI/path string literal resolved through the existing VFS/security boundary;
- supported option names and values are limited to options that can be mapped without semantic loss to existing Export planner/writer contracts;
- unknown options fail binding; they never silently downgrade;
- COPY execution must use the existing ExportCoordinator/Manifest/WriterPool/Committer and ADR-0003/ADR-0004 recovery semantics;
- SQL grammar does not expose protocol-specific multipart/recovery tokens.

The exact option registry is an implementation metadata table over existing export capabilities; adding a new option that changes public semantics after v1 requires the normal Stable compatibility process.

### A.5 DESCRIBE

Stable v1 grammar:

```sql
DESCRIBE <qualified_name>
DESCRIBE <select_statement>
```

Rules:

- relation form uses the normal Catalog/query Binder schema path;
- query form binds the query without executing it;
- source/table-function introspection continues to use AD-042's real Bind/schema-resolution path;
- `SHOW`, `DESC`, and `DESCRIBE TABLE '<uri>'` aliases/extensions are not implied by Stable v1 unless separately classified.

### A.6 Parser and Binder behavior

- all statement AST nodes retain source spans under AD-004;
- Parser only constructs syntax AST; it does not call Catalog, SecretManager, Export, VFS, or format adapters;
- Binder converts statement AST to typed/control-plane bound statements and validates Stable function/options;
- execution adapters call existing subsystems; they do not duplicate Catalog/Security/Export state machines;
- unsupported syntax/options fail explicitly under I-12.

## Rejected Option B — Copy DuckDB statement grammar wholesale

Rejected because:

- DuckDB does not define this project's external-table Catalog abstraction;
- DuckDB persistent-secret/provider/scope semantics do not match the existing SecretManager ACL model;
- wholesale COPY options would promise formats/options not necessarily implemented by the current Export contracts;
- it would expand the frozen Stable surface beyond `feature-contract.md` rather than merely complete it.

## Rejected Option C — Remove or defer the underspecified Stable statements

Rejected because ADR-0006 / AD-044 explicitly chose to preserve the existing v1 Stable contract rather than narrow it.

## Implementation order

1. Extend lexer/token set and Own AST for the five Stable statement families.
2. Extend PEG-style statement parser; Pratt remains expression-only.
3. Add bound statement IR and statement Binder validation.
4. Wire CREATE VIEW/EXTERNAL TABLE to Catalog Transaction.
5. Wire CREATE EXTERNAL TABLE to real TableFunction.Bind/schema resolution; no second inference path.
6. Wire CREATE SECRET to SecretManager with explicit ACL fields and redaction tests.
7. Wire COPY TO to existing Export coordinator/manifest path and a bounded Stable copy-option registry.
8. Wire DESCRIBE to actual Binder/schema path.
9. Add parser/binder/security/export failure tests and DuckDB differential cases for equivalent grammar/semantics.

## Acceptance impact

With Option A accepted, ADR-0006 remediation work item #6 may continue under this frozen grammar. Phase 5 remains `IN_PROGRESS` and Phase 6 remains `LOCKED` until remediation and release-precheck gates complete.

## Architecture invariants

This decision changes no runtime invariant by itself. Its implementation must preserve I-01 through I-12, especially:

- I-07: Statement SourceSnapshot remains immutable once bound;
- I-09: SQL semantics are centralized in Binder/FunctionCatalog rather than per adapter;
- I-10: Catalog/Secret/Export ownership must be released before terminal state;
- I-11: external-table and DESCRIBE schema reuse the real Bind path;
- I-12: unsupported syntax/options fail explicitly rather than silently downgrade.
