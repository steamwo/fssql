# ADR-0002 — HUGEINT Logical Type for DuckDB-Compatible Integer Aggregates

Status: ACCEPTED

## Context

Phase 2 requires Hash Aggregate with the Stable aggregate functions `COUNT / SUM / AVG / MIN / MAX`. Stable SQL semantics are frozen to target DuckDB compatibility (AD-025), and the Feature Contract lists these aggregates as STABLE.

The current logical type system contains `TINYINT`, `SMALLINT`, `INTEGER`, `BIGINT`, floating-point types, `DECIMAL`, `VARCHAR`, temporal and nested types, but no `HUGEINT` logical type. The physical type system already contains `PhysicalInt128`, and ADR-0001 defines the canonical `types.Int128` representation only for `DECIMAL(19..38)`. ADR-0001 explicitly does not authorize broader numeric or Phase 2 behavior.

DuckDB binds integer `SUM` to a `HUGEINT` result. In DuckDB's current aggregate implementation, integer sums use a 128-bit accumulator and expose `LogicalType::HUGEINT` as the result type. DuckDB also uses widened DECIMAL result semantics for DECIMAL sums.

Therefore Phase 2 cannot implement Stable DuckDB-compatible integer `SUM` with the currently frozen public logical type set:

- returning `BIGINT` is observably incompatible and can overflow where DuckDB succeeds;
- returning `DECIMAL(38,0)` changes the observable SQL type and is not DuckDB-compatible;
- using `PhysicalInt128` while reporting `BIGINT` or `DECIMAL` would conflate logical and physical types and violate AD-012;
- adding `HUGEINT` directly without an ADR would change the frozen type-system contract.

This is an architecture gap rather than an aggregate-operator-only implementation detail because the result type is visible to Binder, Vector, result encoding, comparison/hash semantics, introspection, serialization and spill.

## Decision

Authorize a `HUGEINT` logical type with the following strictly scoped semantics:

1. Add `types.HugeInt` as a first-class `LogicalTypeID` mapped to the existing `PhysicalInt128` physical representation.
2. Reuse the canonical signed two's-complement `types.Int128` representation and 16-byte little-endian Own Vector storage already approved by ADR-0001. This ADR changes logical identity only; it does not define a new Int128 physical ABI.
3. Extend centralized numeric semantics for `HUGEINT` equality, comparison, hashing and checked arithmetic using `types.Int128`; no `math/big.Int`, per-row heap representation, or silent two's-complement overflow is permitted on Stable SQL paths.
4. Extend Own Vector copy/serialization/materialization paths so `HUGEINT` is type-preserving while continuing to use `PhysicalInt128` managed buffers.
5. Phase 2 Binder/FunctionCatalog aggregate binding must use the following DuckDB-compatible integer SUM result matrix:
   - `SUM(TINYINT) -> HUGEINT`
   - `SUM(SMALLINT) -> HUGEINT`
   - `SUM(INTEGER) -> HUGEINT`
   - `SUM(BIGINT) -> HUGEINT`
   `COUNT`, `MIN`, `MAX`, and `AVG` do not become `HUGEINT` merely because this ADR exists; each retains its DuckDB-compatible binding rule. DECIMAL SUM retains DECIMAL widening semantics.
6. DECIMAL remains a distinct logical type. A `HUGEINT` value must never be represented to SQL/introspection as `DECIMAL(38,0)` merely because both use `PhysicalInt128`.
7. Phase 2 execution must be able to produce and preserve `HUGEINT` aggregate results through Vector/DataChunk, compare/hash, spill serialization, result metadata and introspection. This does not by itself authorize new parser syntax, literals, table DDL, explicit casts, arithmetic operators, `UHUGEINT`, or `BIGNUM` surface area.
8. Any parser syntax, explicit casts, arithmetic operators, literals or external protocol encodings for `HUGEINT` beyond what existing Stable aggregate results require remain outside this ADR and require existing Feature Contract authorization or a separate ADR when architecture-affecting.
9. Stable aggregate behavior remains DuckDB-targeted. Differential acceptance must verify both value and logical result type, including an integer SUM exceeding signed 64-bit range, NULL-only/empty input, partial-state Combine/Finalize equivalence, and in-memory == forced-spill equivalence.

## Alternatives

### A. Return BIGINT from integer SUM

Rejected because it is observably incompatible with DuckDB and introduces overflow for valid DuckDB queries.

### B. Return DECIMAL(38,0)

Rejected because SQL type identity differs from DuckDB and it violates LogicalType/PhysicalType separation.

### C. Keep a hidden aggregate-only Int128 state and downcast on finalize

Rejected for Stable semantics. A hidden Int128 accumulator is valid internally, but downcasting the final result either rejects queries DuckDB accepts or exposes the wrong result type.

### D. Declare integer SUM an intentional compatibility deviation

Rejected for Phase 2 because the phase explicitly establishes Stable OLAP aggregate semantics.

## Consequences

Positive:

- Enables DuckDB-compatible integer SUM value range and observable type.
- Reuses the already canonical `PhysicalInt128`/`types.Int128` ABI rather than creating another 128-bit representation.
- Keeps LogicalType and PhysicalType separation intact.
- Gives Hash Aggregate, spill serialization, result encoding and introspection one canonical type identity.

Negative:

- Expands the logical type enum and all exhaustive type switches.
- Requires compare/hash/copy/serialization/materialization tests.
- Requires result/introspection paths to distinguish `HUGEINT` from high-precision DECIMAL despite shared physical storage.
- Unsupported external encodings must fail explicitly rather than silently coerce.

## Required Tests

At minimum:

- LogicalType -> PhysicalInt128 mapping for HUGEINT.
- Vector SetInt128/Int128, dictionary, copy and serialization preserving HUGEINT logical identity.
- Compare/hash property tests, including negative values and 128-bit boundaries.
- Aggregate differential tests against DuckDB for `SUM(TINYINT/SMALLINT/INTEGER/BIGINT)` values and result types.
- A BIGINT sum exceeding `math.MaxInt64` that succeeds and returns the DuckDB-equivalent HUGEINT value.
- NULL-only and empty-input SUM semantics.
- Combine/finalize equivalence for partial aggregate states.
- Forced-spill aggregate result/type equivalence.
- Result/introspection tests proving HUGEINT is not reported as DECIMAL.

## Affected Frozen Decisions

- AD-012 Type System / Binder: adds one logical numeric type while preserving LogicalType/PhysicalType separation.
- AD-025 SQL Semantics: closes an incompatibility required by Stable DuckDB aggregate behavior.
- AD-031 Go Runtime / Buffer Ownership: HUGEINT payload remains fixed-width managed Vector storage.
- AD-033 Hash / Row Infrastructure: centralized compare/hash infrastructure gains HUGEINT semantics.
- AD-038 Feature Contract: no new aggregate function is added; this supplies the result type required by an existing STABLE aggregate.
- I-01 Own Vector / DataChunk: reuses existing PhysicalInt128 ABI.
- I-04 Managed Memory: 16-byte payload remains BufferManager-managed.
- I-09 Semantic Centralization: HUGEINT compare/hash/cast behavior must be centralized.

## Migration Plan

1. Amend the canonical AD-012 type-system decision to recognize HUGEINT as a logical type backed by PhysicalInt128.
2. Add type/vector/semantic support and exhaustive-switch regression tests.
3. Implement Phase 2 aggregate binding and checked Int128 aggregate state arithmetic.
4. Implement P2-D4 Hash Aggregate and verify in-memory aggregate semantics.
5. Implement P2-D5 spilled aggregate using the same type identity and spill ABI.
6. Run DuckDB differential tests on both values and result types before Phase 2 acceptance.

## Approval

Approved by the project owner on 2026-11-01. Scope is limited to DuckDB-compatible Stable aggregate result semantics and the shared execution support required to preserve those results.