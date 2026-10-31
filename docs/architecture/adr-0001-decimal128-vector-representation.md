# ADR-0001 — DECIMAL128 Own Vector Representation for Phase 1 Parquet

Status: PROPOSED

## Context

Phase 1 requires Parquet DECIMAL correctness. The frozen type model accepts `DECIMAL(width, scale)` up to width 38 and maps width greater than 18 to `PhysicalInt128`. The Parquet schema layer already accepts values such as `DECIMAL(20,4)`.

The current Own Vector implementation allocates 16 bytes for `PhysicalInt128` through `LogicalType.FixedSize`, but exposes no signed 128-bit value type, no `SetInt128`/`Int128` accessors, and no serialization/copy contract for 128-bit decimals. Parquet PLAIN decode also has no `FIXED_LEN_BYTE_ARRAY` path. Therefore a conforming Parquet `DECIMAL(20,4)` cannot be decoded into the frozen Own Vector model without defining new core representation semantics.

This is an architecture-level gap rather than a Parquet-only decoder omission because dictionary copy, vector serialization, equality/comparison, expression execution, and future spill all need one canonical 128-bit representation.

## Decision

Proposed decision:

1. Add a canonical signed 128-bit core value type, `types.Int128`, represented as two's-complement 128-bit integer with explicit high/low 64-bit limbs.
2. Define Own Vector `PhysicalInt128` storage as exactly 16 bytes per value, little-endian limb layout: low unsigned 64 bits followed by high signed 64 bits.
3. Add Vector accessors `SetInt128(i, types.Int128)` and `Int128(i) (types.Int128, bool)`; all dictionary/copy/serialization code must use these accessors rather than reinterpret raw byte slices outside the vector package.
4. Keep DECIMAL widths 1..18 on the existing signed Int64 physical path. DECIMAL widths 19..38 use `PhysicalInt128`.
5. Parquet DECIMAL decode must accept standard physical encodings permitted by Parquet:
   - INT32 and INT64 where precision permits;
   - BYTE_ARRAY and FIXED_LEN_BYTE_ARRAY encoded as big-endian two's-complement unscaled integers.
   The reader must sign-extend into Int64 or Int128 and preserve the schema scale; it must reject overflow rather than truncate.
6. Timestamp storage remains unchanged: internal Timestamp is signed microseconds since Unix epoch. Parquet timestamp unit conversion, where required by metadata, happens at decode boundaries and must be overflow-checked.
7. This ADR does not expand Phase 1 scope beyond DECIMAL correctness; arithmetic beyond operations already required by Phase 1 remains governed by existing phase boundaries.

Implementation MUST remain managed by MemoryManager/BufferManager and preserve Own Vector/DataChunk ownership and lifecycle invariants.

## Alternatives

1. Limit Phase 1 DECIMAL to precision <= 18.
   - Rejected as the default proposal because the frozen logical type contract explicitly accepts width up to 38 and the current Parquet schema tests accept precision 20. Adopting this alternative would require changing the Phase 1 gate and/or type contract explicitly.

2. Store DECIMAL(19..38) as VARCHAR or raw 16-byte blobs.
   - Rejected because it changes logical semantics, breaks type-preserving bind/introspection, and leaks format-specific representation into execution.

3. Use `math/big.Int` inside each Vector value.
   - Rejected because it introduces per-value heap objects, conflicts with fixed-width vector layout, complicates managed-memory accounting, and is unsuitable for the vectorized execution contract.

4. Decode only Parquet FIXED_LEN_BYTE_ARRAY values that fit Int64 despite declared precision > 18.
   - Rejected because runtime representation would depend on observed values rather than declared logical type and could silently fail on later pages/files.

## Consequences

Positive:
- Restores consistency between logical DECIMAL(1..38), `PhysicalInt128`, and Own Vector storage.
- Provides one representation for Parquet decode, dictionary copy, serialization, expression execution, and later spill.
- Allows Phase 1 interop corpus to include common high-precision Parquet decimals without type degradation.

Negative:
- Requires core vector API and serialization changes before Parquet high-precision DECIMAL can be completed.
- Requires new unit/race/property tests for signed boundaries, sign extension, overflow, dictionary copy, and round-trip serialization.
- Any existing code that assumes all DECIMAL values are readable through `Int64` must be updated to branch on physical width.

## Compatibility Impact

No SQL logical type change is proposed. `DECIMAL(p,s)` remains the public type. The change is an internal physical representation completion for widths 19..38.

Binary vector serialization produced before this decision has no defined Decimal128 payload because the accessor/serialization contract does not yet exist. A format version marker or explicit type-aware 16-byte encoding must be used if serialized vectors are persisted across versions.

## Migration

After approval:

1. Add `types.Int128` helpers and boundary tests.
2. Add Vector Int128 accessors and serialization/copy tests.
3. Update dictionary-copy utilities to support Decimal64 and Decimal128 distinctly.
4. Extend Parquet PLAIN/dictionary decode for INT32/INT64/BYTE_ARRAY/FIXED_LEN_BYTE_ARRAY DECIMAL with checked sign extension.
5. Add DECIMAL corpus cases at widths <=18 and >18, including negative values and precision boundaries.
6. Re-run Phase 1 ordinary/race/leak/differential/interop gates.

No migration code should be merged into execution paths while this ADR remains PROPOSED.

## Affected Frozen Decisions

- I-01 Own Vector / DataChunk: defines the missing canonical representation for existing `PhysicalInt128`; does not replace Own Vector.
- I-04 Managed Memory: 16-byte vector storage remains BufferManager-managed.
- I-10 Lifecycle Protocol: no lifecycle change; new storage follows existing Vector ownership/release rules.
- AD-007 Controlled Parquet Engine: enables controlled DECIMAL decode rather than delegating to a third-party engine.
- Phase 1 correctness gate: resolves the currently unspecified high-precision DECIMAL execution representation.

## Approval

Pending. No DECIMAL128 execution changes are authorized by this document until it is explicitly approved and incorporated into the frozen architecture/decision set.
