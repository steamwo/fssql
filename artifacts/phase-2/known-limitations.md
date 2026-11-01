# Phase 2 Known Limitations

These limitations are deliberate Phase 2 boundaries, not unknown compatibility mismatches.

1. **External Hash Join spill baseline is INNER JOIN.** P2-D1 covers INNER/LEFT/RIGHT/FULL/CROSS/SEMI/ANTI semantics in the in-memory Push Pipeline path. P2-D2/P2-D9 exercise Adaptive/Hybrid External Hash Join and forced-spill equivalence for INNER JOIN. External spill variants for the other join types are not claimed by this phase.
2. **Remote spill is not implemented.** AD-023 freezes v1 temporary storage to local SSD; remote spill remains deferred.
3. **Spill compression is a baseline codec.** Phase 2 uses RLE plus CRC32C checksum to establish the compression/checksum ABI and failure semantics; it is not presented as a general-purpose production compression choice.
4. **HUGEINT SQL surface is intentionally narrow.** ADR-0002 authorizes HUGEINT as the logical result identity required by DuckDB-compatible Stable aggregate results and result propagation. It does not add unrelated HUGEINT literals, DDL, explicit casts, UHUGEINT or BIGNUM syntax.
5. **TopN is bounded-memory rather than an external-spill operator.** It retains at most N rows under an explicit memory budget; full external ordering uses ExternalSort.
6. **Phase 2 operator APIs remain the acceptance surface.** The repository's broader SQL frontend is not expanded merely to expose new Phase 2 operators; this phase does not pull future SQL-surface work forward.
