# Phase 2 Failure / Leak Report

Date: 2026-11-01

The Phase 2 failure matrix is exercised by `scripts/acceptance.sh 2` and package tests.

| Failure | Verification | Required cleanup |
|---|---|---|
| disk full (`ENOSPC`) | `TestDiskFullRollsBackReservation` | temp reservation rollback |
| permission denied | `TestCreatePermissionDeniedHasNoOwnershipLeak`, cleanup retry test | no ownership loss; retry-safe cleanup |
| short write | `TestShortWriteAccountsOnlyWrittenBytes`, `TestSpillablePageStoreShortWritePoisonsStoreAndCleansUp` | only physical bytes accounted; poisoned partial page never visible |
| corrupted spill block | `TestSpillablePageStoreDetectsCorruption` | checksum error; managed read buffers released |
| allocation failure | page-store allocation tests and join/hash managed-allocation tests | managed reservations released |
| cancel during spill | external Join/Aggregate/Sort cancel tests | temp ownership/files and managed state return to zero |
| skew | `TestSkewDetectorSyntheticDataset`, same-key no-progress and external terminal-skew join tests | bounded repartition / terminal fallback |
| oversized aggregate partition | recursive aggregate repartition regression | partition is repartitioned rather than requiring a larger memory budget |
| close with unconsumed spill pages | `TestSpillablePageStoreCloseReclaimsUnconsumedPages` | temp bytes/files and managed metadata return to zero |

All failure paths are designed so partial spill blocks are not published as valid pages. Query-scoped temporary storage retains ownership on cleanup failure so cleanup can be retried instead of silently losing accounting.
