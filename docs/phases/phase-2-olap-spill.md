# Phase 2 — Join / Aggregate / Sort / Spill

## Goal

证明：
```text
dataset > memory_limit
```
时仍可正确执行 OLAP。

## Required Implementation

- HashJoin
- HashAggregate
- DISTINCT
- ExternalSort
- TopN
- AggregateLayout
- SkewDetector
- TemporaryMemoryManager
- TemporaryStorageManager
- Spillable Page Store
- temp quota
- spill compression/checksum baseline

## Hash Join

```text
Adaptive / Hybrid External Hash Join
```

- build side by estimated bytes
- resident/spilled partitions
- recursive repartition
- max depth
- skew handling

## Aggregate

```text
local preaggregate
→ partition/spill
→ Combine
→ Finalize
```

## Sort

```text
sorted runs
→ spill
→ K-way merge
```

## Deliverables

- P2-D1 Join type suite
- P2-D2 External Hash Join demo
- P2-D3 skew dataset
- P2-D4 Hash Aggregate
- P2-D5 spilled aggregate
- P2-D6 DISTINCT
- P2-D7 External Sort
- P2-D8 TopN
- P2-D9 Forced-Spill Verification
- P2-D10 Temp failure suite

## Core Verification

同一 query：

```text
large memory result
==
forced-spill result
```

必须覆盖：
- Join
- Aggregate
- Sort
- DISTINCT

## Failure Gate

- disk full
- permission denied
- short write
- corrupted spill block
- allocation failure
- cancel during spill
- skew

Query failure 后：
```text
temp ownership = 0
reservation = 0
tasks = 0
```

## Acceptance

`artifacts/phase-2/acceptance-report.md`

milestone：
```text
v0.2-olap
```
