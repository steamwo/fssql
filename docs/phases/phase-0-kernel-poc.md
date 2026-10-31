# Phase 0 — Kernel PoC

Status target: ACCEPTED before Phase 1 becomes stable dependency.

## Goal

证明：
```text
Type System + Vector + Expression + Managed Memory + Push Pipeline + Scheduler + Hash
```
在 Go 中可行。

## Required Implementation

1. `internal/core/types`
   - LogicalType / PhysicalType
   - DECIMAL
   - temporal basics
   - TypeOperations

2. `internal/core/vector`
   - FLAT / CONSTANT / DICTIONARY
   - ValidityMask
   - SelectionVector
   - UnifiedVectorFormat
   - StringRef / StringHeap
   - DataChunk

3. `internal/core/memory`
   - MemoryManager
   - Scope
   - Reservation
   - Managed Buffer
   - BufferManager

4. `internal/execution/expression`
   - arithmetic
   - comparison
   - boolean
   - cast
   - IS NULL
   - CASE basic

5. `internal/execution/pipeline`
   - Source / Operator / Sink
   - Push Pipeline
   - Pipeline DAG

6. `internal/execution/scheduler`
   - Task
   - READY/RUNNING/PARKED/DONE/FAILED
   - deterministic test mode

7. `internal/execution/hash`
   - VectorHash
   - RowLayout
   - TupleDataCollection
   - HashTable primitive
   - RadixPartitioner

## Deliverables

- P0-D1 Kernel library
- P0-D2 10M-row Vector demo
- P0-D3 Memory accounting demo
- P0-D4 Push Pipeline demo
- P0-D5 Hash microbenchmark
- P0-D6 benchmark-phase0.json/md
- P0-D7 verification suite
- P0-D8 architecture dependency test

## Mandatory Tests

- FLAT == CONSTANT/DICTIONARY
- single-worker == multi-worker
- Equals(a,b) => Hash(a)==Hash(b)
- Serialize roundtrip
- allocation failure cleanup
- pipeline cancel cleanup
- race test

## Performance Gate

基础 numeric kernel：
- no per-row heap allocation
- establish benchmark baseline
- quantity-order regression vs DuckDB requires investigation before acceptance

## Memory Gate

Query finish：
```text
query-owned managed memory = 0
```

## Failure Gate

不得存在：
- goroutine leak
- buffer leak
- reservation leak

## Acceptance Output

`artifacts/phase-0/acceptance-report.md`

推荐 milestone：
```text
v0.0-kernel
```
