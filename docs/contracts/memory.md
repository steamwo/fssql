# Memory Contract

## Core

```text
MemoryManager
├── Query MemoryScope
├── Reservation
├── BufferAllocator
├── BufferManager
└── SpillCoordinator
```

## Allocation Rule

所有可能无界增长的结构：

```text
Reserve
→ Allocate
→ Resize reservation if needed
→ Release
```

禁止：
```text
allocate first
→ account later
```

## Managed Categories

至少：
- Vector
- StringHeap
- HashTable
- Aggregate
- Sort
- ParquetRange
- ParquetCompressed
- ParquetDecode
- NetworkBuffer
- ReadAhead
- ExportBuffer
- Metadata
- RemoteCache

## Process Memory

`engine_memory_limit != RSS`

必须记录：
- managed peak
- Go heap live
- Go allocated
- RSS
- GC cycles
- temp disk peak
