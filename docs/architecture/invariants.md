# Architecture Invariants

任何实现都必须持续满足以下 invariant。

## I-01 Vector
Internal execution data = own Vector/DataChunk.

## I-02 Arrow Boundary
Arrow 不得渗透到 core execution ABI。

## I-03 Push Pipeline
主执行模型必须是 Push Pipeline。

## I-04 Managed Memory
核心大块内存必须可归因到 MemoryManager。

## I-05 No Silent Range Fallback
Range 不可用时不得静默整文件读取并丢弃前缀。

## I-06 Storage Orthogonality
Protocol × Compression × Format 三者正交。

## I-07 Query Snapshot
同一 Statement 的 ScanSet 必须冻结。

## I-08 Spill Ownership
Spill 必须通过 TemporaryStorageManager。

## I-09 Semantic Centralization
Hash/Compare/Group/Cast/NULL/Decimal/Temporal 统一语义。

## I-10 Lifecycle
Query 终态前：
- active task = 0 或 ownership transferred
- query reservation = 0
- query temp ownership = 0
- query lease = 0
- incomplete multipart = 0

## I-11 Introspection Consistency
DESCRIBE / InspectSource 的 schema 必须与实际 Bind schema 一致。

## I-12 No Hidden Semantic Downgrade
资源不足不得自动改成 approximate result。
