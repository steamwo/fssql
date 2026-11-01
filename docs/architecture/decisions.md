# Canonical Architecture Decision Registry

本文件是项目最高优先级的架构事实源。

状态含义：

- `FROZEN`：普通开发不得修改。
- `ACCEPTED`：当前版本正式采用。
- `EXPERIMENTAL`：可实现但不构成 Stable Contract。
- `DEFERRED`：v1 不实现。

---

## AD-001 Internal Vector ABI
Status: FROZEN

- 内部执行使用自研 DuckDB-style Vector/DataChunk。
- Arrow 仅作为 external interchange。
- VectorType v1：FLAT / CONSTANT / DICTIONARY。
- SelectionVector 独立。
- 使用 UnifiedVectorFormat。
- 默认 DataChunk 2048 rows。
- VARCHAR 使用 StringRef + SSO + Auxiliary StringHeap。
- LIST / STRUCT 是 v1 真实 Vector 能力。

## AD-002 Memory Architecture
Status: FROZEN

采用：
```text
MemoryManager
├── Query MemoryScope
├── Reservation
├── BufferAllocator
├── BufferManager
└── SpillCoordinator
```

- 大块 allocation 必须 Reserve first。
- `engine_memory_limit != process RSS`。
- Go runtime reserve 单独考虑。
- Buffer pool 必须可 reclaim。

## AD-003 Execution Model
Status: FROZEN

v1 使用：
```text
Physical Plan
→ Pipeline DAG
→ Source / Operator / Sink
```

不是 Pull/Volcano-first。

## AD-004 SQL Parser
Status: FROZEN

- 自研 PEG 主 Parser。
- Pratt 只用于 expression。
- Own AST。
- Parser / Binder / Planner 分层。
- AST 保存 source location。

## AD-005 CSV
Status: FROZEN

- Quote-aware state machine。
- Parser 与 boundary recognizer 共用 lexical rules。
- quoted newline 必须正确。
- 无法安全并行时退回 sequential，而非错误切分。

## AD-006 VFS Capability Model
Status: FROZEN

Capabilities 至少：
- RandomRead
- ParallelRandomRead
- StreamingRead
- StreamingWrite
- RandomWrite
- MultipartWrite
- AtomicRename
- ConditionalRead
- ConditionalWrite
- StableETag
- VersionedObject
- ServerSideList

禁止 silent whole-file fallback。

## AD-007 Parquet Reader
Status: FROZEN

采用 Controlled Parquet Engine：

```text
Footer Discovery
→ Internal Metadata
→ RowGroup Pruning
→ Projection
→ Range Planning
→ Async Fetch
→ Page Decode
→ Own Vector
```

不以 Arrow RecordBatch 作为核心路径。

## AD-008 Spill Algorithms
Status: FROZEN

- Hash Join：Adaptive/Hybrid External Hash Join。
- Hash Aggregate：local preagg + partition spill + combine。
- Sort：sorted runs + K-way merge。
- 共享 TemporaryStorageManager / Spillable Page Store。
- Spill ABI pointer-free / relocatable。

## AD-009 COPY / Export
Status: FROZEN

- ExportCoordinator + Manifest + WriterPool + Committer。
- file-level resume。
- incomplete file redo。
- S3 不伪造目录 rename。
- staging + final manifest/success marker 定义 dataset commit。

## AD-010 Catalog
Status: FROZEN

v1：
- MemoryCatalog
- SQLiteCatalog

不使用共享 S3 `catalog.json`。

## AD-011 Development Model
Status: FROZEN

使用风险驱动阶段：
- Phase 0 Kernel
- Phase 1 S3+Parquet
- Phase 2 OLAP+Spill
- Phase 3 Text+SFTP
- Phase 4 Export
- Phase 5 Production
- Phase 6 Release

每阶段必须通过 Correctness / Performance / Memory / Failure Gate。

## AD-012 Type System / Binder
Status: FROZEN

- LogicalType / PhysicalType 分离。
- 强类型 Binder。
- SQLNULL 仅 binding。
- DECIMAL 是正式 v1 类型。
- HUGEINT 是正式 v1 logical numeric type，复用 `PhysicalInt128` / `types.Int128`；当前 Stable SQL surface 仅按 ADR-0002 用于 DuckDB-compatible aggregate result semantics 及其结果传递。
- DECIMAL 与 HUGEINT 保持不同 logical identity，即使二者可共享 `PhysicalInt128`。
- Compare/Hash/Group 共用语义基础设施。

## AD-013 Optimizer
Status: FROZEN

```text
Rule Optimizer
+ Statistics/Cardinality
+ Join Order
+ Physical Cost Model
```

## AD-014 Query Lifecycle
Status: FROZEN

QueryContext 统一管理：
- cancel
- task group
- memory scope
- temp
- remote I/O
- profiler
- source snapshot

Query 终态前 owned async work 必须停止或显式转移 ownership。

## AD-015 Security Boundary
Status: FROZEN

- SecretManager
- Scoped Secret
- URI Resolver
- NetworkPolicy
- Endpoint validation
- redirect revalidation
- SFTP host-key verification
- FTPS default / plain FTP opt-in
- audit

## AD-016 Global Resource Scheduler
Status: FROZEN

统一：
- admission
- CPU slots
- query memory
- remote I/O limits
- spill I/O
- resource groups
- query classes

## AD-017 Profiling
Status: FROZEN

统一 Query/Pipeline/Operator/I/O/Memory-Spill profile。
Hot path 使用 worker-local counters。

## AD-018 Cache Architecture
Status: FROZEN

分离：
- BufferManager
- MetadataCache
- optional RemoteRangeCache
- Query ReadAhead

Prefetch != Cache。

## AD-019 Async Scan
Status: FROZEN

CPU Scheduler != Async I/O Scheduler。
Task 可 PARKED on I/O。

## AD-020 Function Framework
Status: FROZEN

FunctionCatalog：
- Scalar
- Aggregate
- Table

TableFunction：
```text
Bind
→ BindData
→ Pushdown
→ Scan State
→ ScanJob
→ DataChunk
```

## AD-021 Consistency Domains
Status: FROZEN

分离：
- Catalog Transaction
- Statement SourceSnapshot
- Export Manifest Commit

## AD-022 External API
Status: FROZEN

- Native Chunk API 为高性能主接口。
- database/sql 为兼容层。
- Arrow Adapter 为外部交换。
- Server v1 DATA payload 可使用 Arrow IPC。

## AD-023 Temporary Storage
Status: FROZEN

- v1 local SSD。
- remote spill deferred。
- global / resource-group / query temp quota。
- temporary data 与 export staging 分离。

## AD-024 Statistics
Status: FROZEN

StatisticsProvider：
- StorageNative
- CatalogPersistent
- Sampled
- Unknown

带 Confidence 与 DatasetFingerprint。

## AD-025 SQL Semantics
Status: FROZEN

Stable SQL 默认 DuckDB semantic compatibility。

## AD-026 Expression Executor
Status: FROZEN

BoundExpression → ExpressionExecutor → Vector Kernel。
Filter 使用 SelectionVector。
v1 no JIT。

## AD-027 Parquet Writer
Status: FROZEN

Memory-governed Parquet Writer。
RowGroup / Page / Multipart boundary 相互独立。

## AD-028 FileSetPlanner
Status: FROZEN

```text
CanonicalURI / Glob
→ Prefix Extraction
→ Listing Pruning
→ Partition Discovery
→ FileSnapshot
→ DatasetFingerprint
→ Immutable ScanSet
```

## AD-029 JSON / NDJSON
Status: FROZEN

- NDJSON 可 range parallel（满足 capability 时）。
- ARRAY/UNSTRUCTURED v1 structural streaming。
- Tokenizer → Typed Decoder → Vector。
- 不走 map[string]any 热路径。

## AD-030 RemoteClientManager
Status: FROZEN

Endpoint-scoped。
HTTP/S3/WebDAV 复用 client/transport。
SFTP/FTP 使用 bounded session pool。

## AD-031 Go Runtime / Buffer Ownership
Status: FROZEN

Engine-owned Managed Buffer。
大数据不依赖普通 Go heap 无界增长。

## AD-032 Controlled Parquet Dependencies
Status: FROZEN

第三方库仅提供 metadata/codec/encoding primitives。
第三方 type 不进入 core ABI。

## AD-033 Hash / Row Infrastructure
Status: FROZEN

共享：
- RowLayout
- TupleDataCollection
- VectorHash
- Hash primitives
- RadixPartitioner
- Spillable blocks

算法仍独立。

## AD-034 Module Boundaries
Status: FROZEN

严格单向依赖，CI architecture test 强制执行。

## AD-035 Settings
Status: FROZEN

Settings scope：
```text
Engine
→ ResourceGroup
→ Session
→ Statement
→ Immutable QuerySettings
```

## AD-036 Testing Architecture
Status: FROZEN

必须包含：
- SQLLogic
- DuckDB Differential
- Property
- Fuzz
- Fault Injection
- Deterministic Scheduler
- Race
- Interop
- Performance Regression

## AD-037 QueryRegistry
Status: FROZEN

Server：
```text
QueryRegistry
→ QueryHandle
→ QueryContext
→ ResultCursor
```

Query lifetime != HTTP lifetime。

## AD-038 Feature Contract
Status: FROZEN

所有 feature：
- STABLE
- EXPERIMENTAL
- DEFERRED

DuckDB compatibility contract 只覆盖 Stable。

## AD-039 Compression
Status: FROZEN

```text
VFS Raw Bytes
→ Compression
→ Format
```

Scan capability：
```text
VFS × Compression × Format
```

普通 gzip v1 视为 sequential/non-splittable。

## AD-040 Architecture Freeze
Status: FROZEN

冻结架构只通过 ADR 修改。

## AD-041 Release Verification
Status: FROZEN

Feature Freeze 后不再增加 Stable Feature。
v1 release 必须通过：
- compatibility
- correctness
- forced spill
- interop
- failure
- security
- performance baseline
- operations docs

## AD-042 Source Introspection
Status: FROZEN

Stable：
- DESCRIBE table/query
- information_schema.columns
- Go InspectSource
- Server source inspect
- parquet_schema
- parquet_file_metadata

Introspection 必须复用 Query Binder / TableFunction.Bind 的 Schema Resolution。
