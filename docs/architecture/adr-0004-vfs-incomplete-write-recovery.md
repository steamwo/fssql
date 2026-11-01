# ADR-0004 — VFS Incomplete Write Recovery for Crash-Safe Remote Export

Status: PROPOSED

## Context

Phase 4 Failure Gate 明确要求：

```text
Kill / Restart / Resume
```

并保证：

- no duplicate completed files；
- no missing files；
- no invalid committed dataset；
- no leaked multipart。

ADR-0003 已补全稳定 output segment identity，LocalManifestStore 已能在单节点进程重启后恢复 Export Manifest。

当前 S3 writer 的 multipart ownership 仍只存在于进程内 `s3WriteHandle.uploadID`：

```text
CreateMultipartUpload
-> uploadID only in process memory
-> UploadPart...
-> CompleteMultipartUpload / AbortMultipartUpload
```

正常错误、cancel、Close failure 可以通过 live handle `Abort()` 清理。但如果进程在 multipart 已创建后硬退出，`uploadID` 随进程内存丢失；下一次 Resume 只有稳定文件路径和 Manifest file state，无法调用 `AbortMultipartUpload`。

因此当前设计无法证明 Phase 4 的 `no leaked multipart` hard-kill gate。

单纯把 uploadID 写入本地 journal 仍有不可关闭窗口：

```text
S3 CreateMultipartUpload succeeds
-> process killed before local journal fsync
```

该 upload 在重启后仍无法仅靠 journal 找回。

## Decision Proposal

### 1. 增加协议无关的 VFS incomplete-write recovery capability

在 AD-006 VFS capability model 中增加：

```text
IncompleteWriteRecovery
```

该 capability 表示 driver 能在一个**调用方独占的 staging URI scope** 下发现并清理由未完成远端写操作持有的协议资源。

它不是 `AtomicRename`，也不是普通对象删除能力。

### 2. 增加 optional recoverable write driver contract

VFS 增加协议无关 optional interface，概念如下：

```text
RecoverableWritableDriver
  RecoverIncompleteWrites(ctx, scopeURI) -> RecoveryReport
```

`RecoveryReport` 至少记录：

```text
Discovered
Recovered
```

具体 protocol token（例如 S3 upload ID）不得进入 Export、Format、Optimizer 或 core ABI。

### 3. Recovery scope 必须由单个 Export Job 独占

所有声明 crash-resumable 的远端 export 必须使用 job-exclusive staging scope，例如：

```text
s3://bucket/<dataset-staging>/<JobID>/...
```

Export recovery 只能要求 driver 清理该 job-owned scope。

禁止对共享 bucket/dataset prefix 做无边界 cleanup，避免误杀其他 query、其他进程或外部客户端的写入。

### 4. S3 implementation

S3 driver 的 `IncompleteWriteRecovery` 实现采用：

```text
ListMultipartUploads(prefix = exact job staging prefix)
-> paginate all results
-> AbortMultipartUpload for every matching upload
-> only after all aborts succeed report recovery complete
```

这样即使进程发生在：

```text
CreateMultipartUpload success
-> kill before any local fsync
```

重启仍能通过服务端 discovery 找回并清理 orphan multipart。

正常运行时仍优先由 live `WriteHandle.Abort()` 清理；recovery 不是正常错误处理的替代品。

### 5. Resume ordering

对于要求 crash-resumable 且 destination driver 暴露 `IncompleteWriteRecovery` 的作业，恢复顺序固定为：

```text
Load durable Manifest
-> validate QueryFingerprint / SourceSnapshot / SegmentPlan
-> RecoverIncompleteWrites(job staging scope)
-> only when recovery fully succeeds: PrepareResume
-> redo incomplete files
-> dataset commit
```

如果 remote recovery 失败或被取消：

```text
resume MUST stop
```

不得继续写新文件、不得把资源泄漏降级为 warning、不得静默忽略。

### 6. Capability semantics

- S3 resumable export：必须要求 `IncompleteWriteRecovery`。
- S3 non-resumable export：不因缺少该 capability 而被全局禁止，但仍必须在正常 cancel/error 路径执行 live Abort。
- SFTP 的 incomplete file 是普通目标文件而非隐藏 multipart ownership；整文件 truncate/redo 不要求伪造该 capability。
- 其他未来 protocol 只有在存在跨进程隐藏 write ownership 时才实现该 capability。

### 7. Protocol × Compression × Format orthogonality 不变

Export 只认识：

```text
VFS capability + job staging scope
```

Export 不得 import S3 package，也不得解析 upload ID。

FormatWriter/CompressionWriter 不感知 recovery token。

## Alternatives

### A. Persist S3 upload ID in Export Manifest

Rejected。

原因：

- protocol-specific token 泄漏进 Export Manifest；
- 破坏 Protocol × Compression × Format 正交边界；
- 仍无法关闭 CreateMultipartUpload 成功到本地 Manifest fsync 之间的 kill window。

### B. S3 adapter hidden local journal only

Rejected as sole correctness mechanism。

原因：

- 同样存在远端创建成功、本地 journal 尚未 durable 的窗口；
- ownership 对 Query/Export recovery 不可见，难以证明 I-10；
- 无法用 Phase 4 generic failure gate 统一验证。

可以作为优化缓存，但不能作为 correctness source。

### C. Resume 时按稳定 object path 覆盖即可

Rejected。

覆盖最终对象不会删除旧 multipart upload；S3 会继续保留其 part 并产生存储费用，因此不能满足 `no leaked multipart`。

### D. 禁止 multipart，所有文件本地 spool 后 PutObject

Rejected as Phase 4 default architecture。

它会把远端 streaming write 退化为 whole-file local materialization，增加磁盘与 latency，并改变已冻结的 memory-governed streaming/export 方向。

### E. Export 直接调用 S3 ListMultipartUploads

Rejected。

这会让 `internal/export` 依赖具体 protocol adapter，违反模块边界和 AD-006 capability model。

## Consequences

Positive:

- 可以严格验证 hard-kill 后 `incomplete multipart = 0`。
- 不需要把 S3 upload ID 暴露给 Export Manifest。
- 可以覆盖最窄的远端成功、本地状态尚未落盘窗口。
- 与 ADR-0003 stable segment identity、AD-009 staging dataset commit 兼容。
- future protocol 可按真实能力选择实现，不做 silent fallback。

Negative:

- VFS capability set 和 writable driver contract 增加一个 recovery 能力。
- S3 需要实现 ListMultipartUploads pagination 与 Abort reconciliation。
- crash-resumable S3 export 要求凭据拥有 list/abort multipart 权限；缺少权限时必须在恢复阶段明确失败。
- job staging prefix 必须具有明确的独占 ownership contract。

## Required Tests

至少包括：

1. multipart 已创建且至少上传一个 part，模拟进程状态完全丢失后，通过 job staging prefix discovery 找到并 abort。
2. kill 发生在 CreateMultipartUpload 返回后、任何本地 journal 之前，仍能 discovery cleanup。
3. pagination：超过单页 multipart uploads 时全部发现并清理。
4. scope isolation：相邻 job prefix / 共享 bucket 的 upload 不得被误 abort。
5. AbortMultipartUpload transient failure：按既有 retry policy 重试；最终失败时 Resume 被阻断。
6. credential/permission failure：明确失败，不继续数据写入。
7. cancellation：停止 recovery，不启动新的 export writer。
8. recovery idempotence：重复执行后 `Discovered=0` 或所有目标已清理。
9. managed metadata/network buffers 在成功/失败/cancel 后归零。
10. 完整 Kill/Restart/Resume fault test 最终断言 incomplete multipart = 0。

## Affected Frozen Decisions

若批准，本 ADR 将补充：

- AD-006 VFS Capability Model — 增加 `IncompleteWriteRecovery` capability 与 optional recovery contract；
- AD-009 COPY / Export — crash-resumable remote staging 在重写 incomplete files 前必须完成 protocol-resource recovery；
- AD-014 Query Lifecycle — hard kill 后的隐藏 remote write ownership 通过 job recovery scope 显式接管并清理；
- AD-021 Consistency Domains — recovery 只作用于 Export staging，不改变 Statement SourceSnapshot；
- AD-030 RemoteClientManager — S3 recovery 继续使用 endpoint-scoped client；
- I-06 Protocol × Compression × Format orthogonal；
- I-10 Lifecycle Protocol — Phase 4 hard-restart gate 可严格证明 incomplete multipart = 0。

## Approval

Pending project owner approval.
