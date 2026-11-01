# ADR-0005 — Remote Server Transport Contract

Status: ACCEPTED

## Context

Phase 5 `Required Implementation` 原文包含：

```text
HTTP/gRPC Server
```

而 Phase 5 Deliverable 只要求：

```text
P5-D4 Server query API
P5-D5 Source inspect API
```

当前实现已经形成单一 transport-neutral server core：

```text
QueryRegistry
→ QueryHandle
→ QueryContext
→ ResultCursor
→ QueryService
```

并提供 HTTP Query API 与 HTTP Source Inspect API。

AD-022 已冻结：

- Native Chunk API 是高性能主接口；
- database/sql 是兼容层；
- Arrow Adapter 是 external interchange；
- Server v1 DATA payload 可以使用 Arrow IPC。

AD-037 已冻结：

```text
QueryRegistry
→ QueryHandle
→ QueryContext
→ ResultCursor
```

且 Query lifetime 不等于 HTTP request lifetime。

如果把 `HTTP/gRPC Server` 解释为 v1 必须同时提供 HTTP 与 gRPC，就必须在没有现有上位契约的情况下冻结 protobuf package、service/method、message、result framing、stream cancellation、message size 和第三方 SDK boundary。这会过早扩大公开 wire contract，并把 transport 选择抬升为 Phase 5 acceptance blocker。

项目的 v1 目标是保持 embedded/core-first，引擎能力与 remote transport 解耦；remote server 是 QueryService 的外部适配层，不应反向决定 Core/Execution/Query ABI。

## Decision

### 1. Phase 5 v1 必需 remote transport 是 HTTP

Phase 5 的 `HTTP/gRPC Server` 解释为 remote server transport capability，而不是“HTTP + gRPC 双 transport 同时必需”。

v1 / Phase 5 必须提供：

```text
transport-neutral QueryService core
+ production-ready HTTP adapter
```

现有 HTTP Query API 与 Source Inspect API 可满足 P5-D4/P5-D5 的 transport requirement，前提是其 correctness、backpressure、cancellation、security、resource lifecycle 与阶段 Gate 全部通过。

### 2. gRPC 不属于 Phase 5 acceptance requirement

gRPC 在 v1 不构成 Stable 必需能力，也不是 Phase 5 `ACCEPTED` 的 blocker。

未来只有在存在明确产品需求时，才能通过独立 ADR 决定：

- protobuf package/version；
- service/method；
- control message schema；
- result streaming/framing；
- cancellation/disconnect semantics；
- message-size limits；
- compatibility policy。

在该 ADR 出现前，不实现或冻结 gRPC public wire contract。

### 3. QueryService 是唯一 server lifecycle core

任何未来 transport 必须复用：

```text
QueryService
→ QueryRegistry / QueryHandle
→ QueryContext
→ ResultCursor
```

禁止 transport adapter 自行复制：

- Query state machine；
- scheduler/admission；
- source schema inference；
- cancellation lifecycle；
- result buffering ownership；
- security policy。

### 4. Transport lifetime 不拥有 Query lifetime

保持 AD-037：

```text
Query lifetime != HTTP request lifetime
```

更一般地：

```text
Query lifetime != transport request lifetime
```

Submit 请求完成或客户端控制连接结束，不得隐式销毁 server-owned Query。

结果流 consumer 断开时，adapter 必须按现有 ResultCursor/cancellation contract 清理 ownership，不能留下 blocked producer、Managed Buffer 或 admission Lease。

### 5. External wire types 不进入 core ABI

允许：

```text
HTTP / future transport adapter
→ QueryService / Introspection Registry
```

禁止：

```text
HTTP / protobuf / gRPC / Arrow SDK types
→ internal/core
→ internal/execution
→ internal/query
→ internal/resource
```

Arrow 继续只作为 AD-022 定义的 external interchange；本 ADR 不要求 Server v1 DATA 必须使用 Arrow IPC，也不新增内部 Arrow ABI。

### 6. Phase 5 acceptance interpretation

Phase 5 Required Implementation 中的 server requirement 修正为：

```text
HTTP Server (v1 required)
gRPC: optional future adapter, requires separate ADR before becoming Stable
```

因此 Phase 5 acceptance 只需要验证当前 HTTP adapter 与共享 QueryService core，不需要等待 gRPC/protobuf 实现。

## Required Verification

Phase 5 Server acceptance 至少继续验证：

- Query submit/status/cancel/result lifecycle；
- Query lifetime 与 HTTP request lifetime 分离；
- slow-client bounded backpressure；
- disconnect/cancel 后 Managed Memory/Lease/ResultCursor ownership 归零；
- Source Inspect 复用真实 Bind schema resolution；
- request/body/result limits；
- security policy；
- race tests；
- server hot-path benchmark；
- graceful shutdown；
- long-running stability。

## Alternatives Considered

### A. Phase 5 强制同时实现 HTTP 和 gRPC

拒绝。

这会在没有明确产品需求和既有 wire contract 的情况下提前冻结第二套公开 transport，并增加 protobuf/gRPC dependency 与长期 compatibility surface。

### B. 只保留 gRPC，移除 HTTP

拒绝。

Phase 5 已完成并验证 HTTP Query/Source API；没有架构或性能证据要求用 gRPC 替代 HTTP。

### C. 现在定义一个“最小 protobuf”但不实现

拒绝。

没有实际 client/product requirement 时冻结 schema 只会制造无意义的 Stable compatibility obligation。

## Consequences

- Phase 5 不再因缺少 gRPC 而阻塞 acceptance。
- HTTP 是 v1 remote server 的正式 transport adapter。
- QueryService/QueryRegistry/QueryHandle/QueryContext/ResultCursor 保持唯一 server lifecycle core。
- 不修改 Own Vector/DataChunk ABI。
- 不修改 AD-022 Arrow external-interchange 边界。
- 不修改 AD-014/I-10 Query Lifecycle 要求。
- 不新增 Stable SQL feature。
- 未来 gRPC 如有需求，必须重新走 ADR，不得由普通实现任务自行决定 public wire contract。
