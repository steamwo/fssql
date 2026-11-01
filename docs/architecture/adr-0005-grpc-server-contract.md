# ADR-0005 — Phase 5 gRPC Server Contract and Result Streaming

Status: PROPOSED

## Context

Phase 5 `Required Implementation` 明确包含：

```text
HTTP/gRPC Server
```

当前实现已经完成：

```text
QueryRegistry
→ QueryHandle
→ QueryContext
→ ResultCursor
→ QueryService
→ HTTP Query API
```

并已完成 `POST /v1/sources/inspect` 的 Bind-backed Source Introspection HTTP API。

但是仓库当前不存在：

- gRPC service definition；
- protobuf message/version contract；
- gRPC result streaming framing；
- gRPC cancellation / disconnect 与 Query lifetime 的绑定规则；
- gRPC source inspection method；
- gRPC message-size / backpressure contract。

AD-022 已冻结：

- Native Chunk API 为高性能主接口；
- database/sql 为兼容层；
- Arrow Adapter 为 external interchange；
- Server v1 DATA payload **可以**使用 Arrow IPC。

AD-037 已冻结：

```text
QueryRegistry
→ QueryHandle
→ QueryContext
→ ResultCursor
```

且：

```text
Query lifetime != HTTP lifetime
```

因此，直接为了完成 Phase 5 而自行选择 protobuf schema、JSON-over-gRPC、自定义 DATA framing 或 request-disconnect 语义，会创建新的公开 wire contract。该 contract 当前没有上位架构事实源授权，不能由普通实现任务静默决定。

## Problem

Phase 5 在所有 P5-D1～P5-D15 Deliverable 已有实现/验收项后，仍无法证明 `HTTP/gRPC Server` Required Implementation 完整满足。

需要明确：

1. `HTTP/gRPC` 是要求同时提供两个 transport，还是允许二选一；
2. gRPC v1 service/package/method 的稳定名称；
3. Query submit/status/cancel/forget 的 message contract；
4. ResultCursor 如何映射到 server streaming；
5. DATA payload 是否固定 Arrow IPC，以及如何 framing；
6. client stream cancellation 是否取消 Query；
7. Source Inspect 是否同时暴露 gRPC；
8. bounded message/backpressure/error mapping；
9. gRPC/protobuf 第三方类型允许出现的 module boundary。

在这些问题没有正式决定前，不应将 Phase 5 标记为 `ACCEPTED`。

## Proposed Decision

### 1. Phase 5 必须同时完成 HTTP 与 gRPC transport

将 Phase 5 的：

```text
HTTP/gRPC Server
```

解释为：

```text
one shared server lifecycle/core
+ HTTP adapter
+ gRPC adapter
```

两个 transport 必须复用同一个：

```text
QueryService
→ QueryRegistry / QueryHandle
→ QueryContext
→ ResultCursor
```

不得在 gRPC adapter 中复制 Query state machine、scheduler、schema inference 或 resource ownership。

### 2. gRPC API 使用版本化 protobuf package

建议稳定 package：

```text
fssql.v1
```

至少定义：

```text
service QueryService {
  rpc SubmitQuery(SubmitQueryRequest) returns (SubmitQueryResponse);
  rpc GetQuery(GetQueryRequest) returns (QueryStatusResponse);
  rpc CancelQuery(CancelQueryRequest) returns (CancelQueryResponse);
  rpc ForgetQuery(ForgetQueryRequest) returns (ForgetQueryResponse);
  rpc StreamResults(StreamResultsRequest) returns (stream ResultFrame);
}

service SourceService {
  rpc InspectSource(InspectSourceRequest) returns (InspectSourceResponse);
}
```

Query state 必须一一映射当前冻结/已实现状态：

```text
SUBMITTED
QUEUED
PLANNING
RUNNING
FINISHING
SUCCEEDED
FAILED
CANCELED
```

### 3. DATA payload 使用 Arrow IPC external-interchange framing

内部执行仍然是 Own Vector/DataChunk，Arrow 不得进入 core execution ABI。

建议 `StreamResults` 的边界为：

```text
Own DataChunk
→ Arrow Adapter
→ Arrow IPC message/frame
→ gRPC ResultFrame
```

`ResultFrame` 至少包含：

```text
sequence
kind = SCHEMA | RECORD_BATCH
payload = bytes
```

其中：

- 首个 DATA frame 提供 Arrow IPC Schema message；
- 后续 frame 提供 Arrow IPC RecordBatch message；
- gRPC stream 正常结束表示 result EOF；
- Query terminal failure 使用 gRPC terminal status 返回；
- Arrow/protobuf/grpc SDK type 只允许存在于 external adapter boundary，不进入 Query/Execution/Core ABI。

### 4. Result streaming 必须保持 bounded backpressure

`StreamResults` 必须直接消费现有单 consumer `ResultCursor`：

```text
ResultCursor.Next
→ encode one bounded frame
→ Send
→ Release DataChunk
→ next
```

禁止：

- 把全部结果先物化到 Go heap；
- unbounded result channel；
- per-query unbounded goroutine；
- 在 transport adapter 中复制结果队列。

慢 gRPC client 必须自然反压到 `ResultCursor`。

### 5. Query lifetime 与 RPC lifetime 分离

`SubmitQuery` RPC context 只控制 submit/admission 请求本身；一旦 Query 已由 server 接受：

```text
Query lifetime != SubmitQuery RPC lifetime
```

与现有 HTTP 语义一致。

`StreamResults` 是当前结果 consumer ownership。若 stream client 断开、Send 失败或 RPC context canceled：

```text
cancel Query
→ close/drain ResultCursor
→ Query lifecycle cleanup
```

因为 v1 ResultCursor 不提供可恢复结果流 checkpoint，不能让 producer 在失去唯一 consumer 后无限阻塞。

### 6. Source Inspect 必须复用同一 Introspection Registry

`InspectSource` gRPC method 必须调用当前：

```text
introspection.SourceSpec
→ Registry
→ format SpecInspector
→ real SourceBinder.Bind
```

禁止新增 gRPC 专属 schema inference。

### 7. gRPC resource limits

至少必须有显式：

- max inbound control message size；
- max outbound DATA frame size；
- single result consumer；
- bounded active Query / scheduler admission；
- cancellation/leak assertions。

任何超限必须显式失败，不得 silent downgrade。

### 8. Module boundary

允许：

```text
internal/server/grpc
→ internal/server QueryService
→ external grpc/protobuf/Arrow adapter libraries
```

禁止：

```text
grpc/protobuf types
→ internal/core
→ internal/execution
→ internal/query
→ internal/resource
```

## Required Tests if Accepted

至少增加：

- HTTP 与 gRPC 对同一 QueryService state/result 的 behavior equivalence；
- submit RPC disconnect 后 server-owned Query lifetime 测试；
- StreamResults slow-client backpressure；
- stream cancellation → Query canceled → Managed Memory/Lease/ResultCursor ownership = 0；
- single consumer conflict；
- oversized inbound/outbound frame rejection；
- Source Inspect HTTP == gRPC == actual Bind schema；
- protobuf compatibility/golden test；
- race test；
- gRPC streaming benchmark。

## Alternatives Considered

### A. 将 `HTTP/gRPC` 解释为 HTTP 或 gRPC 二选一

不建议。

原因：Phase 5 Required Implementation 没有明确写 `HTTP or gRPC`，而当前直接按二选一接受会把未定义的语义解释写进实现历史，而不是架构事实源。

### B. 使用 JSON custom codec over gRPC

不建议。

原因：会创建额外自定义 wire format，且弱化 protobuf 的 typed/versioned contract；没有现有冻结决定要求这样做。

### C. 把 gRPC 推迟到 Phase 6

不建议在普通开发中自行执行。

原因：AD-011 已冻结阶段边界，Phase 5 文档当前明确包含 HTTP/gRPC Server；若要延期，应由 ADR/Phase contract 显式修改，而不是实现层静默跨阶段。

## Consequences

若本 ADR 被 ACCEPTED：

- Phase 5 在 gRPC adapter、Arrow external encoding、interop/failure/performance tests 完成前保持 `IN_PROGRESS`；
- 不修改 Own Vector/DataChunk ABI；
- 不修改 QueryRegistry/QueryHandle/QueryContext/ResultCursor lifecycle；
- 不新增第二套 scheduler/security/introspection；
- gRPC/protobuf/Arrow 依赖被限制在 external adapter boundary；
- Phase 5 full acceptance gate 必须同时验证 HTTP 和 gRPC。

若决定 Phase 5 只要求 HTTP：

- 应通过 ADR 明确修改 Phase 5 Required Implementation/acceptance interpretation；
- 然后才能在不实现 gRPC 的情况下继续 Phase 5 acceptance。

## Acceptance Impact

在本 ADR 作出正式决定前：

```text
phase_5 remains IN_PROGRESS
phase_6 remains LOCKED
```

不得因为 P5-D1～P5-D15 已完成就绕过 `HTTP/gRPC Server` Required Implementation 直接标记 Phase 5 `ACCEPTED`。
