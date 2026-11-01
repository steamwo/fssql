# Phase 5 — FTP / FTPS / WebDAV + Production

## Goal

进入 Production Candidate。

## Required Implementation

- FTP/FTPS VFS
- WebDAV VFS
- QueryRegistry
- QueryHandle
- ResultCursor
- HTTP Server（v1 required；gRPC 为 optional future adapter，进入 Stable 前必须单独 ADR，见 ADR-0005）
- ResourceGroup
- Global Resource Scheduler
- Security Boundary
- NetworkPolicy
- Secret ACL
- Audit
- SettingsRegistry
- Config migration
- Prometheus / OpenTelemetry adapters
- Graceful shutdown

## Deliverables

- P5-D1 FTP/FTPS driver
- P5-D2 WebDAV driver
- P5-D3 Endpoint capability matrix
- P5-D4 Server query API
- P5-D5 Source inspect API
- P5-D6 Query state machine
- P5-D7 slow-client backpressure
- P5-D8 cancellation leak test
- P5-D9 ResourceGroup fairness
- P5-D10 security suite
- P5-D11 credential rotation
- P5-D12 observability
- P5-D13 graceful shutdown
- P5-D14 config compatibility
- P5-D15 long-running stability test

## Query States

```text
SUBMITTED
→ QUEUED
→ PLANNING
→ RUNNING
→ FINISHING
→ SUCCEEDED / FAILED / CANCELED
```

## Security Tests

至少：
- SSRF
- loopback
- link-local
- metadata endpoint
- redirect bypass
- forbidden port
- unauthorized secret
- wrong SFTP host key
- plain FTP policy

## Cancellation Exit Assertions

```text
active tasks = 0
async requests = 0
reservations = 0
temp resources = 0
connection leases = 0
multipart uploads = 0
```

## Acceptance

`artifacts/phase-5/acceptance-report.md`

milestone：
```text
v1.0-rc
```

随后进入 Feature Freeze。
