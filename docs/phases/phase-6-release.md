# Phase 6 — Feature Freeze / v1 Release

## Rule

本阶段不新增 Stable Feature。

只处理：
- bug
- security
- compatibility
- regression
- operations
- docs
- tests

## Deliverables

- P6-D1 Stable Feature Matrix
- P6-D2 DuckDB Compatibility Report
- P6-D3 Compatibility Deviation Registry
- P6-D4 Parquet Interop Report
- P6-D5 Full Failure Matrix
- P6-D6 Performance Baseline
- P6-D7 Resource Envelope
- P6-D8 Security Review
- P6-D9 Operational Documentation
- P6-D10 Public API Documentation
- P6-D11 Release Artifacts

## Release Gate

Only release when:

```text
Stable Feature Complete
+ SQL Compatibility Verified
+ Correctness Verified
+ Forced-Spill Verified
+ Interop Verified
+ Failure Matrix Passed
+ Security Review Passed
+ Performance Baseline Established
+ Resource Envelope Documented
+ Operational Documentation Complete
```

## Architecture Freeze Check

确认：
- Push Pipeline 未被绕过
- Vector/DataChunk 仍为 internal ABI
- Arrow 仍 external only
- MemoryManager ownership intact
- Remote scan 仍走 unified VFS/I/O
- no silent whole-file fallback
- SourceSnapshot 未破坏
- spill 仍走 TemporaryStorageManager
- Query lifecycle 无后台资源泄漏

## Acceptance

发布：
```text
v1.0 Stable
```
