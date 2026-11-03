# VFS Contract

## Capability Set

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

## EffectiveCapabilities

```text
Static Driver Capabilities
+
Runtime Endpoint Probe
=
EffectiveCapabilities
```

Probe 必须绑定：
- endpoint
- credential scope/version
- security profile

## Fallback

```text
remote_fallback_policy = deny | warn | allow
```

禁止 silent whole-file fallback。

## VFS 不得认识

- Parquet
- CSV
- JSON
- Optimizer
