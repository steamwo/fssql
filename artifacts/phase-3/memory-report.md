# Phase 3 Memory Report

Date: 2026-11-01
Phase: 3 — CSV / JSON / NDJSON + Compression + SFTP
Status: PASS FOR PHASE 3 SCENARIO

## Scenario

`cmd/phase3-metrics` exercises:

1. gzip decompression into a managed output buffer;
2. JSON typed decode into managed Vector/StringHeap storage;
3. an SFTP VFS range read through the bounded session pool;
4. release of all buffers, query-scope reservations, and SFTP leases.

Phase 3 does not use temporary spill storage, so the expected temp-disk peak is zero.

## Captured metrics

```json
{
  "managed_peak_bytes": 3145728,
  "managed_used_after_bytes": 0,
  "go_heap_live_bytes": 147744,
  "go_total_allocated_bytes": 8686280,
  "go_heap_sys_bytes": 12156928,
  "rss_bytes": 11264000,
  "gc_cycles": 3,
  "temp_disk_peak_bytes": 0,
  "sftp_total_sessions_after": 0,
  "sftp_leased_sessions_after": 0,
  "sftp_idle_sessions_after": 0
}
```

## Memory invariants

- Vector payloads and StringHeap remain on the existing managed Vector ABI.
- CSV/JSON decode scratch is accounted through Phase 3 memory tags.
- gzip/zstd output is produced through `BufferManager` and bounded by an explicit maximum output size.
- SFTP range payloads use managed network/SFTP buffers.
- The measured scenario terminates with `managed_used_after_bytes = 0`.
- The measured SFTP lifecycle terminates with zero total, leased, and idle sessions after manager close.

No Architecture Invariant was relaxed or bypassed for this evidence.
