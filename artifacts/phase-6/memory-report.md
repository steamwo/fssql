# Phase 6 Resource Envelope / Memory Report

Date: 2026-11-02
Candidate commit: `fd737c3c6f45f47fe1ba1735d09658e6c1120bc1`
Workflow run: `41430739607`

## Forced-spill OLAP workload

`cmd/phase2-metrics` executed 200,000 rows / 10,000 groups with a 32 MiB engine limit and 64 KiB operator budget.

- managed peak: 960,865 bytes
- managed used after completion: 0 bytes
- temp disk peak: 17,627,184 bytes
- temp used after completion: 0 bytes
- temp files after completion: 0
- Go heap live: 64,200 bytes
- Go total allocated: 333,506,640 bytes
- Go heap sys: 8,126,464 bytes
- RSS: 7,663,616 bytes
- GC cycles: 117
- spill passes: 98
- spill pages: 6,272
- spilled groups: 200,000
- peak aggregate bytes: 501,828

This workload proves forced-spill activity occurred and all Managed Memory / temporary-storage ownership drained after completion.

## Remote / compression / JSON workload

`cmd/phase3-metrics` exercised managed gzip decode, typed JSON decode, and bounded SFTP remote range I/O.

- managed peak: 3,145,728 bytes
- managed used after completion: 0 bytes
- Go heap live: 93,016 bytes
- Go total allocated: 8,630,768 bytes
- Go heap sys: 8,028,160 bytes
- RSS: 12,075,008 bytes
- GC cycles: 3
- temp disk peak: 0 bytes
- SFTP total sessions after close: 0
- SFTP leased sessions after close: 0
- SFTP idle sessions after close: 0

## Acceptance

The Phase 6 release script requires positive managed/heap/RSS/GC measurements, zero managed ownership after both workloads, and positive forced-spill temporary-disk usage in the Phase 2 workload. All checks passed.

The recorded values are a release resource envelope for this CI runner, not an RSS equality guarantee and not a cross-machine memory SLO. Engine-owned large buffers remain governed by MemoryManager / Managed Buffer as required by the frozen memory contract.
