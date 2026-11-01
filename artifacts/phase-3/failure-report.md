# Phase 3 Failure Report

Date: 2026-11-01
Phase: 3 — CSV / JSON / NDJSON + Compression + SFTP
Status: PASS FOR PHASE 3 FAILURE MATRIX

## CSV

Covered failure classes:

- malformed / unterminated quoted fields;
- row-width mismatch;
- incomplete trailing records in non-final chunks;
- managed allocation failure;
- unsafe boundary candidates reduce parallelism instead of guessing a record boundary.

Regression tests verify that failed decode paths release managed buffers and that quoted newlines are never treated as record boundaries.

## JSON / NDJSON

Covered failure classes:

- malformed structural JSON;
- trailing/invalid tokens;
- typed decode mismatch;
- managed allocation failure;
- NDJSON framing and parallel/sequential equality;
- cancellation propagated through source reads.

Allocation-failure tests assert that managed memory returns to zero.

## Compression

Covered failure classes:

- corrupted gzip stream;
- bounded decompression output / memory-limit failure;
- zstd decode baseline and invalid input behavior;
- capability composition preventing gzip/zstd from being treated as splittable random-access input.

On decompression failure, managed output buffers and working reservations are released.

## SFTP

Covered failure/lifecycle classes:

- connection reset: retry only for retryable connection failures;
- poisoned session: discarded and never returned to the idle pool;
- wrong host key: hard failure, no retry/insecure fallback;
- cancellation while reading;
- cancellation while waiting for a bounded session;
- short read;
- endpoint mismatch rejected before dial;
- failed dial rolls back the pool slot;
- SFTP v3 wire protocol STAT/READ/LIST handling;
- strict OpenSSH `known_hosts` policy.

The SFTP tests assert that leases return to zero and failed range reads do not leak managed buffers.

## Phase 3 required failure gate mapping

| Required failure | Evidence | Result |
|---|---|---|
| malformed CSV | CSV lexical/decode regression tests | PASS |
| malformed JSON | `TestMalformedJSON` and tokenizer/typed decode tests | PASS |
| gzip corruption | `TestGzipCorruptionAndLimitReleaseMemory` | PASS |
| SFTP connection reset | `TestDriverRetriesResetPoisonsSession` | PASS |
| wrong host key | `TestWrongHostKeyNoRetry`, strict OpenSSH tests | PASS |
| cancel | CSV/NDJSON context handling and SFTP cancellation tests | PASS |

No failure path was implemented by bypassing MemoryManager, VFS, or the bounded SFTP session lifecycle.
