#!/usr/bin/env bash
set -euo pipefail

# Phase 5 P5-D8：取消后必须证明 Query 生命周期、异步 I/O、临时资源、连接 lease、multipart ownership 全部归零。
# 这里复用真实组件测试，不使用仅为 gate 构造的假 ownership tracker。

echo "[P5-D8] query/server lifecycle"
go test -count=1 ./internal/server \
  -run 'Test(FinalizeRefusesTerminalWhileActiveTaskExists|QueryServiceCancelUnblocksSlowProducerAndReleasesManagedChunks|HTTPAPIEncoderFailureCancelsQueryAndReleasesBufferedChunks)$'

echo "[P5-D8] async I/O cancellation"
go test -count=1 ./internal/storage/asyncio \
  -run '^TestCancellationDrainsActiveRequest$'

echo "[P5-D8] temporary-storage ownership"
go test -count=1 ./internal/storage/temp

echo "[P5-D8] bounded remote session leases"
go test -count=1 ./internal/storage/remote/sftp \
  -run '^(TestPoolBoundedCancelReuseAndPoison|TestPoolFactoryFailureRollsBackSlot)$'

echo "[P5-D8] S3 multipart cleanup"
go test -count=1 ./internal/storage/remote/s3 \
  -run '^(TestS3MultipartRetryAndAbort|TestS3CompleteFailureRemainsAbortable)$'

echo "[P5-D8] race-sensitive lifecycle packages"
go test -race -count=1 \
  ./internal/server \
  ./internal/storage/asyncio \
  ./internal/storage/temp \
  ./internal/storage/remote/sftp \
  ./internal/storage/remote/s3

echo "P5-D8 cancellation leak gate: PASS"
