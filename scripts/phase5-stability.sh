#!/usr/bin/env bash
set -euo pipefail

# Phase 5 P5-D15：重复 Query 生命周期与远端 driver 生命周期，验证资源不会随运行次数持续累积。
# Server stability 每次包含 256 个 Query lifecycle（成功 + 取消）；普通模式重复 3 次，race 模式再执行 1 次。

echo "[P5-D15] repeated server/query lifecycle"
go test -count=3 ./internal/server -run '^TestPhase5LongRunningStability$'

echo "[P5-D15] server/query lifecycle under race detector"
go test -race -count=1 ./internal/server -run '^TestPhase5LongRunningStability$'

echo "[P5-D15] repeated remote protocol lifecycle"
go test -count=3 \
  ./internal/storage/remote/ftp \
  ./internal/storage/remote/webdav \
  ./internal/storage/remote/sftp \
  ./internal/storage/remote/s3

echo "[P5-D15] architecture boundary"
go run ./tools/architecture-check

echo "P5-D15 long-running stability gate: PASS"
