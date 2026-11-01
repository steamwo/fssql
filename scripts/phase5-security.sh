#!/usr/bin/env bash
set -euo pipefail

# Phase 5 P5-D10/P5-D11：安全边界和 credential rotation 必须通过真实协议/manager 测试。
# 覆盖 SSRF/loopback/link-local/metadata/redirect/forbidden-port/plain-FTP、Secret ACL、SFTP host key、credential version client isolation。

echo "[P5-D10] centralized security boundary"
go test -count=1 ./internal/security

echo "[P5-D10] protocol security integration"
go test -count=1 \
  ./internal/storage/remote/ftp \
  ./internal/storage/remote/sftp \
  ./internal/storage/remote/webdav

echo "[P5-D11] endpoint client credential-version isolation"
go test -count=1 ./internal/storage/remote \
  -run '^(TestClientManagerReusesExactScope|TestClientManagerCredentialVersionSeparatesClient|TestClientManagerBounded)$'

echo "[P5-D10/P5-D11] race-sensitive security packages"
go test -race -count=1 \
  ./internal/security \
  ./internal/storage/remote \
  ./internal/storage/remote/ftp \
  ./internal/storage/remote/sftp \
  ./internal/storage/remote/webdav

echo "P5-D10/P5-D11 security gate: PASS"
