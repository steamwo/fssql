#!/usr/bin/env bash
set -euo pipefail
phase="${1:-phase-0}"
if [[ "$phase" != "phase-0" && "$phase" != "0" ]]; then
  echo "only phase-0 is implemented by this acceptance script" >&2
  exit 2
fi
go test ./...
go test -race ./...
go run ./tools/architecture-check .
go test -run '^$' -bench 'BenchmarkNumericAdd2048' -benchmem ./internal/execution/expression
go test -run '^$' -bench 'Benchmark(VectorHashInt64|VectorHashVarchar|HashTableLookup)' -benchmem ./internal/execution/hash
go test -run '^$' -bench 'BenchmarkPushPipeline100K' -benchmem ./internal/demo/phase0
