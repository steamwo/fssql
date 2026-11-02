#!/usr/bin/env bash
set -euo pipefail

# Phase 6 is Feature Freeze: this script may verify/release existing Stable
# behavior, but it must not enable a new Stable feature.

# Repository correctness / race / vet / architecture freeze.
go test ./...
go test -race ./...
go vet ./...
go run ./tools/architecture-check .

# Stable SQL compatibility is a hard release input.
python3 - <<'PY'
import duckdb
want = "2.0.0"
if duckdb.__version__ != want:
    raise SystemExit(f"DuckDB reference version={duckdb.__version__!r}, want {want!r}")
PY
FSSQL_REQUIRE_DUCKDB=1 go test -count=1 ./test/compat
FSSQL_REQUIRE_DUCKDB=1 go test -race -count=1 ./test/compat

# Forced-spill equality must still hold at Feature Freeze.
go test -count=1 ./internal/execution/join -run 'TestExternalForcedSpillMatchesInMemory|TestExternalSkewFallsBackToBoundedTerminalJoin'
go test -count=1 ./internal/execution/aggregate -run 'TestExternalAggregateForcedSpillEqualsInMemory'
go test -count=1 ./internal/execution/distinct -run 'TestExternalDistinctForcedSpillEqualsInMemory'
go test -count=1 ./internal/execution/sort -run 'TestExternalSortForcedSpillEqualsInMemory|TestTopNMatchesFullSortLimit'

# Fault/leak and Phase 5 production failure/security/stability matrices remain
# release gates rather than historical evidence only.
go test -count=1 ./test/fault/... ./test/leak/...
bash ./scripts/phase5-cancellation-leak.sh
bash ./scripts/phase5-security.sh
bash ./scripts/phase5-config-compatibility.sh
bash ./scripts/phase5-stability.sh

# Parquet interoperability: canonical corpus + 4-producer x 4-reader matrix.
# The workflow pins the same reader versions accepted in Phase 1. The runner's
# stdout is machine-readable JSON; keep that contract covered by a lightweight
# unit test before consuming it as release evidence.
python3 -m unittest discover -s ./test/interop/phase1 -p 'run_output_test.py'
phase6_parquet="$(mktemp)"
python3 ./test/interop/phase1/run.py | tee "$phase6_parquet"
python3 - "$phase6_parquet" <<'PY'
import json, sys
with open(sys.argv[1], encoding="utf-8") as f:
    data = json.load(f)
if not data.get("canonical_corpus") or len(data.get("producer_grid", {})) != 4:
    raise SystemExit("Phase 6 Parquet interop evidence is incomplete")
PY

# fssql Parquet writer output must remain readable by DuckDB; PyArrow is
# installed in the release workflow and therefore supplies the additional read.
python3 ./test/interop/phase4/run.py

# Memory Contract resource envelope. These existing release workloads emit
# managed peak, Go heap live/allocated, RSS, GC cycles and temp disk peak.
phase6_resource2="$(mktemp)"
phase6_resource3="$(mktemp)"
go run ./cmd/phase2-metrics | tee "$phase6_resource2"
go run ./cmd/phase3-metrics | tee "$phase6_resource3"
python3 - "$phase6_resource2" "$phase6_resource3" <<'PY'
import json, sys
for index, path in enumerate(sys.argv[1:], start=2):
    with open(path, encoding="utf-8") as f:
        data = json.load(f)
    required_positive = [
        "managed_peak_bytes", "go_heap_live_bytes", "go_total_allocated_bytes",
        "rss_bytes", "gc_cycles",
    ]
    for key in required_positive:
        if int(data.get(key, 0)) <= 0:
            raise SystemExit(f"phase{index} resource metric {key} is not positive: {data.get(key)!r}")
    if int(data.get("managed_used_after_bytes", -1)) != 0:
        raise SystemExit(f"phase{index} managed ownership did not drain: {data}")
if int(json.load(open(sys.argv[1], encoding="utf-8")).get("temp_disk_peak_bytes", 0)) <= 0:
    raise SystemExit("forced-spill resource workload did not record temp disk usage")
PY

# Public/release documentation is part of the gate, not an optional follow-up.
test -s docs/operations.md
test -s docs/public-api.md
test -s compatibility/deviations.yaml
go doc ./pkg/gosql >/dev/null
go list ./pkg/gosql >/dev/null

# Release performance baselines. These are machine-local regression evidence;
# Phase 6 does not invent cross-machine absolute SLOs.
go test -run '^$' -bench 'BenchmarkNativeQueryChunks$' -benchmem -benchtime=100ms ./pkg/gosql
go test -run '^$' -bench 'Benchmark' -benchmem -benchtime=100ms ./internal/format/parquet
go test -run '^$' -bench 'Benchmark(InnerHashJoin2048|ExternalHashJoinForcedSpill)$' -benchmem -benchtime=100ms ./internal/execution/join
go test -run '^$' -bench 'Benchmark(HashAggregateBigInt|ExternalAggregateForcedSpill)$' -benchmem -benchtime=100ms ./internal/execution/aggregate
go test -run '^$' -bench 'Benchmark(ExternalSortForcedSpill|TopN)$' -benchmem -benchtime=100ms ./internal/execution/sort
go test -run '^$' -bench 'Benchmark' -benchmem -benchtime=100ms ./internal/server ./internal/profile

echo "Phase 6 Feature Freeze / v1 release gate: PASS"
