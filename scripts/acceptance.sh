#!/usr/bin/env bash
set -euo pipefail
phase="${1:-phase-0}"

# Phase 6 has its own release-only gate so Feature Freeze checks remain easy to
# audit without duplicating or weakening the already accepted phase gates.
if [[ "$phase" == "phase-6" || "$phase" == "6" ]]; then
  exec bash ./scripts/phase6-release.sh
fi

case "$phase" in
  phase-0|0)
    go test ./...
    go test -race ./...
    go run ./tools/architecture-check .
    go test -run '^$' -bench 'BenchmarkNumericAdd2048' -benchmem ./internal/execution/expression
    go test -run '^$' -bench 'Benchmark(VectorHashInt64|VectorHashVarchar|HashTableLookup)' -benchmem ./internal/execution/hash
    go test -run '^$' -bench 'BenchmarkPushPipeline100K' -benchmem ./internal/demo/phase0
    ;;
  phase-2|2)
    go test ./...
    go test -race ./...
    go vet ./...
    go run ./tools/architecture-check .
    go test -count=1 ./internal/execution/join -run 'TestExternalForcedSpillMatchesInMemory|TestExternalSkewFallsBackToBoundedTerminalJoin'
    go test -count=1 ./internal/execution/aggregate -run 'TestExternalAggregateForcedSpillEqualsInMemory'
    go test -count=1 ./internal/execution/distinct -run 'TestExternalDistinctForcedSpillEqualsInMemory'
    go test -count=1 ./internal/execution/sort -run 'TestExternalSortForcedSpillEqualsInMemory|TestTopNMatchesFullSortLimit'
    go test -count=1 ./internal/storage/temp -run 'TestCreatePermissionDeniedHasNoOwnershipLeak|TestDiskFullRollsBackReservation|TestShortWriteAccountsOnlyWrittenBytes|TestCleanupFailureRetainsOwnershipForRetry|TestSpillablePageStoreDetectsCorruption|TestSpillablePageStoreReadAllocationFailureHasNoLeak|TestSpillablePageStoreShortWritePoisonsStoreAndCleansUp|TestSpillablePageStoreCanceledReadDoesNotAllocate'
    go test -count=1 ./internal/execution/hash -run 'TestSkewDetectorSyntheticDataset|TestSameKeySkewMakesNoProgressAcrossDepth'
    go test -count=1 ./internal/execution/join -run 'TestExternalCancelDuringSpillReleasesManagedState'
    go test -count=1 ./internal/execution/aggregate -run 'TestExternalAggregateTempQuotaFailureCleansUp|TestExternalAggregateCancelAfterSpillCleansUp'
    go test -count=1 ./internal/execution/sort -run 'TestExternalSortTempQuotaFailure|TestExternalSortCancelAfterSpillCleansUp'
    go test -run '^$' -bench 'Benchmark(TemporaryQuotaReserveRelease|RLEEncode|SpillPageStoreRoundTrip)$' -benchmem -benchtime=100ms ./internal/storage/temp
    go test -run '^$' -bench 'Benchmark(HashChunkBigIntVarchar|PartitionHistogramAddHash)$' -benchmem -benchtime=100ms ./internal/execution/hash
    go test -run '^$' -bench 'Benchmark(InnerHashJoin2048|ExternalHashJoinForcedSpill)$' -benchmem -benchtime=100ms ./internal/execution/join
    go test -run '^$' -bench 'Benchmark(HashAggregateBigInt|ExternalAggregateForcedSpill)$' -benchmem -benchtime=100ms ./internal/execution/aggregate
    go test -run '^$' -bench 'BenchmarkExternalDistinct$' -benchmem -benchtime=100ms ./internal/execution/distinct
    go test -run '^$' -bench 'Benchmark(ExternalSortForcedSpill|TopN)$' -benchmem -benchtime=100ms ./internal/execution/sort
    go run ./cmd/phase2-metrics >/dev/null
    ;;
  phase-3|3)
    go test ./...
    go test -race ./...
    go vet ./...
    go run ./tools/architecture-check .
    go test -count=1 ./internal/format/csv -run 'TestParallelCSVEqualsSequentialCSV|TestParallelRequestFallsBackToSequentialWithoutParallelRandomRead|TestSourceBindingAndInspectionShareInference'
    go test -count=1 ./internal/format/json -run 'TestTypedNestedProjection|TestInferNested|TestMalformedJSON|TestAllocationFailureNoLeak|TestParallelNDJSONEqualsSequential'
    go test -count=1 ./internal/format/compression -run 'TestGzipAndZstdLogicalResultEqualsPlain|TestGzipCorruptionAndLimitReleaseMemory|TestCompressionCapabilityComposition'
    go test -count=1 ./internal/storage/remote/sftp -run 'TestDriverRetriesResetPoisonsSession|TestWrongHostKeyNoRetry|TestCancelReadReleasesLeaseAndManagedBuffer|TestEndpointMismatchRejectedBeforeDial|TestSFTPQueryDemoCSV|TestParallelCapabilityRequiresPoolCapacity|TestPoolBoundedCancelReuseAndPoison|TestPoolFactoryFailureRollsBackSlot|TestWireSessionStatReadList|TestOpenSSH'
    go test -count=1 ./test/interop/phase3 -run 'TestDuckDBV200Golden'
    go test -run '^$' -bench 'Benchmark' -benchmem -benchtime=100ms ./internal/format/csv
    go test -run '^$' -bench 'BenchmarkTypedDecode1024' -benchmem -benchtime=100ms ./internal/format/json
    go test -run '^$' -bench 'Benchmark(GzipDecode|ZstdDecode)' -benchmem -benchtime=100ms ./internal/format/compression
    go test -run '^$' -bench 'BenchmarkPoolAcquireRelease' -benchmem -benchtime=100ms ./internal/storage/remote/sftp
    go run ./cmd/phase3-metrics >/dev/null
    ;;
  phase-4|4)
    # Phase 4 全仓 gate：不得用 targeted PASS 替代普通/race/vet/架构检查。
    go test ./...
    go test -race ./...
    go vet ./...
    go run ./tools/architecture-check .

    # Export/VFS/remote packages 分别重跑，固定 Manifest/Resume/Commit/WriterLimit/协议恢复 failure matrix。
    go test -count=1 ./internal/export
    go test -count=1 ./internal/storage/vfs
    go test -count=1 ./internal/storage/remote/s3
    go test -count=1 ./internal/storage/remote/sftp

    # P4-D2 必须真实经过 S3/SFTP driver、CSV decode、Own DataChunk 与 Export writer。
    go test -count=1 ./test/interop/phase4 -run 'Test(S3ToSFTP|SFTPToS3)ProtocolConversion$'

    # P4-D3 DuckDB reader interop：Python module 或 CLI 至少一个必须存在；PyArrow 仅作为可选附加证据。
    python3 ./test/interop/phase4/run.py

    # P4-D4/P4-D9/P4-D3 的性能关键路径只记录本机 baseline，不作为跨机器绝对阈值。
    go test -run '^$' -bench 'Benchmark(WriterPoolAcquireRelease|PartitionRouting2048x64|ParquetWriterInteger1024)$' -benchmem -benchtime=100ms ./internal/export
    ;;
  phase-5|5)
    # Phase 5 Production Candidate 必须先证明全仓普通、race、vet 与架构边界全部成立。
    go test ./...
    go test -race ./...
    go vet ./...
    go run ./tools/architecture-check .

    # ADR-0006 remediation 的参考语义版本是 release input，缺失或漂移必须失败，不能 skip。
    python3 - <<'PY'
import duckdb
want = "2.0.0"
if duckdb.__version__ != want:
    raise SystemExit(f"DuckDB reference version={duckdb.__version__!r}, want {want!r}")
PY

    # Stable public composition / COPY / atomic publish 以真实 production boundaries 重新验证。
    go test -count=1 \
      ./pkg/gosql \
      ./internal/session \
      ./internal/export \
      ./internal/storage/vfs \
      ./internal/storage/remote/s3
    go test -race -count=1 ./pkg/gosql

    # SQLLogic expected result、live DuckDB differential、deviations registry 必须一起通过；Stable mismatch 不允许 registry waiver。
    FSSQL_REQUIRE_DUCKDB=1 go test -count=1 ./test/compat
    FSSQL_REQUIRE_DUCKDB=1 go test -race -count=1 ./test/compat

    # 生命周期、security/credential、配置兼容和长期稳定性使用已独立验收的真实组件 gate。
    bash ./scripts/phase5-cancellation-leak.sh
    bash ./scripts/phase5-security.sh
    bash ./scripts/phase5-config-compatibility.sh
    bash ./scripts/phase5-stability.sh

    # Server/Resource/remote packages 单独重跑，确保最终组合状态仍满足 Query API、Inspect、shutdown 与远端 driver 契约。
    go test -count=1 \
      ./internal/server \
      ./internal/resource \
      ./internal/security \
      ./internal/settings \
      ./internal/storage/remote \
      ./internal/storage/remote/ftp \
      ./internal/storage/remote/webdav \
      ./internal/storage/remote/sftp \
      ./internal/storage/remote/s3

    # Phase 5 hot paths 记录本机 baseline，不使用跨机器绝对阈值；Native API 也必须留下基线。
    go test -run '^$' -bench 'BenchmarkNativeQueryChunks$' -benchmem -benchtime=100ms ./pkg/gosql
    go test -run '^$' -bench 'Benchmark' -benchmem -benchtime=100ms \
      ./internal/server \
      ./internal/profile \
      ./internal/storage/remote/ftp \
      ./internal/storage/remote/webdav
    ;;
  *)
    echo "unsupported acceptance phase: $phase" >&2
    exit 2
    ;;
esac
