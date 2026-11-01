#!/usr/bin/env bash
set -euo pipefail
phase="${1:-phase-0}"
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
  *)
    echo "unsupported acceptance phase: $phase" >&2
    exit 2
    ;;
esac
