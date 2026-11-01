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
    go test -run '^$' -bench 'Benchmark' -benchmem -benchtime=100ms ./internal/format/csv
    go test -run '^$' -bench 'BenchmarkTypedDecode1024' -benchmem -benchtime=100ms ./internal/format/json
    go test -run '^$' -bench 'Benchmark(GzipDecode|ZstdDecode)' -benchmem -benchtime=100ms ./internal/format/compression
    go test -run '^$' -bench 'BenchmarkPoolAcquireRelease' -benchmem -benchtime=100ms ./internal/storage/remote/sftp
    ;;
  *)
    echo "unsupported acceptance phase: $phase" >&2
    exit 2
    ;;
esac
