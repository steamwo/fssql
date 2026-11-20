package join

import (
	"context"
	"errors"
	"fmt"
	"math"
	"sync"

	"github.com/steamwo/fssql/internal/core/memory"
	"github.com/steamwo/fssql/internal/core/types"
	"github.com/steamwo/fssql/internal/core/vector"
	exechash "github.com/steamwo/fssql/internal/execution/hash"
	"github.com/steamwo/fssql/internal/execution/pipeline"
	storagetemp "github.com/steamwo/fssql/internal/storage/temp"
)

// StableExternalHashJoin 在现有 hybrid external join spill core 上补齐 Stable outer/semi/anti 语义。
// INNER 仍复用原 ExternalHashJoin 的 adaptive build-side fast path；其余类型按 preserved-side 约束 build side。
type StableExternalHashJoin struct {
	core            *ExternalHashJoin
	residentMatched []*memory.Buffer // residentMatched 仅 FULL 使用；每个 resident build row 一 bit。
	matchedMu       sync.Mutex       // FULL 的并发 probe worker 共享 matched bitmap。
}

// NewStableExternal 创建 Stable equi hash join。CROSS 不使用 hash join，应走 managed cross runtime。
func NewStableExternal(config ExternalConfig) (*StableExternalHashJoin, error) {
	if err := validateRequiredOutput(config); err != nil { return nil, err }
	config.RequiredOutput = append([]bool(nil), config.RequiredOutput...)
	if config.Type == Cross || config.Type > Anti {
		return nil, fmt.Errorf("stable external hash join requires non-CROSS Stable join type")
	}
	if config.Type == Inner {
		core, err := NewExternal(config)
		if err != nil {
			return nil, err
		}
		return &StableExternalHashJoin{core: core}, nil
	}
	core, err := newStableExternalCore(config, stableBuildSide(config.Type))
	if err != nil {
		return nil, err
	}
	return &StableExternalHashJoin{core: core, residentMatched: make([]*memory.Buffer, len(core.partitions))}, nil
}

func stableBuildSide(kind Type) Side {
	if kind == Right {
		return LeftSide
	}
	// LEFT/FULL/SEMI/ANTI 都以 SQL 左侧为 probe，使 preserved/semi/anti 输出可流式判定。
	return RightSide
}

func newStableExternalCore(config ExternalConfig, buildSide Side) (*ExternalHashJoin, error) {
	if config.Scope == nil || config.Buffers == nil || config.Temp == nil {
		return nil, errors.New("external join requires memory, buffers and temporary scope")
	}
	if config.MemoryBudget < 4096 {
		return nil, errors.New("external join memory budget must be at least 4096 bytes")
	}
	if config.RadixBits == 0 {
		config.RadixBits = 4
	}
	if config.RadixBits > 8 {
		return nil, errors.New("external join radix bits baseline is limited to 1..8")
	}
	if config.MaxDepth <= 0 {
		config.MaxDepth = 4
	}
	if config.MinReductionPercent == 0 {
		config.MinReductionPercent = 10
	}
	if config.SpillBlockBytes <= 0 {
		config.SpillBlockBytes = 64 << 10
	}
	if len(config.LeftKeys) == 0 || len(config.LeftKeys) != len(config.RightKeys) {
		return nil, errors.New("external hash join requires matching non-empty key lists")
	}
	for i := range config.LeftKeys {
		lk, rk := config.LeftKeys[i], config.RightKeys[i]
		if lk < 0 || lk >= len(config.LeftTypes) || rk < 0 || rk >= len(config.RightTypes) {
			return nil, errors.New("external join key index out of range")
		}
		if !sameType(config.LeftTypes[lk], config.RightTypes[rk]) {
			return nil, errors.New("external join key type mismatch")
		}
	}
	leftLayout, err := exechash.NewRelocatableRowLayout(config.LeftTypes)
	if err != nil {
		return nil, err
	}
	rightLayout, err := exechash.NewRelocatableRowLayout(config.RightTypes)
	if err != nil {
		return nil, err
	}
	partitioner, err := exechash.NewRadixPartitioner(config.RadixBits)
	if err != nil {
		return nil, err
	}
	policy, err := exechash.NewRepartitionPolicy(config.MaxDepth, config.MinReductionPercent)
	if err != nil {
		return nil, err
	}
	store, err := storagetemp.NewSpillablePageStore(config.Temp, config.Scope, config.Buffers, storagetemp.PageStoreConfig{Compression: config.Compression, MinCompressBytes: 1024})
	if err != nil {
		return nil, err
	}
	partRes, err := config.Scope.Reserve(memory.TagMetadata, int64(partitioner.Count()*externalPartitionMetadataBytes))
	if err != nil {
		_ = store.Close()
		return nil, err
	}
	core := &ExternalHashJoin{config: config, buildSide: buildSide, partitioner: partitioner, policy: policy, store: store, partitions: make([]hybridPartition, partitioner.Count()), partitionRes: partRes}
	core.stats.BuildSide = buildSide
	if buildSide == RightSide {
		core.buildTypes, core.probeTypes = config.RightTypes, config.LeftTypes
		core.buildKeys, core.probeKeys = config.RightKeys, config.LeftKeys
		core.buildLayout, core.probeLayout = rightLayout, leftLayout
	} else {
		core.buildTypes, core.probeTypes = config.LeftTypes, config.RightTypes
		core.buildKeys, core.probeKeys = config.LeftKeys, config.RightKeys
		core.buildLayout, core.probeLayout = leftLayout, rightLayout
	}
	return core, nil
}

func (j *StableExternalHashJoin) BuildSide() Side { return j.core.buildSide }
func (j *StableExternalHashJoin) Stats() ExternalStats { return j.core.Stats() }

func (j *StableExternalHashJoin) BuildSink() pipeline.Sink {
	return &stableExternalBuildSink{join: j, inner: &externalBuildSink{join: j.core}}
}
func (j *StableExternalHashJoin) ProbeSink(downstream pipeline.Sink) pipeline.Sink {
	return &stableExternalProbeSink{join: j, downstream: downstream}
}
func (j *StableExternalHashJoin) Close() {
	if j == nil || j.core == nil {
		return
	}
	j.core.mu.Lock()
	j.releaseMatchedLocked()
	j.core.releaseLocked()
	j.core.mu.Unlock()
}

func (j *StableExternalHashJoin) releaseMatchedLocked() {
	for i, matched := range j.residentMatched {
		if matched != nil {
			matched.Release()
			j.residentMatched[i] = nil
		}
	}
}
func (j *StableExternalHashJoin) failLocked(err error) error {
	if j.core.failed == nil {
		j.core.failed = err
	}
	j.releaseMatchedLocked()
	j.core.releaseLocked()
	return err
}

type stableExternalBuildSink struct {
	join  *StableExternalHashJoin
	inner *externalBuildSink
}
func (s *stableExternalBuildSink) Consume(ctx context.Context, chunk *vector.DataChunk) error {
	return s.inner.Consume(ctx, chunk)
}
func (s *stableExternalBuildSink) Finalize(ctx context.Context) error {
	if err := s.inner.Finalize(ctx); err != nil {
		return err
	}
	if s.join.core.config.Type != Full {
		return nil
	}
	j := s.join
	j.core.mu.Lock()
	defer j.core.mu.Unlock()
	for i := range j.core.partitions {
		partition := &j.core.partitions[i]
		if partition.spilled || partition.resident == nil || partition.resident.Count() == 0 {
			continue
		}
		matched, err := newMatchedBitmap(j.core.config.Scope, j.core.config.Buffers, partition.resident.Count())
		if err != nil {
			return j.failLocked(err)
		}
		j.residentMatched[i] = matched
	}
	return nil
}

// Close 使 build pipeline source/error 路径能够通过 CloseableSink 回收 join ownership。
func (s *stableExternalBuildSink) Close() error { s.join.Close(); return nil }

type stableExternalProbeSink struct {
	join       *StableExternalHashJoin
	downstream pipeline.Sink
}

func (s *stableExternalProbeSink) Finished() bool { return pipeline.SinkFinished(s.downstream) }

// Consume 保持 outer/semi/anti 语义，并让 INNER 复用已验证的整数直接索引。
func (s *stableExternalProbeSink) Consume(ctx context.Context, chunk *vector.DataChunk) error {
	j := s.join
	core := j.core
	core.mu.Lock()
	if err := ctx.Err(); err != nil {
		core.mu.Unlock()
		return err
	}
	if core.failed != nil {
		err := core.failed
		core.mu.Unlock()
		return err
	}
	if core.closed || !core.built {
		core.mu.Unlock()
		err := errors.New("external join build not finalized")
		core.recordProbeFailure(err)
		return err
	}
	if len(chunk.Columns) != len(core.probeTypes) {
		core.mu.Unlock()
		err := errors.New("external join probe column count mismatch")
		core.recordProbeFailure(err)
		return err
	}
	core.mu.Unlock()

	if s.Finished() {
		return ctx.Err()
	}
	emitter := newBatchEmitter(core.config.Type, core.config.Scope, core.config.Buffers, core.config.LeftTypes, core.config.RightTypes, core.buildSide, core.buildLayout, s.downstream)
	emitter.required = core.config.RequiredOutput
	if err := core.probeChunk(ctx, chunk, &emitter, j); err != nil {
		emitter.abort()
		core.recordProbeFailure(err)
		return err
	}
	core.probeRows.Add(uint64(chunk.Cardinality))
	if err := emitter.flush(ctx); err != nil { emitter.abort(); core.recordProbeFailure(err); return err }
	return nil
}

func (s *stableExternalProbeSink) Finalize(ctx context.Context) error {
	j := s.join
	core := j.core
	core.mu.Lock()
	defer core.mu.Unlock()
	if err := ctx.Err(); err != nil { return j.failLocked(err) }
	if core.failed != nil { return core.failed }
	if core.closed || !core.built { return j.failLocked(errors.New("external join not ready for finalize")) }
	if s.Finished() {
		return s.finishLocked(ctx)
	}
	for i := range core.partitions {
		partition := &core.partitions[i]
		if partition.probeSpill != nil {
			if err := partition.probeSpill.Close(ctx); err != nil { return j.failLocked(err) }
		}
	}
	emitter := newStableStoredEmitter(j, s.downstream)
	for i := range core.partitions {
		if s.Finished() {
			break
		}
		partition := &core.partitions[i]
		if !partition.spilled { continue }
		buildSet := partition.buildSpill.set()
		var probeSet partitionSet
		if partition.probeSpill != nil { probeSet = partition.probeSpill.set() }
		if err := j.processSpilled(ctx, buildSet, probeSet, 0, 0, emitter); err != nil { emitter.abort(); return j.failLocked(err) }
		partition.buildSpill.pages = nil
		if partition.probeSpill != nil { partition.probeSpill.pages = nil }
	}
	if core.config.Type == Full && !s.Finished() {
		for i := range core.partitions {
			partition := &core.partitions[i]
			if partition.spilled || partition.resident == nil { continue }
			reader := partition.resident.borrowReader()
			for row := 0; row < partition.resident.Count(); row++ {
				if !isMatched(j.residentMatched[i], row) {
					_, buildRow, ok := reader.record(row)
					if !ok { emitter.abort(); return j.failLocked(errors.New("invalid resident build record")) }
					if err := emitter.emitUnmatchedBuild(ctx, buildRow); err != nil { emitter.abort(); return j.failLocked(err) }
				}
			}
		}
	}
	if err := emitter.flush(ctx); err != nil { emitter.abort(); return j.failLocked(err) }
	return s.finishLocked(ctx)
}

func (s *stableExternalProbeSink) finishLocked(ctx context.Context) error {
	j := s.join
	if err := s.downstream.Finalize(ctx); err != nil {
		return j.failLocked(err)
	}
	j.releaseMatchedLocked()
	j.core.releaseLocked()
	return nil
}
func (s *stableExternalProbeSink) Close() error { s.join.Close(); return nil }

func (j *StableExternalHashJoin) processSpilled(ctx context.Context, buildSet, probeSet partitionSet, depth int, parentBytes uint64, emitter *stableStoredEmitter) error {
	if buildSet.rows == 0 || probeSet.rows == 0 {
		if buildSet.rows == 0 && probeSet.rows > 0 {
			if err := j.emitProbeSetUnmatched(ctx, probeSet, emitter); err != nil { return err }
		}
		if probeSet.rows == 0 && buildSet.rows > 0 && j.core.config.Type == Full {
			if err := j.emitBuildSetUnmatched(ctx, buildSet, emitter); err != nil { return err }
		}
		if err := j.core.deleteSet(buildSet); err != nil { return err }
		return j.core.deleteSet(probeSet)
	}
	if buildSet.bytes <= uint64(j.core.config.MemoryBudget/4) {
		return j.joinInMemorySet(ctx, buildSet, probeSet, emitter)
	}
	decision := j.core.policy.Decide(depth, parentBytes, buildSet.bytes, uint64(j.core.config.MemoryBudget/4))
	if decision.Action == exechash.PartitionTerminalSkew {
		return j.terminalNestedLoop(ctx, buildSet, probeSet, emitter)
	}
	nextDepth := depth + 1
	buildChildren, histogram, err := j.core.repartitionSet(ctx, buildSet, nextDepth, true)
	if err != nil { return err }
	defer releasePartitionSetMetadata(buildChildren)
	probeChildren, _, err := j.core.repartitionSet(ctx, probeSet, nextDepth, false)
	if err != nil { histogram.Release(); return err }
	defer releasePartitionSetMetadata(probeChildren)
	j.core.stats.Repartitions++
	for partition := range buildChildren {
		bset, pset := buildChildren[partition], probeChildren[partition]
		if bset.rows == 0 || pset.rows == 0 {
			if err := j.processSpilled(ctx, bset, pset, nextDepth, buildSet.bytes, emitter); err != nil { histogram.Release(); return err }
			continue
		}
		stat, err := histogram.Stat(partition)
		if err != nil { histogram.Release(); return err }
		childDecision := j.core.policy.Decide(nextDepth, buildSet.bytes, stat.Bytes, uint64(j.core.config.MemoryBudget/4))
		if childDecision.Action == exechash.PartitionTerminalSkew {
			if err := j.terminalNestedLoop(ctx, bset, pset, emitter); err != nil { histogram.Release(); return err }
			continue
		}
		if err := j.processSpilled(ctx, bset, pset, nextDepth, buildSet.bytes, emitter); err != nil { histogram.Release(); return err }
	}
	histogram.Release()
	return nil
}

func (j *StableExternalHashJoin) joinInMemorySet(ctx context.Context, buildSet, probeSet partitionSet, emitter *stableStoredEmitter) error {
	build, index, err := j.core.loadBuild(ctx, buildSet)
	if err != nil { return err }
	defer build.Release(); defer index.Release()
	reader := build.borrowReader()
	var buildMatched *memory.Buffer
	if j.core.config.Type == Full {
		buildMatched, err = newMatchedBitmap(j.core.config.Scope, j.core.config.Buffers, build.Count())
		if err != nil { return err }
		defer buildMatched.Release()
	}
	for i := 0; i < probeSet.pages.Count(); i++ {
		id, _ := probeSet.pages.At(i)
		buf, err := j.core.store.ReadPage(ctx, id)
		if err != nil { return err }
		err = scanRecords(buf.Bytes(), func(hash uint64, probeRow []byte) error {
			matched, err := j.matchLoaded(ctx, reader, index, buildMatched, probeRow, hash, emitter)
			if err != nil { return err }
			if !matched { return emitter.emitUnmatchedProbe(ctx, probeRow) }
			return nil
		})
		buf.Release()
		if err != nil { return err }
	}
	if j.core.config.Type == Full {
		for row := 0; row < build.Count(); row++ {
			if !isMatched(buildMatched, row) {
				_, buildRow, ok := reader.record(row); if !ok { return errors.New("invalid loaded build record") }
				if err := emitter.emitUnmatchedBuild(ctx, buildRow); err != nil { return err }
			}
		}
	}
	if err := j.core.deleteSet(buildSet); err != nil { return err }
	return j.core.deleteSet(probeSet)
}

func (j *StableExternalHashJoin) matchLoaded(ctx context.Context, build recordReader, index *rowHashTable, buildMatched *memory.Buffer, probeRow []byte, hash uint64, emitter *stableStoredEmitter) (bool, error) {
	matched := false
	buildIndex, ok := index.Head(hash)
	for ok {
		_, buildRow, valid := build.record(buildIndex); if !valid { return false, errors.New("invalid build record") }
		equal, err := exechash.EqualProjectedRows(j.core.buildLayout, buildRow, j.core.buildKeys, j.core.probeLayout, probeRow, j.core.probeKeys, j.core.nullEquality())
		if err != nil { return false, err }
		if equal {
			matched = true
			if buildMatched != nil { setMatched(buildMatched, buildIndex) }
			if err := emitter.emitMatch(ctx, buildRow, probeRow, matched); err != nil { return false, err }
			if j.core.config.Type == Semi || j.core.config.Type == Anti { break }
		}
		buildIndex, ok = index.Next(buildIndex)
	}
	return matched, nil
}

// terminalNestedLoop 在 skew 终态保持 bounded page processing；FULL 的 build match 仅保存一 bit/row。
func (j *StableExternalHashJoin) terminalNestedLoop(ctx context.Context, buildSet, probeSet partitionSet, emitter *stableStoredEmitter) error {
	j.core.stats.TerminalSkewPartitions++
	if buildSet.rows > uint64(math.MaxInt) { return errors.New("terminal join build row count exceeds int range") }
	var buildMatched *memory.Buffer
	var err error
	if j.core.config.Type == Full {
		buildMatched, err = newMatchedBitmap(j.core.config.Scope, j.core.config.Buffers, int(buildSet.rows))
		if err != nil { return err }
		defer buildMatched.Release()
	}
	for probePage := 0; probePage < probeSet.pages.Count(); probePage++ {
		probeID, _ := probeSet.pages.At(probePage)
		probeBuf, err := j.core.store.ReadPage(ctx, probeID); if err != nil { return err }
		probeCount := 0
		if err := scanRecords(probeBuf.Bytes(), func(uint64, []byte) error { probeCount++; return nil }); err != nil { probeBuf.Release(); return err }
		probeMatched, err := newMatchedBitmap(j.core.config.Scope, j.core.config.Buffers, probeCount); if err != nil { probeBuf.Release(); return err }
		buildBase := 0
		for buildPage := 0; buildPage < buildSet.pages.Count(); buildPage++ {
			buildID, _ := buildSet.pages.At(buildPage)
			buildBuf, err := j.core.store.ReadPage(ctx, buildID); if err != nil { probeMatched.Release(); probeBuf.Release(); return err }
			localBuild := 0
			err = scanRecords(buildBuf.Bytes(), func(buildHash uint64, buildRow []byte) error {
				buildIndex := buildBase + localBuild; localBuild++
				probeIndex := 0
				return scanRecords(probeBuf.Bytes(), func(probeHash uint64, probeRow []byte) error {
					idx := probeIndex; probeIndex++
					if buildHash != probeHash { return nil }
					equal, err := exechash.EqualProjectedRows(j.core.buildLayout, buildRow, j.core.buildKeys, j.core.probeLayout, probeRow, j.core.probeKeys, j.core.nullEquality())
					if err != nil || !equal { return err }
					if (j.core.config.Type == Semi || j.core.config.Type == Anti) && isMatched(probeMatched, idx) { return nil }
					setMatched(probeMatched, idx)
					if buildMatched != nil { setMatched(buildMatched, buildIndex) }
					return emitter.emitMatch(ctx, buildRow, probeRow, true)
				})
			})
			buildBuf.Release(); if err != nil { probeMatched.Release(); probeBuf.Release(); return err }
			buildBase += localBuild
		}
		probeIndex := 0
		err = scanRecords(probeBuf.Bytes(), func(_ uint64, probeRow []byte) error {
			matched := isMatched(probeMatched, probeIndex); probeIndex++
			if !matched { return emitter.emitUnmatchedProbe(ctx, probeRow) }
			return nil
		})
		probeMatched.Release(); probeBuf.Release(); if err != nil { return err }
	}
	if j.core.config.Type == Full {
		buildIndex := 0
		for buildPage := 0; buildPage < buildSet.pages.Count(); buildPage++ {
			buildID, _ := buildSet.pages.At(buildPage)
			buildBuf, err := j.core.store.ReadPage(ctx, buildID); if err != nil { return err }
			err = scanRecords(buildBuf.Bytes(), func(_ uint64, buildRow []byte) error {
				idx := buildIndex; buildIndex++
				if !isMatched(buildMatched, idx) { return emitter.emitUnmatchedBuild(ctx, buildRow) }
				return nil
			})
			buildBuf.Release(); if err != nil { return err }
		}
	}
	if err := j.core.deleteSet(buildSet); err != nil { return err }
	return j.core.deleteSet(probeSet)
}

func (j *StableExternalHashJoin) emitProbeSetUnmatched(ctx context.Context, set partitionSet, emitter *stableStoredEmitter) error {
	if !j.probeIsPreservedOrAnti() { return nil }
	if set.pages == nil { return nil }
	for i := 0; i < set.pages.Count(); i++ {
		id, _ := set.pages.At(i); buf, err := j.core.store.ReadPage(ctx, id); if err != nil { return err }
		err = scanRecords(buf.Bytes(), func(_ uint64, row []byte) error { return emitter.emitUnmatchedProbe(ctx, row) })
		buf.Release(); if err != nil { return err }
	}
	return nil
}
func (j *StableExternalHashJoin) emitBuildSetUnmatched(ctx context.Context, set partitionSet, emitter *stableStoredEmitter) error {
	if set.pages == nil { return nil }
	for i := 0; i < set.pages.Count(); i++ {
		id, _ := set.pages.At(i); buf, err := j.core.store.ReadPage(ctx, id); if err != nil { return err }
		err = scanRecords(buf.Bytes(), func(_ uint64, row []byte) error { return emitter.emitUnmatchedBuild(ctx, row) })
		buf.Release(); if err != nil { return err }
	}
	return nil
}
func (j *StableExternalHashJoin) probeIsPreservedOrAnti() bool {
	switch j.core.config.Type { case Left, Right, Full, Anti: return true; default: return false }
}

func newMatchedBitmap(scope *memory.Scope, buffers *memory.BufferManager, rows int) (*memory.Buffer, error) {
	if rows < 0 { return nil, errors.New("negative matched bitmap rows") }
	bytes := (rows + 7) / 8; if bytes < 1 { bytes = 1 }
	return buffers.Allocate(scope, memory.TagHashTable, bytes)
}
func setMatched(bitmap *memory.Buffer, row int) {
	if bitmap == nil || row < 0 { return }
	bitmap.Bytes()[row>>3] |= byte(1 << uint(row&7))
}
func isMatched(bitmap *memory.Buffer, row int) bool {
	if bitmap == nil || row < 0 { return false }
	return bitmap.Bytes()[row>>3]&(byte(1)<<uint(row&7)) != 0
}

type stableStoredEmitter struct {
	join       *StableExternalHashJoin
	downstream pipeline.Sink
	cols       []*vector.Vector
	count      int
}
func newStableStoredEmitter(join *StableExternalHashJoin, downstream pipeline.Sink) *stableStoredEmitter { return &stableStoredEmitter{join: join, downstream: downstream} }
func (e *stableStoredEmitter) outputTypes() []types.LogicalType {
	if e.join.core.config.Type == Semi || e.join.core.config.Type == Anti { return e.join.core.config.LeftTypes }
	out := make([]types.LogicalType, 0, len(e.join.core.config.LeftTypes)+len(e.join.core.config.RightTypes)); out=append(out,e.join.core.config.LeftTypes...); out=append(out,e.join.core.config.RightTypes...); return out
}
func (e *stableStoredEmitter) ensure() error {
	if e.cols != nil { return nil }
	cols, err := newOutputColumns(e.join.core.config.Scope, e.join.core.config.Buffers, e.outputTypes(), e.join.core.config.RequiredOutput, vector.DefaultVectorSize)
	if err != nil { return err }
	e.cols = cols
	return nil
}
func (e *stableStoredEmitter) emitMatch(ctx context.Context, buildRow, probeRow []byte, _ bool) error {
	if e.join.core.config.Type == Anti { return nil }
	if e.join.core.config.Type == Semi { return e.appendProbeOnly(ctx, probeRow) }
	return e.appendPair(ctx, buildRow, probeRow)
}
func (e *stableStoredEmitter) emitUnmatchedProbe(ctx context.Context, probeRow []byte) error {
	switch e.join.core.config.Type { case Left,Right,Full,Anti: return e.appendProbeOnly(ctx,probeRow); default:return nil }
}
func (e *stableStoredEmitter) emitUnmatchedBuild(ctx context.Context, buildRow []byte) error {
	if e.join.core.config.Type != Full { return nil }; return e.appendBuildOnly(ctx,buildRow)
}
func (e *stableStoredEmitter) appendPair(ctx context.Context, buildRow, probeRow []byte) error {
	if err:=ctx.Err();err!=nil{return err};if err:=e.ensure();err!=nil{return err};left:=e.cols[:len(e.join.core.config.LeftTypes)];right:=e.cols[len(e.join.core.config.LeftTypes):]
	if e.join.core.buildSide==RightSide{if err:=e.join.core.probeLayout.MaterializeSelectedRow(probeRow,left,e.count,neededColumns(e.join.core.config.RequiredOutput,0,len(left)));err!=nil{return err};if err:=e.join.core.buildLayout.MaterializeSelectedRow(buildRow,right,e.count,neededColumns(e.join.core.config.RequiredOutput,len(e.join.core.config.LeftTypes),len(right)));err!=nil{return err}}else{if err:=e.join.core.buildLayout.MaterializeSelectedRow(buildRow,left,e.count,neededColumns(e.join.core.config.RequiredOutput,0,len(left)));err!=nil{return err};if err:=e.join.core.probeLayout.MaterializeSelectedRow(probeRow,right,e.count,neededColumns(e.join.core.config.RequiredOutput,len(e.join.core.config.LeftTypes),len(right)));err!=nil{return err}}
	return e.advance(ctx)
}
func (e *stableStoredEmitter) appendProbeOnly(ctx context.Context, probeRow []byte) error {
	if err:=ctx.Err();err!=nil{return err};if err:=e.ensure();err!=nil{return err}
	if e.join.core.config.Type==Semi||e.join.core.config.Type==Anti { if e.join.core.buildSide!=RightSide{return errors.New("semi/anti external join requires left probe")};if err:=e.join.core.probeLayout.MaterializeSelectedRow(probeRow,e.cols,e.count,neededColumns(e.join.core.config.RequiredOutput,0,len(e.cols)));err!=nil{return err};return e.advance(ctx) }
	left:=e.cols[:len(e.join.core.config.LeftTypes)];right:=e.cols[len(e.join.core.config.LeftTypes):]
	if e.join.core.buildSide==RightSide { if err:=e.join.core.probeLayout.MaterializeSelectedRow(probeRow,left,e.count,neededColumns(e.join.core.config.RequiredOutput,0,len(left)));err!=nil{return err};setNullRow(right,e.count) } else { setNullRow(left,e.count);if err:=e.join.core.probeLayout.MaterializeSelectedRow(probeRow,right,e.count,neededColumns(e.join.core.config.RequiredOutput,len(e.join.core.config.LeftTypes),len(right)));err!=nil{return err} }
	return e.advance(ctx)
}
func (e *stableStoredEmitter) appendBuildOnly(ctx context.Context, buildRow []byte) error {
	if err:=ctx.Err();err!=nil{return err};if err:=e.ensure();err!=nil{return err};left:=e.cols[:len(e.join.core.config.LeftTypes)];right:=e.cols[len(e.join.core.config.LeftTypes):]
	if e.join.core.buildSide==RightSide { setNullRow(left,e.count);if err:=e.join.core.buildLayout.MaterializeSelectedRow(buildRow,right,e.count,neededColumns(e.join.core.config.RequiredOutput,len(e.join.core.config.LeftTypes),len(right)));err!=nil{return err} } else { if err:=e.join.core.buildLayout.MaterializeSelectedRow(buildRow,left,e.count,neededColumns(e.join.core.config.RequiredOutput,0,len(left)));err!=nil{return err};setNullRow(right,e.count) }
	return e.advance(ctx)
}
func setNullRow(columns []*vector.Vector,row int){for _,column:=range columns{if column.Kind != vector.Constant {column.SetNull(row)}}}
func (e *stableStoredEmitter) advance(ctx context.Context) error { e.count++;if e.count==vector.DefaultVectorSize{return e.flush(ctx)};return nil }
func (e *stableStoredEmitter) flush(ctx context.Context) error {
	if e.count == 0 {
		return nil
	}
	chunk := &vector.DataChunk{Columns: e.cols, Cardinality: e.count}
	err := pipeline.ConsumeOwnedChunk(ctx, e.downstream, chunk)
	e.cols = nil
	e.count = 0
	return err
}
func (e *stableStoredEmitter) abort(){for _,v:=range e.cols{if v!=nil{v.Release()}};e.cols=nil;e.count=0}
