package aggregate

import (
	"context"
	"errors"
	"fmt"

	"github.com/steamwo/fssql/internal/core/memory"
	"github.com/steamwo/fssql/internal/core/vector"
	"github.com/steamwo/fssql/internal/execution/pipeline"
)

const maxAggregateWorkerShards = 256

// ConcurrentConsume lets the blocking bridge run worker-local Consume calls in
// parallel. Each pipeline worker owns one shard; emit merges their states or
// replays partial states that rebalance spilled to partitions.
func (e *ExternalAggregate) ConcurrentConsume() bool { return true }

func (e *ExternalAggregate) resetShardsToLocal() {
	e.shardMu.Lock()
	defer e.shardMu.Unlock()
	if e.local == nil {
		e.shards = nil
		e.shardBytes = nil
		e.totalBytes = 0
		return
	}
	bytes := e.local.ManagedBytes()
	e.shards = []*HashAggregate{e.local}
	e.shardBytes = []int64{bytes}
	e.totalBytes = bytes
}

// shardForWorker 为 worker 分配独立状态，缓存与状态一起纳入共享聚合预算。
func (e *ExternalAggregate) shardForWorker(ctx context.Context) (int, *HashAggregate, error) {
	worker, ok := pipeline.WorkerID(ctx)
	if !ok || worker < 0 {
		worker = 0
	}
	if worker >= maxAggregateWorkerShards {
		return 0, nil, fmt.Errorf("aggregate worker shard %d exceeds limit %d", worker, maxAggregateWorkerShards)
	}
	e.shardMu.Lock()
	defer e.shardMu.Unlock()
	for len(e.shards) <= worker {
		shard, err := NewHashAggregate(e.cfg.Scope, e.cfg.Buffers, e.cfg.InputTypes, e.cfg.GroupColumns, e.cfg.Specs, max(e.cfg.EstimatedGroups, 8))
		if err != nil {
			return 0, nil, err
		}
		shard.groupCacheBudget = e.cfg.MemoryBudget / 16
		bytes := shard.ManagedBytes()
		e.shards = append(e.shards, shard)
		e.shardBytes = append(e.shardBytes, bytes)
		e.totalBytes += bytes
	}
	return worker, e.shards[worker], nil
}

func (e *ExternalAggregate) consumeWorker(ctx context.Context, chunk *vector.DataChunk) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	e.shardGate.RLock()
	if e.finalized {
		e.shardGate.RUnlock()
		return errors.New("external aggregate finalized")
	}
	index, shard, err := e.shardForWorker(ctx)
	if err != nil {
		e.shardGate.RUnlock()
		return err
	}
	consumeErr := shard.Consume(ctx, chunk)
	var total int64
	if consumeErr == nil {
		bytes := shard.ManagedBytes()
		e.shardMu.Lock()
		total = e.totalBytes + bytes - e.shardBytes[index]
		e.shardBytes[index] = bytes
		e.totalBytes = total
		e.shardMu.Unlock()
		e.updatePeak(total)
	}
	e.shardGate.RUnlock()
	if consumeErr != nil {
		return consumeErr
	}
	if total > e.cfg.MemoryBudget {
		return e.rebalance(ctx)
	}
	return nil
}

// rebalance spills independent partial states without first growing a merged
// hash table. Partition replay combines equal groups after the shards are freed.
func (e *ExternalAggregate) rebalance(ctx context.Context) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	e.mergeMu.Lock()
	defer e.mergeMu.Unlock()
	e.shardGate.Lock()
	defer e.shardGate.Unlock()
	if e.finalized {
		return errors.New("external aggregate finalized")
	}
	e.shardMu.Lock()
	total := e.totalBytes
	e.shardMu.Unlock()
	if total <= e.cfg.MemoryBudget {
		return nil
	}
	return e.spillShardsLocked(ctx)
}

// mergeLocked and spillShardsLocked require the exclusive shardGate barrier.
func (e *ExternalAggregate) mergeLocked(ctx context.Context) error {
	if len(e.shards) == 0 {
		if e.local == nil {
			return errors.New("aggregate has no local state")
		}
		e.shards = []*HashAggregate{e.local}
		e.shardBytes = []int64{e.local.ManagedBytes()}
		e.totalBytes = e.shardBytes[0]
		return nil
	}
	primary := 0
	for i := 1; i < len(e.shards); i++ {
		if e.shards[i] != nil && (e.shards[primary] == nil || e.shards[i].GroupCount() > e.shards[primary].GroupCount()) {
			primary = i
		}
	}
	if e.shards[primary] == nil {
		return errors.New("aggregate shard is nil")
	}
	e.local = e.shards[primary]
	for i, shard := range e.shards {
		if i == primary || shard == nil {
			continue
		}
		if err := ctx.Err(); err != nil {
			return err
		}
		if shard.GroupCount() > 0 {
			// Prepare every allocation before changing aggregate values. On a
			// capacity failure the source is still wholly unmerged, so spilling
			// both states cannot duplicate counts or sums.
			if err := e.local.prepareCombine(ctx, shard); err != nil {
				if errors.Is(err, memory.ErrLimitExceeded) {
					return e.spillShardsLocked(ctx)
				}
				return err
			}
			if err := e.local.CombineStates(ctx, shard); err != nil {
				return err
			}
		}
		shard.Release()
		e.shards[i] = nil
	}
	e.local = e.shards[primary]
	e.shards = []*HashAggregate{e.local}
	e.shardBytes = []int64{e.local.ManagedBytes()}
	e.totalBytes = e.shardBytes[0]
	return nil
}

// prepareCombine may add empty group keys, but never changes aggregate values.
// Empty states are neutral when spilled alongside the complete source state.
func (a *HashAggregate) prepareCombine(ctx context.Context, other *HashAggregate) error {
	// A merge copies at most the source's live string states; arenaUsed also
	// includes obsolete extrema and is therefore a conservative upper bound.
	if err := a.ensureArena(other.arenaUsed); err != nil {
		return err
	}
	for group := 0; group < other.GroupCount(); group++ {
		if err := ctx.Err(); err != nil {
			return err
		}
		row := other.groups.Row(group)
		hash, err := a.groupLayout.HashRow(row)
		if err != nil {
			return err
		}
		if _, err := a.findOrInsertEncoded(row, hash); err != nil {
			return err
		}
	}
	return nil
}

func (e *ExternalAggregate) spillShardsLocked(ctx context.Context) error {
	// Hash indexes and input scratch are unnecessary for encoding partial
	// states. Free them first to leave room for spill buffers even when a
	// failed merge preparation exhausted the available managed memory.
	e.local = nil
	for _, shard := range e.shards {
		if shard == nil {
			continue
		}
		if shard.table != nil {
			shard.table.Release()
			shard.table = nil
		}
		if shard.scratch != nil {
			shard.scratch.Release()
			shard.scratch = nil
		}
	}
	for i, shard := range e.shards {
		if shard == nil {
			continue
		}
		if err := e.spillAggregate(ctx, shard); err != nil {
			return err
		}
		shard.Release()
		e.shards[i] = nil
	}
	if err := e.resetLocal(); err != nil {
		return err
	}
	e.resetShardsToLocal()
	return nil
}
