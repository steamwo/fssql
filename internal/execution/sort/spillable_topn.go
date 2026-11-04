package sort

import (
	"context"
	"errors"

	"github.com/steamwo/fssql/internal/core/memory"
	"github.com/steamwo/fssql/internal/core/vector"
)

// SpillableTopN retains a bounded candidate heap while it fits. If a candidate
// cannot fit, the retained prefix winners and the unconsumed input suffix are
// sufficient to compute the same first N rows using the external sorter.
type SpillableTopN struct {
	cfg       Config
	n         int
	top       *TopN
	external  *ExternalSort
	finalized bool
}

func NewSpillableTopN(cfg Config, n int) (*SpillableTopN, error) {
	if cfg.Temp == nil {
		return nil, errors.New("spillable TopN requires temporary storage")
	}
	top, err := NewTopN(TopNConfig{InputTypes: cfg.InputTypes, Keys: cfg.Keys, N: n, MemoryBudget: cfg.MemoryBudget, Scope: cfg.Scope, Buffers: cfg.Buffers})
	if err == nil && n > 0 {
		// Reserve the final order indexes while checking the candidate budget,
		// rather than surprising a full heap with an extra allocation in Emit.
		if int64(n)*4 > cfg.MemoryBudget-top.managedBytesWith(0) {
			err = memory.ErrLimitExceeded
		} else {
			top.indices, err = newManagedIndices(cfg.Scope, cfg.Buffers, n)
		}
		if err != nil {
			top.Release()
			top = nil
		}
	}
	if err != nil && !errors.Is(err, memory.ErrLimitExceeded) {
		return nil, err
	}
	s := &SpillableTopN{cfg: cfg, n: n, top: top}
	if err != nil {
		s.external, err = New(cfg)
		if err != nil {
			return nil, err
		}
		s.external.emitLimit = n
	}
	return s, nil
}

func (s *SpillableTopN) Consume(ctx context.Context, chunk *vector.DataChunk) error {
	if s.finalized {
		return errors.New("spillable TopN finalized")
	}
	if s.external != nil {
		return s.consumeExternal(ctx, chunk, 0)
	}
	err := s.top.Consume(ctx, chunk)
	if err == nil || !errors.Is(err, memory.ErrLimitExceeded) {
		return err
	}
	start := s.top.nextRow
	if err := s.spillPrefix(ctx); err != nil {
		return err
	}
	// Previous rows in this chunk are represented by the retained prefix.
	// Replaying only the failed row and its suffix avoids double counting.
	return s.consumeExternal(ctx, chunk, start)
}

func (s *SpillableTopN) consumeExternal(ctx context.Context, chunk *vector.DataChunk, start int) error {
	if chunk == nil || len(chunk.Columns) != len(s.cfg.InputTypes) {
		return errors.New("spillable TopN input column count mismatch")
	}
	workspace, err := s.cfg.Scope.Reserve(memory.TagSpillIO, 0)
	if err != nil {
		return err
	}
	defer workspace.Release()
	for row := start; row < chunk.Cardinality; row++ {
		if err := ctx.Err(); err != nil {
			return err
		}
		for attempt := 0; attempt < 2; attempt++ {
			// Do not let builder capacity consume the space needed to sort
			// and spill it. The reservation is released before the actual I/O.
			block := int64(s.external.cfg.SpillBlockBytes)
			if block < 512 {
				block = 512
			}
			needed := int64(s.external.builder.Count()+1)*4 + block*2 + 4096
			err = workspace.Resize(needed)
			if err == nil {
				err = s.external.builder.AppendChunkRow(chunk, row)
			}
			_ = workspace.Resize(0)
			if !errors.Is(err, memory.ErrLimitExceeded) || s.external.builder.Count() == 0 || attempt != 0 {
				break
			}
			// Append reserves capacity before committing a row, so the same
			// row can be retried after spilling the already committed prefix.
			if spillErr := s.external.spillBuilder(ctx); spillErr != nil {
				return spillErr
			}
		}
		if err != nil {
			return err
		}
		s.external.updatePeak()
		if s.external.builderBytes() > s.cfg.MemoryBudget {
			if err := s.external.spillBuilder(ctx); err != nil {
				return err
			}
		}
	}
	return nil
}

func (s *SpillableTopN) spillPrefix(ctx context.Context) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	top := s.top
	// Heap metadata and encoding scratch are no longer needed. Release them
	// before allocating spill/index buffers, especially after a hard-cap failure.
	if top.meta != nil {
		top.meta.Release()
		top.meta = nil
	}
	if top.scratch != nil {
		top.scratch.Release()
		top.scratch = nil
	}
	external, err := New(s.cfg)
	if err != nil {
		return err
	}
	s.external = external
	external.emitLimit = s.n
	if len(top.entries) != 0 {
		indices, err := top.sortedIndexes()
		if err != nil {
			return err
		}
		defer indices.release()
		// A small bounded transfer buffer avoids reserving a full normal spill
		// block while the remaining candidate buffers are still live.
		block := s.cfg.SpillBlockBytes
		if block <= 0 || block > 4096 {
			block = 4096
		}
		writer, err := newRunWriter(external.store, s.cfg.Scope, s.cfg.Buffers, block)
		if err != nil {
			return err
		}
		defer writer.release()
		for i := 0; i < indices.n; i++ {
			index := indices.get(i)
			buf := top.entries[index]
			if err := writer.append(ctx, buf.Bytes()); err != nil {
				return err
			}
			buf.Release()
			top.entries[index] = nil
		}
		run, err := writer.close(ctx)
		if err != nil {
			return err
		}
		if err := external.appendRun(run); err != nil {
			run.pages.Release()
			return err
		}
	}
	top.Release()
	s.top = nil
	return nil
}

func (s *SpillableTopN) Emit(ctx context.Context, emit func(*vector.DataChunk) error) error {
	if s.finalized {
		return errors.New("spillable TopN finalized")
	}
	if emit == nil {
		return errors.New("spillable TopN emit callback required")
	}
	if s.top != nil {
		called := false
		err := s.top.Emit(ctx, func(chunk *vector.DataChunk) error {
			called = true
			return emit(chunk)
		})
		if err == nil {
			s.finalized = true
			return nil
		}
		// Never retry a consumer error or a partially delivered result stream.
		if called || !errors.Is(err, memory.ErrLimitExceeded) {
			return err
		}
		if err := s.spillPrefix(ctx); err != nil {
			return err
		}
	}
	err := s.external.Emit(ctx, emit)
	if err == nil {
		s.finalized = true
	}
	return err
}

func (s *SpillableTopN) Release() {
	if s.top != nil {
		s.top.Release()
		s.top = nil
	}
	if s.external != nil {
		s.external.Release()
		s.external = nil
	}
}
