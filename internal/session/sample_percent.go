package session

import (
	"context"
	"fmt"
	"math"

	"github.com/steamwo/fssql/internal/core/vector"
)

// Reservoir percentages need the population size first. Keep the input in the
// spillable store, then reuse the fixed-size reservoir without retaining it all
// in memory.
func (s *sampleOperator) emitReservoirPercent(ctx context.Context, emit func(*vector.DataChunk) error) error {
	k := uint64(math.Round(float64(s.seen) * math.Float64frombits(s.spec.amount) / 100))
	if k == 0 {
		return nil
	}
	if k > s.seen {
		k = s.seen
	}
	seed := s.spec.seed
	if !s.spec.seeded {
		seed = s.rng.Uint64()
	}
	op, err := newSampleOperator(s.q, s.buffers, fmt.Sprintf("sample:r:%x:s:%x", k, seed))
	if err != nil {
		return err
	}
	defer op.Release()
	for _, id := range s.pages {
		if err := ctx.Err(); err != nil {
			return err
		}
		page, err := s.store.ReadPage(ctx, id)
		if err != nil {
			return err
		}
		chunk, err := vector.DeserializeChunk(page.Bytes(), s.q.Memory, s.buffers)
		page.Release()
		if err != nil {
			return err
		}
		err = op.Consume(ctx, chunk)
		chunk.Release()
		if err != nil {
			return err
		}
	}
	return op.Emit(ctx, emit)
}
