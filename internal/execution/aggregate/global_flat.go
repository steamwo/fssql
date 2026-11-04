package aggregate

import (
	"context"
	"encoding/binary"
	"math"

	"github.com/steamwo/fssql/internal/core/types"
	"github.com/steamwo/fssql/internal/core/vector"
	"github.com/steamwo/fssql/internal/semantics"
)

// Flat float SUM/AVG has no representation/type dispatch or buffer acquisition
// in its loop. Other representations keep the existing generic reduction.
func (a *HashAggregate) updateGlobalFlatSum(ctx context.Context, st []byte, b BoundSpec, chunk *vector.DataChunk) (bool, error) {
	if (b.Spec.Kind != Sum && b.Spec.Kind != Avg) || (b.InputType.ID != types.Float && b.InputType.ID != types.Double) {
		return false, nil
	}
	view, ok := chunk.Columns[b.Spec.Input].BorrowFixedWidth()
	if !ok {
		return false, nil
	}
	data, mask, flat := view.FlatData()
	if !flat {
		return false, nil
	}
	total := math.Float64frombits(binary.LittleEndian.Uint64(st[16:24]))
	valid := 0
	allValid := mask.AllValid(chunk.Cardinality)
	if b.InputType.ID == types.Double {
		for row := 0; row < chunk.Cardinality; row++ {
			if row&255 == 0 {
				if err := ctx.Err(); err != nil {
					return true, err
				}
			}
			if allValid || mask.IsValid(row) {
				total += math.Float64frombits(binary.LittleEndian.Uint64(data[row*8 : row*8+8]))
				valid++
			}
		}
	} else {
		for row := 0; row < chunk.Cardinality; row++ {
			if row&255 == 0 {
				if err := ctx.Err(); err != nil {
					return true, err
				}
			}
			if allValid || mask.IsValid(row) {
				total += float64(math.Float32frombits(binary.LittleEndian.Uint32(data[row*4 : row*4+4])))
				valid++
			}
		}
	}
	if valid == 0 {
		return true, nil
	}
	if b.Spec.Kind == Avg {
		count := binary.LittleEndian.Uint64(st[8:16])
		if uint64(valid) > math.MaxUint64-count {
			return true, semantics.ErrOverflow
		}
		binary.LittleEndian.PutUint64(st[8:16], count+uint64(valid))
	}
	st[0] = 1
	binary.LittleEndian.PutUint64(st[16:24], math.Float64bits(total))
	return true, nil
}
