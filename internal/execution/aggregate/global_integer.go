package aggregate

import (
	"context"
	"encoding/binary"
	"math"

	"github.com/steamwo/fssql/internal/core/types"
	"github.com/steamwo/fssql/internal/core/vector"
	"github.com/steamwo/fssql/internal/semantics"
)

// updateGlobalFlatInteger 将 BIGINT 的类型与向量分派移到批次外，保留逐值溢出语义。
func (a *HashAggregate) updateGlobalFlatInteger(ctx context.Context, state []byte, bound BoundSpec, chunk *vector.DataChunk) (bool, error) {
	if bound.InputType.ID != types.BigInt || bound.InputType.Unsigned {
		return false, nil
	}
	view, ok := chunk.Columns[bound.Spec.Input].BorrowFixedWidth()
	if !ok {
		return false, nil
	}
	data, mask, flat := view.FlatData()
	if !flat {
		return false, nil
	}
	if err := ctx.Err(); err != nil {
		return true, err
	}
	allValid := mask.AllValid(chunk.Cardinality)
	valid := 0
	count := binary.LittleEndian.Uint64(state[8:16])
	switch bound.Spec.Kind {
	case Count:
		for row := 0; row < chunk.Cardinality; row++ {
			if allValid || mask.IsValid(row) {
				valid++
			}
		}
		if valid != 0 {
			state[0] = 1
			binary.LittleEndian.PutUint64(state[8:16], count+uint64(valid))
		}
	case Min, Max:
		best := int64(binary.LittleEndian.Uint64(state[16:24]))
		seen, maximum := state[0] != 0, bound.Spec.Kind == Max
		for row := 0; row < chunk.Cardinality; row++ {
			if row&255 == 0 {
				if err := ctx.Err(); err != nil {
					return true, err
				}
			}
			if !allValid && !mask.IsValid(row) {
				continue
			}
			x := int64(binary.LittleEndian.Uint64(data[row*8:]))
			if !seen || maximum && x > best || !maximum && x < best {
				best = x
			}
			seen = true
		}
		if seen {
			state[0] = 1
			binary.LittleEndian.PutUint64(state[16:24], uint64(best))
		}
	case Sum, Avg:
		total := types.Int128{Lo: binary.LittleEndian.Uint64(state[16:24]), Hi: int64(binary.LittleEndian.Uint64(state[24:32]))}
		average := bound.Spec.Kind == Avg
		for row := 0; row < chunk.Cardinality; row++ {
			if row&255 == 0 {
				if err := ctx.Err(); err != nil {
					return true, err
				}
			}
			if !allValid && !mask.IsValid(row) {
				continue
			}
			if average {
				if count == math.MaxUint64 {
					return true, semantics.ErrOverflow
				}
				count++
			}
			x := int64(binary.LittleEndian.Uint64(data[row*8:]))
			var err error
			total, err = types.AddInt128(total, types.Int128FromInt64(x))
			if err != nil {
				return true, semantics.ErrOverflow
			}
			valid++
		}
		if valid != 0 {
			state[0] = 1
			binary.LittleEndian.PutUint64(state[8:16], count)
			binary.LittleEndian.PutUint64(state[16:24], total.Lo)
			binary.LittleEndian.PutUint64(state[24:32], uint64(total.Hi))
		}
	default:
		return false, nil
	}
	return true, ctx.Err()
}
