package aggregate

import (
	"context"
	"encoding/binary"
	"math"

	"github.com/steamwo/fssql/internal/core/types"
	"github.com/steamwo/fssql/internal/core/vector"
	"github.com/steamwo/fssql/internal/semantics"
)

// Keep the accumulator in registers for a complete batch. The input view
// borrows the existing vector; no payload copy or new reservation is needed.
func (a *HashAggregate) updateGlobalFixed(ctx context.Context, st []byte, b BoundSpec, chunk *vector.DataChunk) (bool, error) {
	if handled, err := a.updateGlobalFlatSum(ctx, st, b, chunk); handled {
		return true, err
	}
	v := chunk.Columns[b.Spec.Input]
	view, ok := v.BorrowFixedWidth()
	if !ok {
		return false, nil
	}
	kind, typ := b.Spec.Kind, b.InputType
	count := binary.LittleEndian.Uint64(st[8:16])
	total := types.Int128{Lo: binary.LittleEndian.Uint64(st[16:24]), Hi: int64(binary.LittleEndian.Uint64(st[24:32]))}
	floatTotal := math.Float64frombits(total.Lo)
	seen := st[0] != 0
	var best [16]byte
	copy(best[:], st[16:32])
	for row := 0; row < chunk.Cardinality; row++ {
		if row&255 == 0 {
			if err := ctx.Err(); err != nil {
				return true, err
			}
		}
		data, valid := view.Value(row)
		if !valid {
			continue
		}
		switch kind {
		case Count:
			count++
		case Sum, Avg:
			if kind == Avg {
				if count == math.MaxUint64 {
					return true, semantics.ErrOverflow
				}
				count++
			}
			if typ.ID == types.Float {
				floatTotal += float64(math.Float32frombits(binary.LittleEndian.Uint32(data)))
			} else if typ.ID == types.Double {
				floatTotal += math.Float64frombits(binary.LittleEndian.Uint64(data))
			} else {
				var err error
				total, err = types.AddInt128(total, aggregateInteger(typ, data))
				if err != nil {
					return true, semantics.ErrOverflow
				}
			}
		case Min, Max:
			cmp := 0
			if seen {
				cmp = compareAggregateFixed(typ, best[:], data)
			}
			if !seen || kind == Min && cmp > 0 || kind == Max && cmp < 0 {
				copy(best[:], data)
			}
		default:
			return false, nil
		}
		seen = true
	}
	if !seen {
		return true, nil
	}
	st[0] = 1
	binary.LittleEndian.PutUint64(st[8:16], count)
	switch kind {
	case Sum, Avg:
		if typ.ID == types.Float || typ.ID == types.Double {
			binary.LittleEndian.PutUint64(st[16:24], math.Float64bits(floatTotal))
		} else {
			binary.LittleEndian.PutUint64(st[16:24], total.Lo)
			binary.LittleEndian.PutUint64(st[24:32], uint64(total.Hi))
		}
	case Min, Max:
		copy(st[16:32], best[:])
	}
	return true, nil
}

func aggregateInteger(typ types.LogicalType, data []byte) types.Int128 {
	if typ.IsUnsignedInteger() {
		switch typ.ID {
		case types.TinyInt:
			return types.Int128{Lo: uint64(data[0])}
		case types.SmallInt:
			return types.Int128{Lo: uint64(binary.LittleEndian.Uint16(data))}
		case types.Integer:
			return types.Int128{Lo: uint64(binary.LittleEndian.Uint32(data))}
		case types.BigInt:
			return types.Int128{Lo: binary.LittleEndian.Uint64(data)}
		}
	}
	switch typ.ID {
	case types.Boolean:
		if data[0] != 0 {
			return types.Int128FromInt64(1)
		}
		return types.Int128{}
	case types.TinyInt:
		return types.Int128FromInt64(int64(int8(data[0])))
	case types.SmallInt:
		return types.Int128FromInt64(int64(int16(binary.LittleEndian.Uint16(data))))
	case types.Integer, types.DateType:
		return types.Int128FromInt64(int64(int32(binary.LittleEndian.Uint32(data))))
	case types.HugeInt:
		return types.Int128{Lo: binary.LittleEndian.Uint64(data), Hi: int64(binary.LittleEndian.Uint64(data[8:]))}
	case types.Decimal:
		if typ.Decimal.Width > 18 {
			return types.Int128{Lo: binary.LittleEndian.Uint64(data), Hi: int64(binary.LittleEndian.Uint64(data[8:]))}
		}
	}
	return types.Int128FromInt64(int64(binary.LittleEndian.Uint64(data)))
}

func compareAggregateFixed(typ types.LogicalType, left, right []byte) int {
	switch typ.ID {
	case types.Float:
		return semantics.CompareFloat32(math.Float32frombits(binary.LittleEndian.Uint32(left)), math.Float32frombits(binary.LittleEndian.Uint32(right)))
	case types.Double:
		return semantics.CompareFloat64(math.Float64frombits(binary.LittleEndian.Uint64(left)), math.Float64frombits(binary.LittleEndian.Uint64(right)))
	default:
		return types.CompareInt128(aggregateInteger(typ, left), aggregateInteger(typ, right))
	}
}
