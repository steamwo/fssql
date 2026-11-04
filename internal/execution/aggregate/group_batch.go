package aggregate

import (
	"context"
	"encoding/binary"
	"errors"
	"math"

	"github.com/steamwo/fssql/internal/core/types"
	"github.com/steamwo/fssql/internal/core/vector"
	exechash "github.com/steamwo/fssql/internal/execution/hash"
	"github.com/steamwo/fssql/internal/semantics"
)

// findOrCreateGroups resolves every row in a batch against the hash table
// before any aggregate state is touched. Existing groups never encode their
// group key; only newly inserted groups pay the append/encoding cost.
func (a *HashAggregate) findOrCreateGroups(ctx context.Context, projected *vector.DataChunk, hashes []uint64, groupIDs []int) error {
	comparer, err := exechash.NewChunkKeyComparer(projected, a.groupKeys)
	if err != nil {
		return err
	}
	reader := a.groups.BorrowRowReader()
	for row := 0; row < projected.Cardinality; row++ {
		if row&255 == 0 {
			if err := ctx.Err(); err != nil {
				return err
			}
		}
		found, ok, err := a.table.lookup(hashes[row], func(existing int) (bool, error) {
			return comparer.Equal(row, a.groupLayout, reader(existing), a.groupKeys, exechash.NullsEqual)
		})
		if err != nil {
			return err
		}
		if ok {
			groupIDs[row] = found
			continue
		}
		if err := a.table.ensureInsert(); err != nil {
			return err
		}
		index := a.groups.Count()
		if err := a.ensureStates(index + 1); err != nil {
			return err
		}
		if err := a.groups.AppendChunkRow(projected, row); err != nil {
			return err
		}
		if err := a.table.putNoGrow(hashes[row], index); err != nil {
			return err
		}
		groupIDs[row] = index
		reader = a.groups.BorrowRowReader()
	}
	return nil
}

func (a *HashAggregate) stateAt(states []byte, group, agg int) []byte {
	offset := group*a.layout.StateWidth + a.layout.States[agg].Offset
	return states[offset : offset+aggregateStateWidth]
}

// updateGroupsBatch dispatches each aggregate once per batch, then runs a
// typed kernel over the resolved group ids.
func (a *HashAggregate) updateGroupsBatch(ctx context.Context, chunk *vector.DataChunk, groupIDs []int) error {
	if len(a.specs) == 0 {
		return nil
	}
	states := a.states.Bytes()
	for ai, spec := range a.specs {
		if err := a.updateGroupStates(ctx, states, ai, spec, chunk, groupIDs); err != nil {
			return err
		}
	}
	return nil
}

func (a *HashAggregate) updateGroupStates(ctx context.Context, states []byte, ai int, spec BoundSpec, chunk *vector.DataChunk, groupIDs []int) error {
	switch spec.Spec.Kind {
	case CountStar:
		return a.countGroupStates(ctx, states, ai, nil, groupIDs)
	case Count:
		return a.countGroupStates(ctx, states, ai, chunk.Columns[spec.Spec.Input], groupIDs)
	case Sum, Avg:
		return a.sumGroupStates(ctx, states, ai, spec, chunk, groupIDs)
	case Min, Max:
		return a.fallbackGroupStates(ctx, states, ai, spec, chunk, groupIDs)
	default:
		return errors.New("unknown aggregate kind")
	}
}

func (a *HashAggregate) countGroupStates(ctx context.Context, states []byte, ai int, v *vector.Vector, groupIDs []int) error {
	var view vector.FixedWidthView
	fixed := false
	if v != nil {
		view, fixed = v.BorrowFixedWidth()
	}
	for row, group := range groupIDs {
		if row&255 == 0 {
			if err := ctx.Err(); err != nil {
				return err
			}
		}
		if v != nil {
			valid := false
			if fixed {
				_, valid = view.Value(row)
			} else {
				valid = v.IsValid(row)
			}
			if !valid {
				continue
			}
		}
		st := a.stateAt(states, group, ai)
		binary.LittleEndian.PutUint64(st[8:16], binary.LittleEndian.Uint64(st[8:16])+1)
		st[0] = 1
	}
	return nil
}

func (a *HashAggregate) sumGroupStates(ctx context.Context, states []byte, ai int, spec BoundSpec, chunk *vector.DataChunk, groupIDs []int) error {
	v := chunk.Columns[spec.Spec.Input]
	view, fixed := v.BorrowFixedWidth()
	if !fixed {
		return a.fallbackGroupStates(ctx, states, ai, spec, chunk, groupIDs)
	}
	avg := spec.Spec.Kind == Avg
	if spec.InputType.ID == types.Double {
		for row, group := range groupIDs {
			if row&255 == 0 {
				if err := ctx.Err(); err != nil {
					return err
				}
			}
			data, valid := view.Value(row)
			if !valid {
				continue
			}
			if err := addFloatGroupState(a.stateAt(states, group, ai), math.Float64frombits(binary.LittleEndian.Uint64(data)), avg); err != nil {
				return err
			}
		}
		return nil
	}
	if spec.InputType.ID == types.Float {
		for row, group := range groupIDs {
			if row&255 == 0 {
				if err := ctx.Err(); err != nil {
					return err
				}
			}
			data, valid := view.Value(row)
			if !valid {
				continue
			}
			if err := addFloatGroupState(a.stateAt(states, group, ai), float64(math.Float32frombits(binary.LittleEndian.Uint32(data))), avg); err != nil {
				return err
			}
		}
		return nil
	}

	for row, group := range groupIDs {
		if row&255 == 0 {
			if err := ctx.Err(); err != nil {
				return err
			}
		}
		data, valid := view.Value(row)
		if !valid {
			continue
		}
		st := a.stateAt(states, group, ai)
		if avg {
			count := binary.LittleEndian.Uint64(st[8:16])
			if count == math.MaxUint64 {
				return semantics.ErrOverflow
			}
			binary.LittleEndian.PutUint64(st[8:16], count+1)
		}
		if err := a.add128(st, aggregateInteger(spec.InputType, data)); err != nil {
			return err
		}
	}
	return nil
}

func addFloatGroupState(st []byte, x float64, avg bool) error {
	if avg {
		count := binary.LittleEndian.Uint64(st[8:16])
		if count == math.MaxUint64 {
			return semantics.ErrOverflow
		}
		binary.LittleEndian.PutUint64(st[8:16], count+1)
	}
	current := math.Float64frombits(binary.LittleEndian.Uint64(st[16:24]))
	binary.LittleEndian.PutUint64(st[16:24], math.Float64bits(current+x))
	st[0] = 1
	return nil
}

func (a *HashAggregate) fallbackGroupStates(ctx context.Context, states []byte, ai int, spec BoundSpec, chunk *vector.DataChunk, groupIDs []int) error {
	for row, group := range groupIDs {
		if row&255 == 0 {
			if err := ctx.Err(); err != nil {
				return err
			}
		}
		if err := a.updateState(a.stateAt(states, group, ai), spec, chunk, row); err != nil {
			return err
		}
	}
	return nil
}
