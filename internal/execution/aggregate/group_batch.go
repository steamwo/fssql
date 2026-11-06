package aggregate

import (
	"context"
	"encoding/binary"
	"errors"
	"math"

	"github.com/steamwo/fssql/internal/core/memory"
	"github.com/steamwo/fssql/internal/core/types"
	"github.com/steamwo/fssql/internal/core/vector"
	exechash "github.com/steamwo/fssql/internal/execution/hash"
	"github.com/steamwo/fssql/internal/semantics"
)

// findOrCreateGroups 批量解析分组；已知整数键优先查有界精确缓存，未命中仍走完整键比较。
func (a *HashAggregate) findOrCreateGroups(ctx context.Context, projected *vector.DataChunk, hashes []uint64, groupIDs []int) error {
	comparer, err := exechash.NewChunkKeyComparer(projected, a.groupKeys)
	if err != nil {
		return err
	}
	encoder, err := exechash.NewChunkRowEncoder(projected, a.groupLayout)
	if err != nil {
		return err
	}
	cache, cacheView, keyWidth, err := a.integerGroupCache(projected)
	if err != nil {
		return err
	}
	for row := 0; row < projected.Cardinality; row++ {
		if row&255 == 0 {
			if err := ctx.Err(); err != nil {
				return err
			}
		}
		hash := normalizeGroupHash(hashes[row])
		var cacheSlot, key []byte
		var valid bool
		if cache != nil {
			start := int(hash&(integerGroupCacheSlots-1)) * (5 + keyWidth)
			cacheSlot = cache[start : start+5+keyWidth]
			key, valid = cacheView.Value(row)
			if group, ok := cachedIntegerGroup(cacheSlot, key, valid); ok {
				groupIDs[row] = group
				continue
			}
		}
		if err := a.table.prepareInsert(); err != nil {
			return err
		}
		mask := a.table.capacity - 1
		resolved := false
		for probe := 0; probe < a.table.capacity; probe++ {
			slot := (int(hash) + probe) & mask
			stored := binary.LittleEndian.Uint64(a.table.hashes[slot*8 : slot*8+8])
			if stored == 0 {
				size, err := encoder.RowSize(row)
				if err != nil {
					return err
				}
				if err := a.ensureScratch(max(size, 1)); err != nil {
					return err
				}
				enc := a.scratchData[:size]
				if err := encoder.WriteRowSized(row, enc, size); err != nil {
					return err
				}
				index := a.groups.Count()
				if err := a.ensureStates(index + 1); err != nil {
					return err
				}
				if err := a.groups.AppendEncodedRow(enc); err != nil {
					return err
				}
				binary.LittleEndian.PutUint64(a.table.hashes[slot*8:slot*8+8], hash)
				binary.LittleEndian.PutUint32(a.table.rows[slot*4:slot*4+4], uint32(index))
				a.table.count++
				groupIDs[row] = index
				resolved = true
				break
			}
			if stored != hash {
				continue
			}
			existing := int(binary.LittleEndian.Uint32(a.table.rows[slot*4 : slot*4+4]))
			equal, err := comparer.Equal(row, a.groupLayout, a.groups.Row(existing), a.groupKeys, exechash.NullsEqual)
			if err != nil {
				return err
			}
			if equal {
				groupIDs[row] = existing
				resolved = true
				break
			}
		}
		if !resolved {
			return errors.New("aggregate hash table full")
		}
		if cacheSlot != nil {
			binary.LittleEndian.PutUint32(cacheSlot, uint32(groupIDs[row]+1))
			cacheSlot[4] = 0
			if valid {
				cacheSlot[4] = 1
				copy(cacheSlot[5:], key)
			}
		}
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

const denseCountGroupLimit = 1 << 16

// ensureCountScratch keeps a reusable dense per-group counter. Slots are zero
// between batches because the flush resets only touched groups.
func (a *HashAggregate) ensureCountScratch(groups int) error {
	need := groups * 4
	if a.countScratch == nil {
		buf, err := a.bm.Allocate(a.scope, memory.TagAggregate, need)
		if err != nil {
			return err
		}
		a.countScratch = buf
		return nil
	}
	if a.countScratch.Len() >= need {
		return nil
	}
	return a.countScratch.Resize(need, nil)
}

func (a *HashAggregate) countGroupStates(ctx context.Context, states []byte, ai int, v *vector.Vector, groupIDs []int) error {
	var view vector.FixedWidthView
	fixed := false
	if v != nil {
		view, fixed = v.BorrowFixedWidth()
	}
	groups := a.groups.Count()
	if groups > 0 && groups <= denseCountGroupLimit {
		if err := a.ensureCountScratch(groups); err != nil {
			return err
		}
		scratch := a.countScratch.Bytes()[:groups*4]
		a.countTouched = a.countTouched[:0]
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
			if group < 0 || group >= groups {
				return errors.New("aggregate group id out of range")
			}
			slot := scratch[group*4 : group*4+4]
			count := binary.LittleEndian.Uint32(slot)
			if count == 0 {
				a.countTouched = append(a.countTouched, group)
			}
			binary.LittleEndian.PutUint32(slot, count+1)
		}
		for _, group := range a.countTouched {
			slot := scratch[group*4 : group*4+4]
			st := a.stateAt(states, group, ai)
			binary.LittleEndian.PutUint64(st[8:16], binary.LittleEndian.Uint64(st[8:16])+uint64(binary.LittleEndian.Uint32(slot)))
			st[0] = 1
			binary.LittleEndian.PutUint32(slot, 0)
		}
		return nil
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
