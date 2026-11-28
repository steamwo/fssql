package aggregate

import (
	"context"
	"encoding/binary"
	"errors"
	"math"

	"github.com/steamwo/fssql/internal/core/types"
	"github.com/steamwo/fssql/internal/core/vector"
	"github.com/steamwo/fssql/internal/execution/pipeline"
	"github.com/steamwo/fssql/internal/semantics"
)

func (a *HashAggregate) acceptsJoinBatch() bool {
	if len(a.groupColumns) != 0 {
		return false
	}
	for _, b := range a.specs {
		if b.Spec.Kind != CountStar && b.Spec.Kind != Count && b.Spec.Kind != Sum {
			return false
		}
	}
	return true
}

type joinAggregateColumn struct {
	probe                 *vector.Vector
	view                  vector.FixedWidthView
	fixed                 bool
	probeSide             bool
	column, offset, width int
	batch                 pipeline.JoinBatch
}

func joinColumn(batch pipeline.JoinBatch, column int, typ types.LogicalType) (joinAggregateColumn, error) {
	c := joinAggregateColumn{batch: batch}
	if column >= batch.ProbeStart && column-batch.ProbeStart < batch.ProbeWidth {
		c.probeSide = true
		if batch.Probe == nil {
			return c, nil
		}
		c.probe = batch.Probe.Columns[column-batch.ProbeStart]
		if c.probe == nil || !c.probe.Type.Same(typ) || c.probe.Count() < batch.Probe.Cardinality {
			return c, errors.New("join aggregate probe column mismatch")
		}
		c.view, c.fixed = c.probe.BorrowFixedWidth()
		return c, nil
	}
	c.column = column - batch.BuildStart
	if c.column < 0 || c.column >= len(batch.BuildLayout.Columns) {
		return c, errors.New("join aggregate column out of range")
	}
	col := batch.BuildLayout.Columns[c.column]
	if !col.Type.Same(typ) {
		return c, errors.New("join aggregate build column mismatch")
	}
	c.offset, c.width = col.Offset, col.Width
	return c, nil
}

func (c *joinAggregateColumn) value(row int) ([]byte, bool) {
	if c.probeSide {
		if c.probe == nil {
			return nil, false
		}
		index := c.batch.ProbeRows[row]
		if index < 0 {
			return nil, false
		}
		if c.fixed {
			return c.view.Value(index)
		}
		return nil, c.probe.IsValid(index)
	}
	data := c.batch.BuildRows[row]
	if data == nil || data[c.column>>3]&(1<<uint(c.column&7)) != 0 {
		return nil, false
	}
	return data[c.offset : c.offset+c.width], true
}

// ConsumeJoinBatch updates global COUNT/SUM states directly from borrowed
// matches. It preserves match order, duplicate multiplicity and NULL extension.
func (a *HashAggregate) ConsumeJoinBatch(ctx context.Context, batch pipeline.JoinBatch) (bool, error) {
	if !a.acceptsJoinBatch() {
		return false, nil
	}
	if a.finalized {
		return true, errors.New("aggregate already finalized")
	}
	if err := ctx.Err(); err != nil {
		return true, err
	}
	n := len(batch.BuildRows)
	if n != len(batch.ProbeRows) || n > vector.DefaultVectorSize {
		return true, errors.New("invalid join aggregate batch")
	}
	for row, build := range batch.BuildRows {
		if build != nil && len(build) < batch.BuildLayout.FixedWidth {
			return true, errors.New("short join aggregate build row")
		}
		index := batch.ProbeRows[row]
		if index < -1 || batch.Probe != nil && index >= batch.Probe.Cardinality {
			return true, errors.New("invalid join aggregate probe row")
		}
	}
	if n == 0 {
		return true, nil
	}
	if a.groups.Count() == 0 {
		if _, err := a.findOrInsert(&vector.DataChunk{Cardinality: n}, 0, semantics.HashUint64(0)); err != nil {
			return true, err
		}
	}
	if len(a.specs) == 0 {
		return true, nil
	}
	states := a.states.Bytes()
	for i, bound := range a.specs {
		offset := a.layout.States[i].Offset
		st := states[offset : offset+aggregateStateWidth]
		if bound.Spec.Kind == CountStar {
			binary.LittleEndian.PutUint64(st[8:16], binary.LittleEndian.Uint64(st[8:16])+uint64(n))
			st[0] = 1
			continue
		}
		column, err := joinColumn(batch, bound.Spec.Input, bound.InputType)
		if err != nil {
			return true, err
		}
		if handled, err := updateJoinBuildFixed(ctx, st, bound, &column); handled {
			if err != nil {
				return true, err
			}
			continue
		}
		count := binary.LittleEndian.Uint64(st[8:16])
		total := types.Int128{Lo: binary.LittleEndian.Uint64(st[16:24]), Hi: int64(binary.LittleEndian.Uint64(st[24:32]))}
		floatTotal, seen := math.Float64frombits(total.Lo), st[0] != 0
		for row := 0; row < n; row++ {
			if row&255 == 0 {
				if err := ctx.Err(); err != nil {
					return true, err
				}
			}
			data, valid := column.value(row)
			if !valid {
				continue
			}
			seen = true
			if bound.Spec.Kind == Count {
				count++
				continue
			}
			switch bound.InputType.ID {
			case types.Double:
				floatTotal += math.Float64frombits(binary.LittleEndian.Uint64(data))
			case types.Float:
				floatTotal += float64(math.Float32frombits(binary.LittleEndian.Uint32(data)))
			default:
				total, err = types.AddInt128(total, aggregateInteger(bound.InputType, data))
				if err != nil {
					return true, semantics.ErrOverflow
				}
			}
		}
		if !seen {
			continue
		}
		st[0] = 1
		binary.LittleEndian.PutUint64(st[8:16], count)
		if bound.Spec.Kind == Sum {
			if bound.InputType.ID == types.Float || bound.InputType.ID == types.Double {
				binary.LittleEndian.PutUint64(st[16:24], math.Float64bits(floatTotal))
			} else {
				binary.LittleEndian.PutUint64(st[16:24], total.Lo)
				binary.LittleEndian.PutUint64(st[24:32], uint64(total.Hi))
			}
		}
	}
	return true, nil
}

// Dispatch once for build-side COUNT and BIGINT SUM. The inner loop reads
// native payload directly instead of resolving a generic joined column per row.
func updateJoinBuildFixed(ctx context.Context, state []byte, bound BoundSpec, column *joinAggregateColumn) (bool, error) {
	if column.probeSide {
		return false, nil
	}
	if bound.Spec.Kind == Count {
		count := binary.LittleEndian.Uint64(state[8:16])
		for row, data := range column.batch.BuildRows {
			if row&255 == 0 {
				if err := ctx.Err(); err != nil {
					return true, err
				}
			}
			if data != nil && data[column.column>>3]&(1<<uint(column.column&7)) == 0 {
				count++
				state[0] = 1
			}
		}
		binary.LittleEndian.PutUint64(state[8:16], count)
		return true, nil
	}
	if bound.InputType.ID != types.BigInt {
		return false, nil
	}
	total := types.Int128{Lo: binary.LittleEndian.Uint64(state[16:24]), Hi: int64(binary.LittleEndian.Uint64(state[24:32]))}
	signed := !bound.InputType.Unsigned
	seen := state[0] != 0
	for row, data := range column.batch.BuildRows {
		if row&255 == 0 {
			if err := ctx.Err(); err != nil {
				return true, err
			}
		}
		if data == nil || data[column.column>>3]&(1<<uint(column.column&7)) != 0 {
			continue
		}
		x := types.Int128{Lo: binary.LittleEndian.Uint64(data[column.offset : column.offset+8])}
		if signed && int64(x.Lo) < 0 {
			x.Hi = -1
		}
		var err error
		total, err = types.AddInt128(total, x)
		if err != nil {
			return true, semantics.ErrOverflow
		}
		seen = true
	}
	if seen {
		state[0] = 1
		binary.LittleEndian.PutUint64(state[16:24], total.Lo)
		binary.LittleEndian.PutUint64(state[24:32], uint64(total.Hi))
	}
	return true, nil
}
