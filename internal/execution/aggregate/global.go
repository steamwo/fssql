package aggregate

import (
	"context"
	"encoding/binary"

	"github.com/steamwo/fssql/internal/core/vector"
	"github.com/steamwo/fssql/internal/semantics"
)

// A global aggregate has exactly one group: resolve it once per batch and
// borrow its state buffer once, avoiding hashing/serialization on every row.
func (a *HashAggregate) consumeGlobal(ctx context.Context, chunk *vector.DataChunk) error {
	if chunk.Cardinality == 0 {
		return nil
	}
	if a.groups.Count() == 0 {
		if _, err := a.findOrInsert(&vector.DataChunk{Cardinality: chunk.Cardinality}, 0, semantics.HashUint64(0)); err != nil {
			return err
		}
	}
	if len(a.specs) == 0 {
		return nil
	}
	states := a.states.Bytes()
	for ai, bound := range a.specs {
		offset := a.layout.States[ai].Offset
		st := states[offset : offset+aggregateStateWidth]
		if bound.Spec.Kind == CountStar {
			binary.LittleEndian.PutUint64(st[8:16], binary.LittleEndian.Uint64(st[8:16])+uint64(chunk.Cardinality))
			st[0] = 1
			continue
		}
		if handled, err := a.updateGlobalFixed(ctx, st, bound, chunk); handled || err != nil {
			if err != nil {
				return err
			}
			continue
		}
		for row := 0; row < chunk.Cardinality; row++ {
			if row&255 == 0 {
				if err := ctx.Err(); err != nil {
					return err
				}
			}
			if err := a.updateState(st, bound, chunk, row); err != nil {
				return err
			}
		}
	}
	return nil
}
