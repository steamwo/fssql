package parquet

import (
	"context"
	"errors"

	"github.com/steamwo/fssql/internal/core/memory"
	"github.com/steamwo/fssql/internal/core/types"
	"github.com/steamwo/fssql/internal/core/vector"
)

const maxBatchDictionaryEntries = 256
const maxBatchDictionaryBytes = 32 << 10

// readOwnedDictionaryBatch keeps small string dictionaries compressed through
// the execution pipeline. Eligibility and every managed allocation precede
// level/index consumption, so an unsuitable page or allocation can use the
// normal flat decoder without replaying or dropping values.
func (c *ColumnCursor) readOwnedDictionaryBatch(ctx context.Context, rows int) (*vector.Vector, bool, error) {
	if err := ctx.Err(); err != nil {
		return nil, true, err
	}
	if c.hasPending || c.leaf.Logical.ID != types.Varchar {
		return nil, false, nil
	}
	if c.page == nil || c.page.remaining == 0 {
		if err := c.loadPage(ctx); err != nil {
			return nil, true, err
		}
	}
	p := c.page
	if p == nil || p.dict == nil || p.remaining < rows || c.dictionary == nil {
		return nil, false, nil
	}
	size := c.dictionary.Count()
	// Including the NULL sentinel, child references and selection must be
	// smaller than a flat string-reference array. Large dictionaries and long
	// strings retain the existing bounded flat path.
	if size == 0 || size > maxBatchDictionaryEntries || (size+1)*24+rows*4 >= rows*24 {
		return nil, false, nil
	}
	view, _ := c.dictionary.BorrowBytes()
	bytes := 0
	for i := 0; i < size; i++ {
		data, valid := view.Value(i)
		if valid {
			if len(data) > maxBatchDictionaryBytes-bytes {
				return nil, false, nil
			}
			bytes += len(data)
		}
	}
	child, err := vector.NewFlat(c.scope, c.bm, c.leaf.Logical, size+1)
	if err != nil {
		return nil, !errors.Is(err, memory.ErrLimitExceeded), errUnlessLimit(err)
	}
	for i := 0; i < size; i++ {
		if err = vector.CopyValue(c.dictionary, i, child, i); err != nil {
			child.Release()
			return nil, !errors.Is(err, memory.ErrLimitExceeded), errUnlessLimit(err)
		}
	}
	child.SetNull(size)
	var indices [vector.DefaultVectorSize]uint32
	sel := vector.SelectionFrom(indices[:rows]) // initialized to valid index 0
	out, err := vector.NewOwnedDictionary(c.scope, child, sel)
	if err != nil {
		child.Release()
		return nil, !errors.Is(err, memory.ErrLimitExceeded), errUnlessLimit(err)
	}
	keep := false
	defer func() {
		if !keep {
			out.Release()
		}
	}()
	var positions [vector.DefaultVectorSize]int
	defined := rows
	if p.def.definedPrefix(rows) == rows {
		p.def.consumeDefined(rows)
		for row := 0; row < rows; row++ {
			positions[row] = row
		}
	} else {
		// A nil destination only collects defined positions. Undefined rows
		// become the owned child's NULL sentinel instead of mutable validity.
		defined, err = p.def.readValidity(nil, 0, rows, positions[:0])
		if err != nil {
			return nil, true, err
		}
	}
	if err = p.dict.Read(indices[:defined]); err != nil {
		return nil, true, err
	}
	for row := 0; row < rows; row++ {
		sel.Set(row, size)
	}
	for i, row := range positions[:defined] {
		if uint64(indices[i]) >= uint64(size) {
			return nil, true, errors.New("parquet: dictionary index out of range")
		}
		sel.Set(row, int(indices[i]))
	}
	c.finishScalarBatch(rows, defined)
	keep = true
	return out, true, nil
}

func errUnlessLimit(err error) error {
	if errors.Is(err, memory.ErrLimitExceeded) {
		return nil
	}
	return err
}
