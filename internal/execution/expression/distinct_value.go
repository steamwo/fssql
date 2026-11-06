package expression

import (
	"bytes"

	"github.com/steamwo/fssql/internal/core/types"
	"github.com/steamwo/fssql/internal/core/vector"
	"github.com/steamwo/fssql/internal/semantics"
	"github.com/steamwo/fssql/internal/sql/binder"
)

func (e *Executor) executeDistinctValue(expr *binder.BoundExpression, chunk *vector.DataChunk) (*vector.Vector, error) {
	args := make([]*vector.Vector, 0, len(expr.Children))
	for _, child := range expr.Children {
		arg, err := e.ExecuteSQL(child, chunk)
		if err != nil {
			releaseScalarVectors(args)
			return nil, err
		}
		args = append(args, arg)
	}
	defer releaseScalarVectors(args)
	state := windowStateFor(e, expr)
	state.mu.Lock()
	defer state.mu.Unlock()
	out, err := vector.NewFlat(e.Scope, e.Buffers, expr.Type, chunk.Cardinality)
	if err != nil {
		return nil, err
	}
	if len(args) == 1 && distinctRowSupported(args[0].Type) {
		arg := args[0]
		for row := 0; row < chunk.Cardinality; row++ {
			same := false
			if row > 0 {
				same, _ = distinctRowsEqual(arg, row, arg, row-1)
			} else if state.distinctPrevious != nil {
				same, _ = distinctRowsEqual(arg, row, state.distinctPrevious.Columns[0], 0)
			}
			if same {
				out.SetNull(row)
			} else if err := vector.CopyValue(arg, row, out, row); err != nil {
				out.Release()
				return nil, err
			}
		}
	} else {
		for row := 0; row < chunk.Cardinality; row++ {
			same := row > 0 || state.distinctPrevious != nil
			if same {
				for i, arg := range args {
					previous, previousRow := arg, row-1
					if row == 0 {
						previous = state.distinctPrevious.Columns[i]
						previousRow = 0
					}
					if arg.IsValid(row) != previous.IsValid(previousRow) {
						same = false
						break
					}
					if !arg.IsValid(row) {
						continue
					}
					a, er := scalarFromVector(arg, row)
					if er != nil {
						out.Release()
						return nil, er
					}
					b, er := scalarFromVector(previous, previousRow)
					if er != nil {
						out.Release()
						return nil, er
					}
					order, er := predicateCompare(a, b)
					if er != nil {
						out.Release()
						return nil, er
					}
					if order != 0 {
						same = false
						break
					}
				}
			}
			if same {
				out.SetNull(row)
			} else if err := vector.CopyValue(args[len(args)-1], row, out, row); err != nil {
				out.Release()
				return nil, err
			}
		}
	}
	if err := e.rememberDistinctPrevious(state, args, chunk.Cardinality); err != nil {
		out.Release()
		return nil, err
	}
	return out, nil
}

// rememberDistinctPrevious stores the last row so the next batch can compare
// its first row without re-reading consumed input.
func (e *Executor) rememberDistinctPrevious(state *windowState, args []*vector.Vector, count int) error {
	if count == 0 {
		return nil
	}
	columns := make([]*vector.Vector, 0, len(args))
	for _, arg := range args {
		v, err := vector.NewFlat(e.Scope, e.Buffers, arg.Type, 1)
		if err != nil {
			releaseScalarVectors(columns)
			return err
		}
		if err := vector.CopyValue(arg, count-1, v, 0); err != nil {
			v.Release()
			releaseScalarVectors(columns)
			return err
		}
		columns = append(columns, v)
	}
	previous, err := vector.NewDataChunk(columns...)
	if err != nil {
		releaseScalarVectors(columns)
		return err
	}
	if state.distinctPrevious != nil {
		state.distinctPrevious.Release()
	}
	state.distinctPrevious = previous
	return nil
}

// distinctRowSupported selects the allocation-free adjacent-row comparison.
func distinctRowSupported(t types.LogicalType) bool {
	switch t.ID {
	case types.Boolean, types.TinyInt, types.SmallInt, types.Integer, types.DateType, types.BigInt,
		types.TimestampType, types.HugeInt, types.Decimal, types.Float, types.Double, types.Varchar:
		return true
	default:
		return false
	}
}

// distinctRowsEqual compares two rows with the same equality used by grouping:
// NULLs are equal to NULLs, NaNs are equal to NaNs, and signed zeroes are equal.
func distinctRowsEqual(current *vector.Vector, currentRow int, previous *vector.Vector, previousRow int) (bool, bool) {
	currentValid := current.IsValid(currentRow)
	previousValid := previous.IsValid(previousRow)
	if !currentValid || !previousValid {
		return !currentValid && !previousValid, true
	}
	if !current.Type.Same(previous.Type) {
		return false, false
	}
	switch current.Type.ID {
	case types.Varchar:
		a, _ := current.Bytes(currentRow)
		b, _ := previous.Bytes(previousRow)
		return bytes.Equal(a, b), true
	case types.Float:
		a, _ := current.Float32(currentRow)
		b, _ := previous.Float32(previousRow)
		return semantics.EqualFloat32(a, b), true
	case types.Double:
		a, _ := current.Float64(currentRow)
		b, _ := previous.Float64(previousRow)
		return semantics.EqualFloat64(a, b), true
	default:
		aView, aOK := current.BorrowFixedWidth()
		bView, bOK := previous.BorrowFixedWidth()
		if !aOK || !bOK {
			return false, false
		}
		a, aValid := aView.Value(currentRow)
		b, bValid := bView.Value(previousRow)
		if !aValid || !bValid {
			return !aValid && !bValid, true
		}
		return bytes.Equal(a, b), true
	}
}
