package expression

import (
	"github.com/steamwo/fssql/internal/core/vector"
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
	if chunk.Cardinality > 0 {
		columns := make([]*vector.Vector, 0, len(args))
		for _, arg := range args {
			v, er := vector.NewFlat(e.Scope, e.Buffers, arg.Type, 1)
			if er != nil {
				releaseScalarVectors(columns)
				out.Release()
				return nil, er
			}
			if er := vector.CopyValue(arg, chunk.Cardinality-1, v, 0); er != nil {
				v.Release()
				releaseScalarVectors(columns)
				out.Release()
				return nil, er
			}
			columns = append(columns, v)
		}
		previous, er := vector.NewDataChunk(columns...)
		if er != nil {
			releaseScalarVectors(columns)
			out.Release()
			return nil, er
		}
		if state.distinctPrevious != nil {
			state.distinctPrevious.Release()
		}
		state.distinctPrevious = previous
	}
	return out, nil
}
