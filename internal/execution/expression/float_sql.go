package expression

import (
	"fmt"

	"github.com/steamwo/fssql/internal/core/vector"
	"github.com/steamwo/fssql/internal/semantics"
	"github.com/steamwo/fssql/internal/sql/binder"
)

func (e *Executor) executeSQLFloatBinary(expr *binder.BoundExpression, chunk *vector.DataChunk) (*vector.Vector, error) {
	left, err := e.ExecuteSQL(expr.Children[0], chunk)
	if err != nil {
		return nil, err
	}
	defer left.Release()
	right, err := e.ExecuteSQL(expr.Children[1], chunk)
	if err != nil {
		return nil, err
	}
	defer right.Release()
	out, err := vector.NewFlat(e.Scope, e.Buffers, expr.Type, chunk.Cardinality)
	if err != nil {
		return nil, err
	}
	for row := 0; row < chunk.Cardinality; row++ {
		x, xok := left.Float32(row)
		y, yok := right.Float32(row)
		if !xok || !yok {
			out.SetNull(row)
			continue
		}
		if isComparisonOperator(expr.Op) {
			out.SetBool(row, comparisonFromOrder(semantics.CompareFloat32(x, y), expr.Op))
			continue
		}
		switch expr.Op {
		case "+":
			out.SetFloat32(row, x+y)
		case "-":
			out.SetFloat32(row, x-y)
		case "*":
			out.SetFloat32(row, x*y)
		case "/":
			out.SetFloat32(row, x/y)
		default:
			out.Release()
			return nil, fmt.Errorf("unsupported FLOAT operator %q", expr.Op)
		}
	}
	return out, nil
}
