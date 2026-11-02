package expression

import (
	"fmt"

	"github.com/steamwo/fssql/internal/core/types"
	"github.com/steamwo/fssql/internal/core/vector"
	"github.com/steamwo/fssql/internal/semantics"
	"github.com/steamwo/fssql/internal/sql/binder"
)

// executeSQLBooleanBinary handles BOOLEAN expressions whose children require
// SQL-specific typed kernels and therefore cannot be lowered as one legacy IR.
// AND/OR preserve SQL three-valued logic; comparisons propagate NULL.
func (e *Executor) executeSQLBooleanBinary(expr *binder.BoundExpression, chunk *vector.DataChunk) (*vector.Vector, error) {
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

	out, err := vector.NewFlat(e.Scope, e.Buffers, types.LogicalType{ID: types.Boolean}, chunk.Cardinality)
	if err != nil {
		return nil, err
	}
	for row := 0; row < chunk.Cardinality; row++ {
		x, xok := left.Bool(row)
		y, yok := right.Bool(row)
		switch expr.Op {
		case "AND":
			if (xok && !x) || (yok && !y) {
				out.SetBool(row, false)
			} else if !xok || !yok {
				out.SetNull(row)
			} else {
				out.SetBool(row, true)
			}
		case "OR":
			if (xok && x) || (yok && y) {
				out.SetBool(row, true)
			} else if !xok || !yok {
				out.SetNull(row)
			} else {
				out.SetBool(row, false)
			}
		default:
			if !isComparisonOperator(expr.Op) {
				out.Release()
				return nil, fmt.Errorf("SQL expression executor: unsupported BOOLEAN operator %q", expr.Op)
			}
			if !xok || !yok {
				out.SetNull(row)
				continue
			}
			cmp := 0
			if !x && y {
				cmp = -1
			} else if x && !y {
				cmp = 1
			}
			out.SetBool(row, comparisonFromOrder(cmp, expr.Op))
		}
	}
	return out, nil
}

// executeSQLBigIntBinary is the typed fallback for BIGINT expressions whose
// children cannot be lowered as a complete legacy expression tree (for example
// aggregate/HAVING rewrites). Arithmetic still delegates overflow semantics to
// the central semantics package; comparisons use its signed int64 ordering.
func (e *Executor) executeSQLBigIntBinary(expr *binder.BoundExpression, chunk *vector.DataChunk) (*vector.Vector, error) {
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

	comparison := isComparisonOperator(expr.Op)
	outType := expr.Type
	if comparison {
		outType = types.LogicalType{ID: types.Boolean}
	}
	out, err := vector.NewFlat(e.Scope, e.Buffers, outType, chunk.Cardinality)
	if err != nil {
		return nil, err
	}
	for row := 0; row < chunk.Cardinality; row++ {
		x, xok := left.Int64(row)
		y, yok := right.Int64(row)
		if !xok || !yok {
			out.SetNull(row)
			continue
		}
		if comparison {
			out.SetBool(row, comparisonFromOrder(semantics.CompareInt64(x, y), expr.Op))
			continue
		}
		var value int64
		switch expr.Op {
		case "+":
			value, err = semantics.AddInt64(x, y)
		case "-":
			value, err = semantics.SubInt64(x, y)
		case "*":
			value, err = semantics.MulInt64(x, y)
		default:
			out.Release()
			return nil, fmt.Errorf("SQL expression executor: unsupported BIGINT operator %q", expr.Op)
		}
		if err != nil {
			out.Release()
			return nil, err
		}
		out.SetInt64(row, value)
	}
	return out, nil
}
