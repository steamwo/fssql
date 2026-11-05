package expression

import (
	"fmt"
	"math"
	"math/big"
	"math/bits"

	"github.com/steamwo/fssql/internal/core/types"
	"github.com/steamwo/fssql/internal/core/vector"
	"github.com/steamwo/fssql/internal/semantics"
	"github.com/steamwo/fssql/internal/sql/binder"
)

func (e *Executor) executeSQLExactIntegerBinary(expr *binder.BoundExpression, chunk *vector.DataChunk) (*vector.Vector, error) {
	if len(expr.Children) != 2 || !expr.Children[0].Type.Same(expr.Children[1].Type) {
		return nil, fmt.Errorf("SQL integer kernel requires matching input types")
	}
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
	op := expr.Op
	if op == "!=" {
		op = "<>"
	}
	for row := 0; row < chunk.Cardinality; row++ {
		if !left.IsValid(row) || !right.IsValid(row) {
			out.SetNull(row)
			continue
		}
		x, err := scalarFromVector(left, row)
		if err != nil {
			out.Release()
			return nil, err
		}
		y, err := scalarFromVector(right, row)
		if err != nil {
			out.Release()
			return nil, err
		}
		if isComparisonOperator(op) {
			order, err := compareScalar(x, y)
			if err != nil {
				out.Release()
				return nil, err
			}
			out.SetBool(row, comparisonFromOrder(order, op))
			continue
		}
		value, err := exactIntegerBinaryValue(x, y, op)
		if err != nil {
			out.Release()
			return nil, err
		}
		if err := writeScalarValue(out, row, expr.Type, value); err != nil {
			out.Release()
			return nil, err
		}
	}
	return out, nil
}

func exactIntegerBinaryValue(x, y ScalarValue, op string) (ScalarValue, error) {
	out := ScalarValue{Type: x.Type, Valid: true}
	if x.Type.ID == types.HugeInt {
		if op == "+" {
			value, err := types.AddInt128(x.Int128, y.Int128)
			out.Int128 = value
			return out, err
		}
		a, b := types.Int128Big(x.Int128), types.Int128Big(y.Int128)
		var value big.Int
		switch op {
		case "-":
			value.Sub(a, b)
		case "*":
			value.Mul(a, b)
		case "%":
			if b.Sign() == 0 {
				return ScalarValue{}, fmt.Errorf("HUGEINT modulo by zero")
			}
			if x.Int128.Hi == math.MinInt64 && x.Int128.Lo == 0 && y.Int128.Hi == -1 && y.Int128.Lo == math.MaxUint64 {
				return ScalarValue{}, semantics.ErrOverflow
			}
			value.Rem(a, b)
		default:
			return ScalarValue{}, fmt.Errorf("unsupported HUGEINT operator %q", op)
		}
		wide, ok := types.Int128FromBig(&value)
		if !ok {
			return ScalarValue{}, semantics.ErrOverflow
		}
		out.Int128 = wide
		return out, nil
	}
	if x.Type.IsUnsignedInteger() {
		var overflow uint64
		switch op {
		case "+":
			out.Uint64, overflow = bits.Add64(x.Uint64, y.Uint64, 0)
		case "-":
			out.Uint64, overflow = bits.Sub64(x.Uint64, y.Uint64, 0)
		case "*":
			overflow, out.Uint64 = bits.Mul64(x.Uint64, y.Uint64)
		case "%":
			if y.Uint64 == 0 {
				return ScalarValue{}, fmt.Errorf("unsigned integer modulo by zero")
			}
			out.Uint64 = x.Uint64 % y.Uint64
		default:
			return ScalarValue{}, fmt.Errorf("unsupported unsigned integer operator %q", op)
		}
		if overflow != 0 || out.Uint64 > phase4CastUnsignedMax(x.Type.ID) {
			return ScalarValue{}, semantics.ErrOverflow
		}
		return out, nil
	}
	var err error
	switch op {
	case "+":
		out.Int64, err = semantics.AddInt64(x.Int64, y.Int64)
	case "-":
		out.Int64, err = semantics.SubInt64(x.Int64, y.Int64)
	case "*":
		out.Int64, err = semantics.MulInt64(x.Int64, y.Int64)
	case "%":
		if y.Int64 == 0 {
			return ScalarValue{}, fmt.Errorf("integer modulo by zero")
		}
		if y.Int64 == -1 && (x.Int64 == math.MinInt64 || !phase4CastSignedFits(x.Type.ID, -x.Int64)) {
			return ScalarValue{}, semantics.ErrOverflow
		}
		out.Int64 = x.Int64 % y.Int64
	default:
		return ScalarValue{}, fmt.Errorf("unsupported integer operator %q", op)
	}
	if err != nil {
		return ScalarValue{}, err
	}
	if !phase4CastSignedFits(x.Type.ID, out.Int64) {
		return ScalarValue{}, semantics.ErrOverflow
	}
	return out, nil
}

func (e *Executor) executeSQLExactIntegerUnary(expr *binder.BoundExpression, chunk *vector.DataChunk) (*vector.Vector, error) {
	if len(expr.Children) != 1 {
		return nil, fmt.Errorf("SQL integer unary kernel requires one input")
	}
	src, err := e.ExecuteSQL(expr.Children[0], chunk)
	if err != nil {
		return nil, err
	}
	defer src.Release()
	out, err := vector.NewFlat(e.Scope, e.Buffers, expr.Type, chunk.Cardinality)
	if err != nil {
		return nil, err
	}
	for row := 0; row < chunk.Cardinality; row++ {
		if !src.IsValid(row) {
			out.SetNull(row)
			continue
		}
		value, err := scalarFromVector(src, row)
		if err != nil {
			out.Release()
			return nil, err
		}
		switch expr.Op {
		case "+":
		case "-":
			switch {
			case value.Type.IsUnsignedInteger():
				value.Uint64 = (0 - value.Uint64) & phase4CastUnsignedMax(value.Type.ID)
			case value.Type.ID == types.HugeInt:
				value.Int128, err = types.NegInt128(value.Int128)
			default:
				value.Int64, err = semantics.SubInt64(0, value.Int64)
				if err == nil && !phase4CastSignedFits(value.Type.ID, value.Int64) {
					err = semantics.ErrOverflow
				}
			}
		default:
			err = fmt.Errorf("unsupported integer unary operator %q", expr.Op)
		}
		if err != nil {
			out.Release()
			return nil, err
		}
		if err := writeScalarValue(out, row, expr.Type, value); err != nil {
			out.Release()
			return nil, err
		}
	}
	return out, nil
}
