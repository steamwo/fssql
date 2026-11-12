package binder

import (
	"fmt"
	"math/big"
	"strconv"

	"github.com/steamwo/fssql/internal/core/types"
	"github.com/steamwo/fssql/internal/function"
)

func roundDecimalType(args []*BoundExpression) (types.LogicalType, error) {
	t := args[0].Type
	var precision int64
	if len(args) == 2 {
		var ok bool
		precision, ok = constantRoundPrecision(args[1])
		if !ok {
			return types.LogicalType{}, fmt.Errorf("round DECIMAL precision must be a non-NULL constant integer expression")
		}
	}
	if precision >= int64(t.Decimal.Scale) {
		return t, nil
	}
	if precision < 0 {
		if t.Decimal.Scale == 0 && t.Decimal.Width < 38 {
			t.Decimal.Width++
		}
		t.Decimal.Scale = 0
	} else {
		t.Decimal.Scale = uint8(precision)
	}
	return t, nil
}

// Fold only exact integer expressions; arbitrary function calls stay unsupported.
func constantRoundPrecision(e *BoundExpression) (int64, bool) {
	if e == nil || e.Type.Unsigned {
		return 0, false
	}
	size, fixed := e.Type.FixedSize()
	if !fixed || (e.Type.ID != types.TinyInt && e.Type.ID != types.SmallInt && e.Type.ID != types.Integer && e.Type.ID != types.BigInt) {
		return 0, false
	}
	var value int64
	var ok bool
	switch e.Kind {
	case BoundLiteral:
		var err error
		value, err = strconv.ParseInt(e.Literal, 10, 64)
		ok = err == nil
	case BoundCast:
		if len(e.Children) == 1 {
			value, ok = constantRoundPrecision(e.Children[0])
		}
	case BoundFunction:
		if e.FunctionID == function.AbsID && len(e.Children) == 1 {
			value, ok = constantRoundPrecision(e.Children[0])
			if ok && value < 0 {
				n := new(big.Int).Abs(big.NewInt(value))
				if !n.IsInt64() {
					return 0, false
				}
				value = n.Int64()
			}
			break
		}
		if e.FunctionID == function.ModID && len(e.Children) == 2 {
			a, aok := constantRoundPrecision(e.Children[0])
			d, dok := constantRoundPrecision(e.Children[1])
			if !aok || !dok || d == 0 {
				return 0, false
			}
			value, ok = a%d, true
			break
		}
		if (e.FunctionID == function.FloorID || e.FunctionID == function.RoundID) && len(e.Children) == 1 {
			value, ok = constantRoundPrecision(e.Children[0])
			break
		}
		if e.FunctionID >= function.FirstCastFunctionID && e.FunctionID < function.FirstTryCastFunctionID && len(e.Children) == 1 {
			child := e.Children[0]
			if child.Kind == BoundLiteral && child.Type.ID == types.Varchar && !child.Type.Binary {
				var err error
				value, err = strconv.ParseInt(child.Literal, 10, 64)
				ok = err == nil
			} else {
				value, ok = constantRoundPrecision(child)
			}
		}
	case BoundUnary:
		if len(e.Children) != 1 {
			return 0, false
		}
		value, ok = constantRoundPrecision(e.Children[0])
		if e.Op == "-" {
			n := new(big.Int).Neg(big.NewInt(value))
			if !n.IsInt64() {
				return 0, false
			}
			value = n.Int64()
		} else if e.Op != "+" {
			return 0, false
		}
	case BoundBinary:
		if len(e.Children) != 2 {
			return 0, false
		}
		a, aok := constantRoundPrecision(e.Children[0])
		b, bok := constantRoundPrecision(e.Children[1])
		if !aok || !bok {
			return 0, false
		}
		n, d := big.NewInt(a), big.NewInt(b)
		switch e.Op {
		case "+":
			n.Add(n, d)
		case "-":
			n.Sub(n, d)
		case "*":
			n.Mul(n, d)
		case "%":
			if b == 0 {
				return 0, false
			}
			n.Rem(n, d)
		default:
			return 0, false
		}
		if !n.IsInt64() {
			return 0, false
		}
		value, ok = n.Int64(), true
	}
	bits := uint(size * 8)
	if bits < 64 && (value < -(int64(1)<<(bits-1)) || value >= int64(1)<<(bits-1)) {
		return 0, false
	}
	return value, ok
}
