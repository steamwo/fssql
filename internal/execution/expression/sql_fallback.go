package expression

import (
	"fmt"
	"math/big"

	"github.com/steamwo/fssql/internal/core/types"
	"github.com/steamwo/fssql/internal/core/vector"
	"github.com/steamwo/fssql/internal/sql/binder"
)

func (e *Executor) executeSQL(expr *binder.BoundExpression, chunk *vector.DataChunk) (*vector.Vector, error) {
	if expr == nil { return nil, fmt.Errorf("SQL expression is nil") }
	switch expr.Kind {
	case binder.BoundLiteral:
		if expr.Type.ID != types.Decimal { return nil, fmt.Errorf("SQL literal type %d unsupported", expr.Type.ID) }
		value, err := types.ParseDecimalText(expr.Literal, expr.Type.Decimal.Width, expr.Type.Decimal.Scale)
		if err != nil { return nil, err }
		out, err := vector.NewConstant(e.Scope, e.Buffers, expr.Type, chunk.Cardinality)
		if err != nil { return nil, err }
		if expr.Type.Decimal.Width <= 18 {
			b := types.Int128Big(value)
			if !b.IsInt64() { out.Release(); return nil, fmt.Errorf("DECIMAL literal overflow") }
			out.SetInt64(0, b.Int64())
		} else {
			out.SetInt128(0, value)
		}
		return out, nil
	case binder.BoundUnary:
		if len(expr.Children) != 1 || expr.Type.ID != types.Decimal { return nil, fmt.Errorf("SQL unary expression unsupported") }
		src, err := e.ExecuteSQL(expr.Children[0], chunk)
		if err != nil { return nil, err }
		defer src.Release()
		out, err := vector.NewFlat(e.Scope, e.Buffers, expr.Type, chunk.Cardinality)
		if err != nil { return nil, err }
		for row := 0; row < chunk.Cardinality; row++ {
			v, err := scalarFromVector(src, row)
			if err != nil { out.Release(); return nil, err }
			if !v.Valid { out.SetNull(row); continue }
			if expr.Op == "-" {
				n := types.Int128Big(v.Int128)
				n.Neg(n)
				wide, ok := types.Int128FromBig(n)
				if !ok { out.Release(); return nil, fmt.Errorf("DECIMAL unary overflow") }
				v.Int128 = wide
			} else if expr.Op != "+" {
				out.Release(); return nil, fmt.Errorf("unsupported unary operator %q", expr.Op)
			}
			v.Type = expr.Type
			if err := writeScalarValue(out, row, expr.Type, v); err != nil { out.Release(); return nil, err }
		}
		return out, nil
	case binder.BoundCast:
		if len(expr.Children) != 1 { return nil, fmt.Errorf("SQL CAST requires one child") }
		src, err := e.ExecuteSQL(expr.Children[0], chunk)
		if err != nil { return nil, err }
		defer src.Release()
		out, err := vector.NewFlat(e.Scope, e.Buffers, expr.Type, chunk.Cardinality)
		if err != nil { return nil, err }
		for row := 0; row < chunk.Cardinality; row++ {
			v, err := scalarFromVector(src, row)
			if err != nil { out.Release(); return nil, err }
			casted, err := castScalarWithDecimalRescaling(v, expr.Type)
			if err != nil { out.Release(); return nil, err }
			if err := writeScalarValue(out, row, expr.Type, casted); err != nil { out.Release(); return nil, err }
		}
		return out, nil
	case binder.BoundBinary:
		return e.executeBinary(expr, chunk)
	default:
		return nil, fmt.Errorf("SQL expression kind %d unsupported", expr.Kind)
	}
}

func (e *Executor) executeBinary(expr *binder.BoundExpression, chunk *vector.DataChunk) (*vector.Vector, error) {
	if len(expr.Children) != 2 { return nil, fmt.Errorf("SQL binary requires two children") }
	left, err := e.ExecuteSQL(expr.Children[0], chunk)
	if err != nil { return nil, err }
	defer left.Release()
	right, err := e.ExecuteSQL(expr.Children[1], chunk)
	if err != nil { return nil, err }
	defer right.Release()
	out, err := vector.NewFlat(e.Scope, e.Buffers, expr.Type, chunk.Cardinality)
	if err != nil { return nil, err }
	for row := 0; row < chunk.Cardinality; row++ {
		if !left.IsValid(row) || !right.IsValid(row) { out.SetNull(row); continue }
		lv, err := scalarFromVector(left, row)
		if err != nil { out.Release(); return nil, err }
		rv, err := scalarFromVector(right, row)
		if err != nil { out.Release(); return nil, err }
		if expr.Type.ID == types.Boolean {
			cmp, err := compareScalar(lv, rv)
			if err != nil { out.Release(); return nil, err }
			switch expr.Op {
			case "=": out.SetBool(row, cmp == 0)
			case "<>": out.SetBool(row, cmp != 0)
			case "<": out.SetBool(row, cmp < 0)
			case "<=": out.SetBool(row, cmp <= 0)
			case ">": out.SetBool(row, cmp > 0)
			case ">=": out.SetBool(row, cmp >= 0)
			default: out.Release(); return nil, fmt.Errorf("SQL comparison %q unsupported", expr.Op)
			}
			continue
		}
		if expr.Type.ID == types.Double {
			lf, err := scalarNumericFloat(lv)
			if err != nil { out.Release(); return nil, err }
			rf, err := scalarNumericFloat(rv)
			if err != nil { out.Release(); return nil, err }
			switch expr.Op {
			case "+": out.SetFloat64(row, lf+rf)
			case "-": out.SetFloat64(row, lf-rf)
			case "*": out.SetFloat64(row, lf*rf)
			case "/": out.SetFloat64(row, lf/rf)
			default: out.Release(); return nil, fmt.Errorf("SQL DOUBLE op %q unsupported", expr.Op)
			}
			continue
		}
		if expr.Type.ID != types.Decimal { out.Release(); return nil, fmt.Errorf("SQL binary output type %d unsupported", expr.Type.ID) }
		ln := scaledDecimal(lv, expr.Type.Decimal.Scale)
		rn := scaledDecimal(rv, expr.Type.Decimal.Scale)
		result := new(big.Int)
		switch expr.Op {
		case "+":
			result.Add(ln, rn)
		case "-":
			result.Sub(ln, rn)
		case "%":
			if rn.Sign() == 0 { out.Release(); return nil, fmt.Errorf("DECIMAL modulo by zero") }
			result.Rem(ln, rn)
		case "*":
			raw := new(big.Int).Mul(types.Int128Big(lv.Int128), types.Int128Big(rv.Int128))
			sourceScale := int(lv.Type.Decimal.Scale) + int(rv.Type.Decimal.Scale)
			targetScale := int(expr.Type.Decimal.Scale)
			if sourceScale > targetScale {
				divisor := new(big.Int).Exp(big.NewInt(10), big.NewInt(int64(sourceScale-targetScale)), nil)
				q, r := new(big.Int), new(big.Int)
				q.QuoRem(raw, divisor, r)
				if r.Sign() != 0 { out.Release(); return nil, fmt.Errorf("DECIMAL multiplication cannot be represented exactly at scale %d", targetScale) }
				result = q
			} else {
				result = raw
				if sourceScale < targetScale { result.Mul(result, new(big.Int).Exp(big.NewInt(10), big.NewInt(int64(targetScale-sourceScale)), nil)) }
			}
		default:
			out.Release(); return nil, fmt.Errorf("DECIMAL operator %q unsupported", expr.Op)
		}
		wide, ok := types.Int128FromBig(result)
		if !ok { out.Release(); return nil, fmt.Errorf("DECIMAL arithmetic overflow") }
		digits := new(big.Int).Abs(new(big.Int).Set(result)).String()
		if len(digits) > int(expr.Type.Decimal.Width) { out.Release(); return nil, fmt.Errorf("DECIMAL(%d,%d) arithmetic overflow", expr.Type.Decimal.Width, expr.Type.Decimal.Scale) }
		if expr.Type.Decimal.Width <= 18 {
			if !result.IsInt64() { out.Release(); return nil, fmt.Errorf("DECIMAL64 overflow") }
			out.SetInt64(row, result.Int64())
		} else {
			out.SetInt128(row, wide)
		}
	}
	return out, nil
}

func scaledDecimal(v ScalarValue, targetScale uint8) *big.Int {
	n := types.Int128Big(v.Int128)
	if v.Type.Decimal.Scale < targetScale { n.Mul(n, new(big.Int).Exp(big.NewInt(10), big.NewInt(int64(targetScale-v.Type.Decimal.Scale)), nil)) }
	return n
}

func scalarNumericFloat(v ScalarValue) (float64, error) {
	if v.Type.IsUnsignedInteger() { return float64(v.Uint64), nil }
	switch v.Type.ID {
	case types.TinyInt, types.SmallInt, types.Integer, types.BigInt:
		return float64(v.Int64), nil
	case types.Float, types.Double:
		return v.Float64, nil
	case types.Decimal:
		r := new(big.Rat).SetFrac(types.Int128Big(v.Int128), new(big.Int).Exp(big.NewInt(10), big.NewInt(int64(v.Type.Decimal.Scale)), nil))
		f, _ := r.Float64()
		return f, nil
	}
	return 0, fmt.Errorf("numeric float conversion unsupported type %d", v.Type.ID)
}
