package expression

import (
	"fmt"
	"math/big"

	"github.com/steamwo/fssql/internal/core/types"
	"github.com/steamwo/fssql/internal/core/vector"
	"github.com/steamwo/fssql/internal/function"
	"github.com/steamwo/fssql/internal/sql/binder"
)

func isCastFunction(expr *binder.BoundExpression) bool {
	return expr != nil && expr.FunctionKind == function.Scalar && expr.FunctionID >= function.FirstCastFunctionID && expr.FunctionID < function.FirstDynamicScalarID
}

func (e *Executor) executeRegisteredCast(expr *binder.BoundExpression, chunk *vector.DataChunk) (*vector.Vector, error) {
	if len(expr.Children) != 1 { return nil, fmt.Errorf("CAST requires one argument") }
	if value, ok, parseErr := constantTemporalCast(expr); ok {
		if parseErr != nil { return nil, parseErr }
		out, err := vector.NewConstant(e.Scope, e.Buffers, expr.Type, chunk.Cardinality)
		if err != nil { return nil, err }
		if !value.Valid { out.SetNull(0) } else if expr.Type.ID == types.DateType { out.SetInt32(0, int32(value.Int64)) } else { out.SetInt64(0, value.Int64) }
		return out, nil
	}
	src, err := e.ExecuteSQL(expr.Children[0], chunk)
	if err != nil { return nil, err }
	defer src.Release()
	out, err := vector.NewFlat(e.Scope, e.Buffers, expr.Type, chunk.Cardinality)
	if err != nil { return nil, err }
	for row := 0; row < chunk.Cardinality; row++ {
		value, err := scalarFromVector(src, row)
		if err != nil { out.Release(); return nil, err }
		casted, err := castScalarWithDecimalRescaling(value, expr.Type)
		if err != nil { out.Release(); return nil, err }
		if err := writeScalarValue(out, row, expr.Type, casted); err != nil { out.Release(); return nil, err }
	}
	return out, nil
}

// constantTemporalCast recognizes a text literal cast to DATE/TIMESTAMP and
// evaluates it once per batch instead of re-parsing every row.
func constantTemporalCast(expr *binder.BoundExpression) (ScalarValue, bool, error) {
	if expr == nil || len(expr.Children) != 1 {
		return ScalarValue{}, false, nil
	}
	child := expr.Children[0]
	if child == nil || child.Kind != binder.BoundLiteral || child.Type.ID != types.Varchar {
		return ScalarValue{}, false, nil
	}
	if expr.Type.ID != types.DateType && expr.Type.ID != types.TimestampType {
		return ScalarValue{}, false, nil
	}
	value, err := parseScalarText(child.Literal, expr.Type)
	return value, true, err
}

func castScalarWithDecimalRescaling(value ScalarValue, target types.LogicalType) (ScalarValue, error) {
	if !value.Valid { return ScalarValue{Type: target, Valid: false}, nil }
	if value.Type.Same(target) { value.Type = target; return value, nil }
	if value.Type.ID == types.HugeInt || target.ID == types.HugeInt { return castHugeIntScalar(value, target) }
	if target.ID != types.Decimal { return castScalar(value, target) }

	if value.Type.ID == types.Varchar && !value.Type.Binary { return parseScalarText(string(value.Bytes), target) }
	var unscaled *big.Int
	var sourceScale uint8
	switch {
	case value.Type.ID == types.Decimal:
		unscaled = types.Int128Big(value.Int128)
		sourceScale = value.Type.Decimal.Scale
	case value.Type.IsUnsignedInteger():
		unscaled = new(big.Int).SetUint64(value.Uint64)
	case value.Type.ID >= types.TinyInt && value.Type.ID <= types.BigInt:
		unscaled = big.NewInt(value.Int64)
	default:
		return ScalarValue{}, fmt.Errorf("unsupported CAST %d -> DECIMAL", value.Type.ID)
	}

	unscaled = rescaleDecimalInteger(unscaled, sourceScale, target.Decimal.Scale)
	if !decimalIntegerFitsWidth(unscaled, target.Decimal.Width) {
		return ScalarValue{}, fmt.Errorf("CAST DECIMAL(%d,%d) overflow", target.Decimal.Width, target.Decimal.Scale)
	}
	wide, ok := types.Int128FromBig(unscaled)
	if !ok { return ScalarValue{}, fmt.Errorf("CAST DECIMAL overflow") }
	return ScalarValue{Type: target, Valid: true, Int128: wide}, nil
}

func rescaleDecimalInteger(value *big.Int, fromScale, toScale uint8) *big.Int {
	out := new(big.Int).Set(value)
	if fromScale == toScale { return out }
	if fromScale < toScale {
		factor := new(big.Int).Exp(big.NewInt(10), big.NewInt(int64(toScale-fromScale)), nil)
		return out.Mul(out, factor)
	}
	divisor := new(big.Int).Exp(big.NewInt(10), big.NewInt(int64(fromScale-toScale)), nil)
	q, r := new(big.Int), new(big.Int)
	q.QuoRem(out, divisor, r)
	absR := new(big.Int).Abs(new(big.Int).Set(r))
	absR.Lsh(absR, 1)
	if absR.Cmp(divisor) >= 0 {
		if out.Sign() < 0 { q.Sub(q, big.NewInt(1)) } else { q.Add(q, big.NewInt(1)) }
	}
	return q
}

func decimalIntegerFitsWidth(value *big.Int, width uint8) bool {
	if width == 0 { return false }
	abs := new(big.Int).Abs(new(big.Int).Set(value))
	return len(abs.String()) <= int(width)
}
