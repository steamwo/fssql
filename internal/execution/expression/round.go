package expression

import (
	"fmt"
	"math"
	"math/big"

	"github.com/steamwo/fssql/internal/core/types"
)

func roundScalar(result types.LogicalType, args []ScalarValue) (ScalarValue, error) {
	v := args[0]
	var precision int64
	if len(args) == 2 {
		precision = args[1].Int64
	}
	out := ScalarValue{Type: result, Valid: true}
	if v.Type.ID == types.Float || v.Type.ID == types.Double {
		x := v.Float64
		if math.IsNaN(x) || math.IsInf(x, 0) {
			out.Float64 = x
			return out, nil
		}
		limit := int64(308)
		if precision > limit {
			out.Float64 = x
			return out, nil
		}
		if precision < -limit {
			return out, nil
		}
		factor := math.Pow10(int(precision))
		if precision < 0 {
			factor = math.Pow10(int(-precision))
		}
		scaled := x * factor
		if precision < 0 {
			scaled = x / factor
		}
		if math.IsInf(scaled, 0) {
			out.Float64 = x
			return out, nil
		}
		y := math.Round(scaled)
		if precision < 0 {
			y *= factor
		} else {
			y /= factor
		}
		if v.Type.ID == types.Float {
			y = float64(float32(y))
		}
		if math.IsInf(y, 0) {
			y = x
		}
		out.Float64 = y
		return out, nil
	}
	var n *big.Int
	scale := int64(0)
	switch v.Type.ID {
	case types.HugeInt:
		n = types.Int128Big(v.Int128)
	case types.Decimal:
		n = types.Int128Big(v.Int128)
		scale = int64(v.Type.Decimal.Scale)
	default:
		n = big.NewInt(v.Int64)
	}
	if precision < scale {
		if scale-precision > 39 {
			n.SetInt64(0)
		} else {
			factor := new(big.Int).Exp(big.NewInt(10), big.NewInt(scale-precision), nil)
			q, r := new(big.Int), new(big.Int)
			q.QuoRem(n, factor, r)
			if new(big.Int).Lsh(new(big.Int).Abs(r), 1).Cmp(factor) >= 0 {
				if n.Sign() < 0 {
					q.Sub(q, big.NewInt(1))
				} else {
					q.Add(q, big.NewInt(1))
				}
			}
			n = q
			if precision < 0 {
				n.Mul(n, new(big.Int).Exp(big.NewInt(10), big.NewInt(-precision), nil))
			}
		}
	} else if result.ID == types.Decimal && scale > int64(result.Decimal.Scale) {
		n = rescaleDecimalInteger(n, uint8(scale), result.Decimal.Scale)
	}
	switch result.ID {
	case types.HugeInt, types.Decimal:
		if result.ID == types.Decimal && !decimalIntegerFitsWidth(n, result.Decimal.Width) {
			return ScalarValue{}, fmt.Errorf("round DECIMAL overflow")
		}
		var ok bool
		out.Int128, ok = types.Int128FromBig(n)
		if !ok {
			return ScalarValue{}, fmt.Errorf("round integer overflow")
		}
	default:
		if !n.IsInt64() || !castSignedFits(result.ID, n.Int64()) {
			return ScalarValue{}, fmt.Errorf("round integer overflow")
		}
		out.Int64 = n.Int64()
	}
	return out, nil
}
