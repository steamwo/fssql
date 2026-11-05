package expression

import (
	"fmt"
	"math"
	"math/big"
	"strings"

	"github.com/steamwo/fssql/internal/core/types"
)

func castHugeIntScalar(value ScalarValue, target types.LogicalType) (ScalarValue, error) {
	out := ScalarValue{Type: target, Valid: true}
	var integer *big.Int
	switch {
	case value.Type.ID == types.HugeInt:
		integer = types.Int128Big(value.Int128)
	case value.Type.ID == types.Varchar && !value.Type.Binary:
		var ok bool
		integer, ok = new(big.Int).SetString(strings.TrimSpace(string(value.Bytes)), 10)
		if !ok {
			return ScalarValue{}, fmt.Errorf("invalid HUGEINT text")
		}
	case value.Type.ID == types.Decimal:
		integer = rescaleDecimalInteger(types.Int128Big(value.Int128), value.Type.Decimal.Scale, 0)
	case value.Type.IsUnsignedInteger():
		integer = new(big.Int).SetUint64(value.Uint64)
	case phase4CastSignedIntegerType(value.Type):
		integer = big.NewInt(value.Int64)
	case value.Type.ID == types.Float || value.Type.ID == types.Double:
		if math.IsNaN(value.Float64) || math.IsInf(value.Float64, 0) {
			return ScalarValue{}, fmt.Errorf("CAST HUGEINT overflow")
		}
		integer, _ = new(big.Float).SetFloat64(math.RoundToEven(value.Float64)).Int(nil)
	case value.Type.ID == types.Boolean:
		integer = new(big.Int)
		if value.Bool {
			integer.SetInt64(1)
		}
	default:
		return ScalarValue{}, fmt.Errorf("unsupported CAST %d -> %d", value.Type.ID, target.ID)
	}
	switch {
	case target.ID == types.HugeInt:
		wide, ok := types.Int128FromBig(integer)
		if !ok {
			return ScalarValue{}, fmt.Errorf("CAST HUGEINT overflow")
		}
		out.Int128 = wide
	case target.ID == types.Varchar && !target.Binary:
		out.Bytes = []byte(integer.String())
	case target.ID == types.Decimal:
		integer = rescaleDecimalInteger(integer, 0, target.Decimal.Scale)
		if !decimalIntegerFitsWidth(integer, target.Decimal.Width) {
			return ScalarValue{}, fmt.Errorf("CAST DECIMAL overflow")
		}
		wide, ok := types.Int128FromBig(integer)
		if !ok {
			return ScalarValue{}, fmt.Errorf("CAST DECIMAL overflow")
		}
		out.Int128 = wide
	case target.IsUnsignedInteger():
		if !integer.IsUint64() || integer.Uint64() > phase4CastUnsignedMax(target.ID) {
			return ScalarValue{}, fmt.Errorf("CAST integer overflow")
		}
		out.Uint64 = integer.Uint64()
	case phase4CastSignedIntegerType(target):
		if !integer.IsInt64() || !phase4CastSignedFits(target.ID, integer.Int64()) {
			return ScalarValue{}, fmt.Errorf("CAST integer overflow")
		}
		out.Int64 = integer.Int64()
	case target.ID == types.Double || target.ID == types.Float:
		out.Float64, _ = new(big.Float).SetInt(integer).Float64()
	default:
		return ScalarValue{}, fmt.Errorf("unsupported CAST HUGEINT -> %d", target.ID)
	}
	return out, nil
}
