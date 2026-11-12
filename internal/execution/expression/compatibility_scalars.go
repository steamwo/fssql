package expression

import (
	"fmt"
	"math"
	"strings"
	"unicode"

	"github.com/steamwo/fssql/internal/core/types"
)

// DuckDB uses simple Unicode case mapping, including sharp S -> capital sharp S.
func upperText(s string) string {
	return strings.Map(func(r rune) rune {
		if r == '\u00df' {
			return '\u1e9e'
		}
		return unicode.ToUpper(r)
	}, s)
}

func absScalar(v ScalarValue) (ScalarValue, error) {
	if v.Type.IsUnsignedInteger() {
		return v, nil
	}
	switch v.Type.ID {
	case types.TinyInt, types.SmallInt, types.Integer, types.BigInt:
		size, _ := v.Type.FixedSize()
		bits := uint(size * 8)
		if v.Int64 == -(int64(1) << (bits - 1)) {
			return ScalarValue{}, fmt.Errorf("abs overflow")
		}
		if v.Int64 < 0 {
			v.Int64 = -v.Int64
		}
	case types.HugeInt, types.Decimal:
		n := types.Int128Big(v.Int128)
		n.Abs(n)
		var ok bool
		v.Int128, ok = types.Int128FromBig(n)
		if !ok {
			return ScalarValue{}, fmt.Errorf("abs overflow")
		}
	case types.Float, types.Double:
		v.Float64 = math.Abs(v.Float64)
	default:
		return ScalarValue{}, fmt.Errorf("abs unsupported type %d", v.Type.ID)
	}
	return v, nil
}
