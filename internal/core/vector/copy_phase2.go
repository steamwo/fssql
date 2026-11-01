package vector

import (
	"errors"

	"github.com/steamwo/fssql/internal/core/types"
)

// CopyValue copies one scalar cell between own vectors without converting
// through interface{} or allocating per row.
func CopyValue(src *Vector, srcRow int, dst *Vector, dstRow int) error {
	if src == nil || dst == nil {
		return errors.New("nil vector")
	}
	if src.Type.ID != dst.Type.ID || src.Type.Decimal != dst.Type.Decimal {
		return errors.New("vector type mismatch")
	}
	if !src.IsValid(srcRow) {
		dst.SetNull(dstRow)
		return nil
	}
	switch src.Type.ID {
	case types.Boolean:
		x, _ := src.Bool(srcRow)
		dst.SetBool(dstRow, x)
	case types.TinyInt:
		x, _ := src.Int8(srcRow)
		dst.SetInt8(dstRow, x)
	case types.SmallInt:
		x, _ := src.Int16(srcRow)
		dst.SetInt16(dstRow, x)
	case types.Integer, types.DateType:
		x, _ := src.Int32(srcRow)
		dst.SetInt32(dstRow, x)
	case types.BigInt, types.TimestampType:
		x, _ := src.Int64(srcRow)
		dst.SetInt64(dstRow, x)
	case types.Float:
		x, _ := src.Float32(srcRow)
		dst.SetFloat32(dstRow, x)
	case types.Double:
		x, _ := src.Float64(srcRow)
		dst.SetFloat64(dstRow, x)
	case types.Decimal:
		if src.Type.Decimal.Width <= 18 {
			x, _ := src.Int64(srcRow)
			dst.SetInt64(dstRow, x)
		} else {
			x, _ := src.Int128(srcRow)
			dst.SetInt128(dstRow, x)
		}
	case types.Varchar:
		r, h, _ := src.StringRefAt(srcRow)
		if r.External {
			return dst.SetStringBytes(dstRow, h.ExternalBytes(r))
		}
		return dst.SetStringBytes(dstRow, r.Inline[:r.Length])
	default:
		return errors.New("copy unsupported vector type")
	}
	return nil
}
