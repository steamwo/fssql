package hash

import (
	"encoding/binary"
	"errors"
	"math"

	"github.com/steamwo/fssql/internal/core/types"
	"github.com/steamwo/fssql/internal/core/vector"
	"github.com/steamwo/fssql/internal/semantics"
)

func fixedHasher(typ types.LogicalType) func([]byte) uint64 {
	if typ.IsUnsignedInteger() {
		switch typ.ID {
		case types.TinyInt:
			return func(b []byte) uint64 { return semantics.HashUint64(uint64(b[0])) }
		case types.SmallInt:
			return func(b []byte) uint64 { return semantics.HashUint64(uint64(binary.LittleEndian.Uint16(b))) }
		case types.Integer:
			return func(b []byte) uint64 { return semantics.HashUint64(uint64(binary.LittleEndian.Uint32(b))) }
		case types.BigInt:
			return func(b []byte) uint64 { return semantics.HashUint64(binary.LittleEndian.Uint64(b)) }
		}
	}
	switch typ.ID {
	case types.Boolean:
		return func(b []byte) uint64 { return semantics.HashBool(b[0] != 0) }
	case types.TinyInt:
		return func(b []byte) uint64 { return semantics.HashInt64(int64(int8(b[0]))) }
	case types.SmallInt:
		return func(b []byte) uint64 { return semantics.HashInt64(int64(int16(binary.LittleEndian.Uint16(b)))) }
	case types.Integer, types.DateType:
		return func(b []byte) uint64 { return semantics.HashInt64(int64(int32(binary.LittleEndian.Uint32(b)))) }
	case types.BigInt, types.TimestampType:
		return func(b []byte) uint64 { return semantics.HashInt64(int64(binary.LittleEndian.Uint64(b))) }
	case types.HugeInt:
		return hash128Bytes
	case types.Decimal:
		if typ.Decimal.Width > 18 {
			return hash128Bytes
		}
		return func(b []byte) uint64 { return semantics.HashInt64(int64(binary.LittleEndian.Uint64(b))) }
	case types.Float:
		return func(b []byte) uint64 {
			return semantics.HashFloat32(math.Float32frombits(binary.LittleEndian.Uint32(b)))
		}
	case types.Double:
		return func(b []byte) uint64 {
			return semantics.HashFloat64(math.Float64frombits(binary.LittleEndian.Uint64(b)))
		}
	}
	return nil
}

func hash128Bytes(b []byte) uint64 {
	return semantics.HashInt128(binary.LittleEndian.Uint64(b), int64(binary.LittleEndian.Uint64(b[8:])))
}

func hashVectorBatch(v *vector.Vector, count int, out []uint64, combine bool) error {
	if v == nil || count < 0 || count > v.Count() || len(out) < count {
		return errors.New("invalid hash batch bounds")
	}
	if v.Type.ID == types.Varchar {
		view, _ := v.BorrowBytes()
		for row := 0; row < count; row++ {
			data, valid := view.Value(row)
			h := semantics.HashNull()
			if valid {
				h = semantics.HashBytes(data)
			}
			if combine {
				out[row] = semantics.CombineHash(out[row], h)
			} else {
				out[row] = h
			}
		}
		return nil
	}
	hasher := fixedHasher(v.Type)
	if hasher == nil {
		// Preserve the scalar fallback: NULL has a semantic hash even when
		// non-null payloads of this type are unsupported.
		for row := 0; row < count; row++ {
			h, err := hashValue(v, row)
			if err != nil {
				return err
			}
			if combine {
				out[row] = semantics.CombineHash(out[row], h)
			} else {
				out[row] = h
			}
		}
		return nil
	}
	view, ok := v.BorrowFixedWidth()
	if !ok {
		return errors.New("hash requires fixed-width view")
	}
	for row := 0; row < count; row++ {
		data, valid := view.Value(row)
		h := semantics.HashNull()
		if valid {
			h = hasher(data)
		}
		if combine {
			out[row] = semantics.CombineHash(out[row], h)
		} else {
			out[row] = h
		}
	}
	return nil
}
