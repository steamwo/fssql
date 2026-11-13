package export

import (
	"bytes"
	"cmp"
	"encoding/binary"
	"math"

	"github.com/steamwo/fssql/internal/core/types"
	"github.com/steamwo/fssql/internal/core/vector"
	"github.com/steamwo/fssql/internal/format/parquet"
)

// Large strings keep their values but omit bounds, rather than growing footer
// metadata with the size of individual cells or inventing truncated bounds.
const maxParquetStatisticsBound = 1024

// collectParquetStatistics borrows string bounds until the row group is released.
// Fixed-size bounds and counters belong to the returned small metadata object.
func collectParquetStatistics(vec *vector.Vector, start, count int) *parquet.Statistics {
	state := &struct {
		stats       parquet.Statistics
		nulls, nans int64
		min, max    [16]byte
		exact       bool
	}{exact: true}
	stats := &state.stats
	stats.NullCount = &state.nulls
	typ := vec.Type
	floating := typ.ID == types.Float || typ.ID == types.Double
	if floating {
		stats.NaNCount = &state.nans
	}
	var value func(int) ([]byte, bool)
	if typ.ID == types.Varchar {
		view, _ := vec.BorrowBytes()
		value = view.Value
	} else {
		view, _ := vec.BorrowFixedWidth()
		value = view.Value
	}
	compare := parquetStatisticsComparator(typ)
	var minimum, maximum []byte
	hasValue, omitBounds := false, false
	for row := start; row < start+count; row++ {
		raw, valid := value(row)
		if !valid {
			state.nulls++
			continue
		}
		if floating {
			v := parquetStatisticsFloat(raw, typ.ID)
			if math.IsNaN(v) {
				state.nans++
				omitBounds = true
				continue
			}
		}
		if len(raw) > maxParquetStatisticsBound {
			omitBounds = true
		}
		if omitBounds {
			continue
		}
		if !hasValue {
			minimum, maximum, hasValue = raw, raw, true
			continue
		}
		if compare(raw, minimum) < 0 {
			minimum = raw
		}
		if compare(raw, maximum) > 0 {
			maximum = raw
		}
	}
	if hasValue && !omitBounds {
		stats.MinValue = encodeParquetStatisticsBound(state.min[:], minimum, typ, true)
		stats.MaxValue = encodeParquetStatisticsBound(state.max[:], maximum, typ, false)
		// TYPE_ORDER expands zero bounds for floats; those byte patterns need
		// not both occur in the input. Other types have exact, observed bounds.
		if !floating {
			stats.IsMinValueExact, stats.IsMaxValueExact = &state.exact, &state.exact
		}
	}
	return stats
}

func parquetStatisticsComparator(typ types.LogicalType) func([]byte, []byte) int {
	if typ.IsUnsignedInteger() {
		switch typ.ID {
		case types.TinyInt:
			return func(a, b []byte) int { return cmp.Compare(a[0], b[0]) }
		case types.SmallInt:
			return func(a, b []byte) int {
				return cmp.Compare(binary.LittleEndian.Uint16(a), binary.LittleEndian.Uint16(b))
			}
		case types.Integer:
			return func(a, b []byte) int {
				return cmp.Compare(binary.LittleEndian.Uint32(a), binary.LittleEndian.Uint32(b))
			}
		case types.BigInt:
			return func(a, b []byte) int {
				return cmp.Compare(binary.LittleEndian.Uint64(a), binary.LittleEndian.Uint64(b))
			}
		}
	}
	switch typ.ID {
	case types.Boolean:
		return func(a, b []byte) int { return cmp.Compare(a[0], b[0]) }
	case types.TinyInt:
		return func(a, b []byte) int { return cmp.Compare(int8(a[0]), int8(b[0])) }
	case types.SmallInt:
		return func(a, b []byte) int {
			return cmp.Compare(int16(binary.LittleEndian.Uint16(a)), int16(binary.LittleEndian.Uint16(b)))
		}
	case types.Integer, types.DateType:
		return func(a, b []byte) int {
			return cmp.Compare(int32(binary.LittleEndian.Uint32(a)), int32(binary.LittleEndian.Uint32(b)))
		}
	case types.Float:
		return func(a, b []byte) int {
			return cmp.Compare(math.Float32frombits(binary.LittleEndian.Uint32(a)), math.Float32frombits(binary.LittleEndian.Uint32(b)))
		}
	case types.Double:
		return func(a, b []byte) int {
			return cmp.Compare(math.Float64frombits(binary.LittleEndian.Uint64(a)), math.Float64frombits(binary.LittleEndian.Uint64(b)))
		}
	case types.Varchar:
		return bytes.Compare
	case types.Decimal:
		if typ.Decimal.Width > 18 {
			return func(a, b []byte) int {
				return types.CompareInt128(types.Int128{Lo: binary.LittleEndian.Uint64(a), Hi: int64(binary.LittleEndian.Uint64(a[8:]))}, types.Int128{Lo: binary.LittleEndian.Uint64(b), Hi: int64(binary.LittleEndian.Uint64(b[8:]))})
			}
		}
	}
	return func(a, b []byte) int {
		return cmp.Compare(int64(binary.LittleEndian.Uint64(a)), int64(binary.LittleEndian.Uint64(b)))
	}
}

// Page bounds are already PLAIN-encoded. Only DECIMAL32 and DECIMAL128
// differ from the vector's native representation when merging page bounds.
func parquetStatisticsEncodedComparator(typ types.LogicalType) func([]byte, []byte) int {
	if typ.ID == types.Decimal {
		if typ.Decimal.Width <= 9 {
			return parquetStatisticsComparator(types.LogicalType{ID: types.Integer})
		}
		if typ.Decimal.Width > 18 {
			return func(a, b []byte) int {
				return types.CompareInt128(types.Int128{Hi: int64(binary.BigEndian.Uint64(a)), Lo: binary.BigEndian.Uint64(a[8:])}, types.Int128{Hi: int64(binary.BigEndian.Uint64(b)), Lo: binary.BigEndian.Uint64(b[8:])})
			}
		}
	}
	return parquetStatisticsComparator(typ)
}

func encodeParquetStatisticsBound(out, raw []byte, typ types.LogicalType, minimum bool) []byte {
	if typ.ID == types.Varchar {
		return raw
	}
	switch typ.ID {
	case types.Boolean:
		out[0] = raw[0]
		return out[:1]
	case types.TinyInt:
		value := int32(int8(raw[0]))
		if typ.Unsigned {
			value = int32(raw[0])
		}
		binary.LittleEndian.PutUint32(out, uint32(value))
		return out[:4]
	case types.SmallInt:
		value := int32(int16(binary.LittleEndian.Uint16(raw)))
		if typ.Unsigned {
			value = int32(binary.LittleEndian.Uint16(raw))
		}
		binary.LittleEndian.PutUint32(out, uint32(value))
		return out[:4]
	case types.Decimal:
		if typ.Decimal.Width <= 9 {
			binary.LittleEndian.PutUint32(out, uint32(binary.LittleEndian.Uint64(raw)))
			return out[:4]
		}
		if typ.Decimal.Width > 18 {
			binary.BigEndian.PutUint64(out, binary.LittleEndian.Uint64(raw[8:]))
			binary.BigEndian.PutUint64(out[8:], binary.LittleEndian.Uint64(raw))
			return out[:16]
		}
	case types.Float:
		if math.Float32frombits(binary.LittleEndian.Uint32(raw)) == 0 {
			bits := uint32(0)
			if minimum {
				bits = 1 << 31
			}
			binary.LittleEndian.PutUint32(out, bits)
			return out[:4]
		}
	case types.Double:
		if math.Float64frombits(binary.LittleEndian.Uint64(raw)) == 0 {
			bits := uint64(0)
			if minimum {
				bits = 1 << 63
			}
			binary.LittleEndian.PutUint64(out, bits)
			return out[:8]
		}
	}
	copy(out, raw)
	return out[:len(raw)]
}

func parquetStatisticsFloat(raw []byte, id types.LogicalTypeID) float64 {
	if id == types.Double {
		return math.Float64frombits(binary.LittleEndian.Uint64(raw))
	}
	return float64(math.Float32frombits(binary.LittleEndian.Uint32(raw)))
}
