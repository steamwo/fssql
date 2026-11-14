package export

import (
	"github.com/steamwo/fssql/internal/core/types"
	"github.com/steamwo/fssql/internal/core/vector"
)

// encodeParquetFlatPlain borrows types whose native bytes already match PLAIN.
// Narrow integers and DECIMAL32/128 still require the scalar conversion path.
func encodeParquetFlatPlain(out *parquetPageBuffer, vec *vector.Vector, typ types.LogicalType, start, count int) (bool, error) {
	var width int
	switch typ.ID {
	case types.Integer, types.DateType, types.Float:
		width = 4
	case types.BigInt, types.TimestampType, types.Double:
		width = 8
	case types.Decimal:
		if typ.Decimal.Width <= 9 || typ.Decimal.Width > 18 {
			return false, nil
		}
		width = 8
	default:
		return false, nil
	}
	if vec.Kind != vector.Flat || start < 0 || count < 0 || start > vec.Count() || count > vec.Count()-start {
		return false, nil
	}
	if count == 0 {
		return true, nil
	}
	view, _ := vec.BorrowFixedWidth()
	data, validity, _ := view.FlatData()
	end := start + count
	if validity.AllValid(end) {
		return true, out.Append(data[start*width : end*width])
	}

	// Reserve only the non-NULL payload, so sparse pages retain their original
	// memory envelope. Borrow the destination once after its possible resize.
	validCount := 0
	for row := start; row < end; row++ {
		if validity.IsValid(row) {
			validCount++
		}
	}
	if validCount == 0 {
		return true, nil
	}
	size := validCount * width
	if err := out.Reserve(size); err != nil {
		return true, err
	}
	dst := out.buffer.Bytes()[out.used : out.used+size]
	offset := 0
	for row := start; row < end; row++ {
		if validity.IsValid(row) {
			copy(dst[offset:offset+width], data[row*width:(row+1)*width])
			offset += width
		}
	}
	out.used += size
	return true, nil
}
