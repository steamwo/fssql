package export

import (
	"encoding/binary"
	"errors"
	"math"

	"github.com/steamwo/fssql/internal/core/vector"
)

// Borrow string/BLOB storage and reserve exactly the non-NULL encoded payload.
// The second pass copies directly into the page without per-value buffer locks
// or temporary byte slices. The input remains immutable throughout both passes.
func encodeParquetBytesPlain(out *parquetPageBuffer, vec *vector.Vector, start, count int) error {
	view, _ := vec.BorrowBytes()
	size := 0
	for row := start; row < start+count; row++ {
		value, valid := view.Value(row)
		if !valid {
			continue
		}
		if uint64(len(value)) > math.MaxUint32 {
			return errors.New("export: parquet byte array exceeds uint32")
		}
		if len(value) > math.MaxInt-size-4 {
			return errors.New("export: parquet byte array page size overflows int")
		}
		size += 4 + len(value)
	}
	if size == 0 {
		return nil
	}
	if size > math.MaxInt-out.used {
		return errors.New("export: parquet byte array page size overflows int")
	}
	if err := out.Reserve(size); err != nil {
		return err
	}
	dst := out.buffer.Bytes()[out.used : out.used+size]
	offset := 0
	for row := start; row < start+count; row++ {
		value, valid := view.Value(row)
		if !valid {
			continue
		}
		binary.LittleEndian.PutUint32(dst[offset:], uint32(len(value)))
		offset += 4
		copy(dst[offset:], value)
		offset += len(value)
	}
	out.used += size
	return nil
}
