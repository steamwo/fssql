package hash

import (
	"bytes"
	"encoding/binary"
	"errors"
	"math"

	"github.com/steamwo/fssql/internal/core/types"
	"github.com/steamwo/fssql/internal/core/vector"
	"github.com/steamwo/fssql/internal/semantics"
)

type NullEquality uint8

const (
	NullsUnequal NullEquality = iota
	NullsEqual
)

func hashEncoded(col ColumnLayout, data []byte) (uint64, error) {
	if col.Type.IsUnsignedInteger() {
		switch col.Type.ID {
		case types.TinyInt:
			return semantics.HashUint64(uint64(data[0])), nil
		case types.SmallInt:
			return semantics.HashUint64(uint64(binary.LittleEndian.Uint16(data))), nil
		case types.Integer:
			return semantics.HashUint64(uint64(binary.LittleEndian.Uint32(data))), nil
		case types.BigInt:
			return semantics.HashUint64(binary.LittleEndian.Uint64(data)), nil
		}
	}
	switch col.Type.ID {
	case types.Boolean:
		return semantics.HashBool(data[0] != 0), nil
	case types.TinyInt:
		return semantics.HashInt64(int64(int8(data[0]))), nil
	case types.SmallInt:
		return semantics.HashInt64(int64(int16(binary.LittleEndian.Uint16(data)))), nil
	case types.Integer, types.DateType:
		return semantics.HashInt64(int64(int32(binary.LittleEndian.Uint32(data)))), nil
	case types.BigInt, types.TimestampType:
		return semantics.HashInt64(int64(binary.LittleEndian.Uint64(data))), nil
	case types.HugeInt:
		return semantics.HashInt128(binary.LittleEndian.Uint64(data[:8]), int64(binary.LittleEndian.Uint64(data[8:16]))), nil
	case types.Float:
		return semantics.HashFloat32(math.Float32frombits(binary.LittleEndian.Uint32(data))), nil
	case types.Double:
		return semantics.HashFloat64(math.Float64frombits(binary.LittleEndian.Uint64(data))), nil
	case types.Decimal:
		if col.Type.Decimal.Width <= 18 {
			return semantics.HashInt64(int64(binary.LittleEndian.Uint64(data))), nil
		}
		return semantics.HashInt128(binary.LittleEndian.Uint64(data[:8]), int64(binary.LittleEndian.Uint64(data[8:16]))), nil
	case types.Varchar:
		return semantics.HashBytes(data), nil
	default:
		return 0, errors.New("row hash unsupported type")
	}
}

func (l RowLayout) HashRow(row []byte) (uint64, error) {
	if !l.relocatable {
		return 0, errors.New("HashRow requires relocatable layout")
	}
	if len(l.Columns) == 0 {
		return semantics.HashUint64(0), nil
	}
	var out uint64
	for i, col := range l.Columns {
		data, valid, err := l.ColumnData(row, i)
		if err != nil {
			return 0, err
		}
		part := semantics.HashNull()
		if valid {
			part, err = hashEncoded(col, data)
			if err != nil {
				return 0, err
			}
		}
		if i == 0 {
			out = part
		} else {
			out = semantics.CombineHash(out, part)
		}
	}
	return out, nil
}

func equalEncoded(col ColumnLayout, a, b []byte) (bool, error) {
	switch col.Type.ID {
	case types.Float:
		return semantics.EqualFloat32(math.Float32frombits(binary.LittleEndian.Uint32(a)), math.Float32frombits(binary.LittleEndian.Uint32(b))), nil
	case types.Double:
		return semantics.EqualFloat64(math.Float64frombits(binary.LittleEndian.Uint64(a)), math.Float64frombits(binary.LittleEndian.Uint64(b))), nil
	case types.Boolean, types.TinyInt, types.SmallInt, types.Integer, types.DateType, types.BigInt, types.HugeInt, types.TimestampType, types.Decimal, types.Varchar:
		return bytes.Equal(a, b), nil
	default:
		return false, errors.New("row equality unsupported type")
	}
}

func (l RowLayout) EqualRows(a, b []byte, nulls NullEquality) (bool, error) {
	if !l.relocatable {
		return false, errors.New("EqualRows requires relocatable layout")
	}
	for i, col := range l.Columns {
		ad, av, err := l.ColumnData(a, i)
		if err != nil {
			return false, err
		}
		bd, bv, err := l.ColumnData(b, i)
		if err != nil {
			return false, err
		}
		if !av || !bv {
			if !av && !bv && nulls == NullsEqual {
				continue
			}
			return false, nil
		}
		eq, err := equalEncoded(col, ad, bd)
		if err != nil || !eq {
			return eq, err
		}
	}
	return true, nil
}

func setEncodedValue(dst *vector.Vector, dstRow int, col ColumnLayout, data []byte) error {
	if col.Type.IsUnsignedInteger() {
		switch col.Type.ID {
		case types.TinyInt:
			dst.SetUint8(dstRow, data[0])
		case types.SmallInt:
			dst.SetUint16(dstRow, binary.LittleEndian.Uint16(data))
		case types.Integer:
			dst.SetUint32(dstRow, binary.LittleEndian.Uint32(data))
		case types.BigInt:
			dst.SetUint64(dstRow, binary.LittleEndian.Uint64(data))
		}
		return nil
	}
	switch col.Type.ID {
	case types.Boolean:
		dst.SetBool(dstRow, data[0] != 0)
	case types.TinyInt:
		dst.SetInt8(dstRow, int8(data[0]))
	case types.SmallInt:
		dst.SetInt16(dstRow, int16(binary.LittleEndian.Uint16(data)))
	case types.Integer, types.DateType:
		dst.SetInt32(dstRow, int32(binary.LittleEndian.Uint32(data)))
	case types.BigInt, types.TimestampType:
		dst.SetInt64(dstRow, int64(binary.LittleEndian.Uint64(data)))
	case types.HugeInt:
		dst.SetInt128(dstRow, types.Int128{Lo: binary.LittleEndian.Uint64(data[:8]), Hi: int64(binary.LittleEndian.Uint64(data[8:16]))})
	case types.Float:
		dst.SetFloat32(dstRow, math.Float32frombits(binary.LittleEndian.Uint32(data)))
	case types.Double:
		dst.SetFloat64(dstRow, math.Float64frombits(binary.LittleEndian.Uint64(data)))
	case types.Decimal:
		if col.Type.Decimal.Width <= 18 {
			dst.SetInt64(dstRow, int64(binary.LittleEndian.Uint64(data)))
		} else {
			dst.SetInt128(dstRow, types.Int128{Lo: binary.LittleEndian.Uint64(data[:8]), Hi: int64(binary.LittleEndian.Uint64(data[8:16]))})
		}
	case types.Varchar:
		return dst.SetStringBytes(dstRow, data)
	default:
		return errors.New("materialize unsupported type")
	}
	return nil
}

func (l RowLayout) MaterializeRow(row []byte, dst []*vector.Vector, dstRow int) error {
	if !l.relocatable {
		return errors.New("MaterializeRow requires relocatable layout")
	}
	if len(dst) != len(l.Columns) {
		return errors.New("materialize column count mismatch")
	}
	for i, col := range l.Columns {
		data, valid, err := l.ColumnData(row, i)
		if err != nil {
			return err
		}
		if !valid {
			dst[i].SetNull(dstRow)
			continue
		}
		if err := setEncodedValue(dst[i], dstRow, col, data); err != nil {
			return err
		}
	}
	return nil
}

// MaterializeRows gathers encoded rows into output columns column-by-column so
// the type dispatch happens once per column instead of once per row.
func (l RowLayout) MaterializeRows(rows [][]byte, dst []*vector.Vector) error {
	return l.MaterializeSelectedRows(rows, dst, nil)
}

func (c *TupleDataCollection) Truncate(count int) error {
	if count < 0 || count > c.count {
		return errors.New("truncate count out of range")
	}
	if c.layout == nil {
		c.count = count
		return nil
	}
	c.count = count
	c.used = int(binary.LittleEndian.Uint32(c.offsetData[count*4 : count*4+4]))
	return nil
}

// ChunkKeyComparer borrows the fixed-width key columns of one vector chunk so
// batch probes compare keys without re-acquiring managed buffers per row.
type ChunkKeyComparer struct {
	chunk     *vector.DataChunk
	columns   []int
	views     []vector.FixedWidthView
	viewOK    []bool
	byteViews []vector.BytesView
}

func NewChunkKeyComparer(chunk *vector.DataChunk, columns []int) (*ChunkKeyComparer, error) {
	c := &ChunkKeyComparer{}
	if err := c.Reset(chunk, columns); err != nil {
		return nil, err
	}
	return c, nil
}

// Reset borrows a new batch while reusing the comparer metadata. Call Clear
// before returning a comparer to a query pool so it cannot retain input chunks.
func (c *ChunkKeyComparer) Reset(chunk *vector.DataChunk, columns []int) error {
	c.Clear()
	if chunk == nil {
		return errors.New("nil key chunk")
	}
	c.chunk = chunk
	if cap(c.columns) < len(columns) {
		c.columns = make([]int, len(columns))
		c.views = make([]vector.FixedWidthView, len(columns))
		c.viewOK = make([]bool, len(columns))
		c.byteViews = make([]vector.BytesView, len(columns))
	}
	c.columns, c.views, c.viewOK, c.byteViews = c.columns[:len(columns)], c.views[:len(columns)], c.viewOK[:len(columns)], c.byteViews[:len(columns)]
	copy(c.columns, columns)
	for i, col := range columns {
		if col < 0 || col >= len(chunk.Columns) {
			return errors.New("projected row key index out of range")
		}
		c.views[i], c.viewOK[i] = chunk.Columns[col].BorrowFixedWidth()
		c.byteViews[i], _ = chunk.Columns[col].BorrowBytes()
	}
	return nil
}

func (c *ChunkKeyComparer) Clear() {
	c.chunk = nil
	clear(c.views)
	clear(c.byteViews)
}

// Equal compares one chunk row against encoded key columns. The encoded layout
// must be immutable for the lifetime of the comparer.
func (c *ChunkKeyComparer) Equal(chunkRow int, encodedLayout RowLayout, encoded []byte, encodedColumns []int, nulls NullEquality) (bool, error) {
	if !encodedLayout.relocatable {
		return false, errors.New("key comparison requires relocatable layout")
	}
	if chunkRow < 0 || chunkRow >= c.chunk.Cardinality {
		return false, errors.New("probe row out of range")
	}
	if len(c.columns) != len(encodedColumns) {
		return false, errors.New("projected row key count mismatch")
	}
	for i, cc := range c.columns {
		ec := encodedColumns[i]
		if ec < 0 || ec >= len(encodedLayout.Columns) {
			return false, errors.New("projected row key index out of range")
		}
		v := c.chunk.Columns[cc]
		col := encodedLayout.Columns[ec]
		if !v.Type.Same(col.Type) {
			return false, errors.New("projected row key type mismatch")
		}
		var data []byte
		valid := false
		if c.viewOK[i] {
			data, valid = c.views[i].Value(chunkRow)
		} else {
			data, valid = c.byteViews[i].Value(chunkRow)
		}
		ed, encodedValid, err := encodedLayout.ColumnData(encoded, ec)
		if err != nil {
			return false, err
		}
		if !valid || !encodedValid {
			if !valid && !encodedValid && nulls == NullsEqual {
				continue
			}
			return false, nil
		}
		var eq bool
		switch col.Type.ID {
		case types.Float:
			eq = semantics.EqualFloat32(math.Float32frombits(binary.LittleEndian.Uint32(data)), math.Float32frombits(binary.LittleEndian.Uint32(ed)))
		case types.Double:
			eq = semantics.EqualFloat64(math.Float64frombits(binary.LittleEndian.Uint64(data)), math.Float64frombits(binary.LittleEndian.Uint64(ed)))
		case types.Varchar:
			eq = bytes.Equal(data, ed)
		default:
			eq = bytes.Equal(data, ed)
		}
		if !eq {
			return false, nil
		}
	}
	return true, nil
}

func EqualProjectedRows(aLayout RowLayout, a []byte, aColumns []int, bLayout RowLayout, b []byte, bColumns []int, nulls NullEquality) (bool, error) {
	if len(aColumns) != len(bColumns) {
		return false, errors.New("projected row key count mismatch")
	}
	for i := range aColumns {
		ac, bc := aColumns[i], bColumns[i]
		if ac < 0 || ac >= len(aLayout.Columns) || bc < 0 || bc >= len(bLayout.Columns) {
			return false, errors.New("projected row key index out of range")
		}
		if !aLayout.Columns[ac].Type.Same(bLayout.Columns[bc].Type) {
			return false, errors.New("projected row key type mismatch")
		}
		ad, av, err := aLayout.ColumnData(a, ac)
		if err != nil {
			return false, err
		}
		bd, bv, err := bLayout.ColumnData(b, bc)
		if err != nil {
			return false, err
		}
		if !av || !bv {
			if !av && !bv && nulls == NullsEqual {
				continue
			}
			return false, nil
		}
		eq, err := equalEncoded(aLayout.Columns[ac], ad, bd)
		if err != nil || !eq {
			return eq, err
		}
	}
	return true, nil
}
