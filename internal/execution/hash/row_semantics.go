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
	case types.Boolean, types.TinyInt, types.SmallInt, types.Integer, types.DateType, types.BigInt, types.TimestampType, types.Decimal, types.Varchar:
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
		switch col.Type.ID {
		case types.Boolean:
			dst[i].SetBool(dstRow, data[0] != 0)
		case types.TinyInt:
			dst[i].SetInt8(dstRow, int8(data[0]))
		case types.SmallInt:
			dst[i].SetInt16(dstRow, int16(binary.LittleEndian.Uint16(data)))
		case types.Integer, types.DateType:
			dst[i].SetInt32(dstRow, int32(binary.LittleEndian.Uint32(data)))
		case types.BigInt, types.TimestampType:
			dst[i].SetInt64(dstRow, int64(binary.LittleEndian.Uint64(data)))
		case types.Float:
			dst[i].SetFloat32(dstRow, math.Float32frombits(binary.LittleEndian.Uint32(data)))
		case types.Double:
			dst[i].SetFloat64(dstRow, math.Float64frombits(binary.LittleEndian.Uint64(data)))
		case types.Decimal:
			if col.Type.Decimal.Width <= 18 {
				dst[i].SetInt64(dstRow, int64(binary.LittleEndian.Uint64(data)))
			} else {
				dst[i].SetInt128(dstRow, types.Int128{Lo: binary.LittleEndian.Uint64(data[:8]), Hi: int64(binary.LittleEndian.Uint64(data[8:16]))})
			}
		case types.Varchar:
			if err := dst[i].SetStringBytes(dstRow, data); err != nil {
				return err
			}
		default:
			return errors.New("materialize unsupported type")
		}
	}
	return nil
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
	c.used = int(binary.LittleEndian.Uint32(c.offsets.Bytes()[count*4 : count*4+4]))
	return nil
}
