package sort

import (
	"bytes"
	"encoding/binary"
	"fmt"
	"math"

	"github.com/steamwo/fssql/internal/core/types"
	"github.com/steamwo/fssql/internal/core/vector"
	exechash "github.com/steamwo/fssql/internal/execution/hash"
	"github.com/steamwo/fssql/internal/semantics"
)

func compareFixedInputKey(typ types.LogicalType, key Key, input []byte, inputValid bool, threshold []byte, thresholdValid bool) (int, error) {
	if !inputValid || !thresholdValid {
		if inputValid == thresholdValid {
			return 0, nil
		}
		if !inputValid == key.NullsFirst {
			return -1, nil
		}
		return 1, nil
	}
	cmp, err := compareEncoded(typ, input, threshold)
	if key.Desc {
		cmp = -cmp
	}
	return cmp, err
}

func compareInputRow(layout exechash.RowLayout, keys []Key, chunk *vector.DataChunk, row int, encoded []byte) (int, error) {
	for _, key := range keys {
		v := chunk.Columns[key.Column]
		data, valid, err := layout.ColumnData(encoded, key.Column)
		if err != nil {
			return 0, err
		}
		inputValid := v.IsValid(row)
		if !inputValid || !valid {
			if !inputValid && !valid {
				continue
			}
			if !inputValid == key.NullsFirst {
				return -1, nil
			}
			return 1, nil
		}
		cmp, err := compareInputValue(v, row, data)
		if err != nil {
			return 0, err
		}
		if cmp != 0 {
			if key.Desc {
				cmp = -cmp
			}
			return cmp, nil
		}
	}
	return 0, nil
}

func compareInputValue(v *vector.Vector, row int, data []byte) (int, error) {
	switch v.Type.ID {
	case types.Varchar:
		r, h, _ := v.StringRefAt(row)
		if r.External {
			return bytes.Compare(h.ExternalBytes(r), data), nil
		}
		return bytes.Compare(r.Inline[:r.Length], data), nil
	case types.Boolean:
		x, _ := v.Bool(row)
		y := data[0] != 0
		if x == y {
			return 0, nil
		}
		if !x {
			return -1, nil
		}
		return 1, nil
	case types.TinyInt:
		x, _ := v.Int8(row)
		return semantics.CompareInt64(int64(x), int64(int8(data[0]))), nil
	case types.SmallInt:
		x, _ := v.Int16(row)
		return semantics.CompareInt64(int64(x), int64(int16(binary.LittleEndian.Uint16(data)))), nil
	case types.Integer, types.DateType:
		x, _ := v.Int32(row)
		return semantics.CompareInt64(int64(x), int64(int32(binary.LittleEndian.Uint32(data)))), nil
	case types.BigInt, types.TimestampType:
		x, _ := v.Int64(row)
		return semantics.CompareInt64(x, int64(binary.LittleEndian.Uint64(data))), nil
	case types.Float:
		x, _ := v.Float32(row)
		return semantics.CompareFloat32(x, math.Float32frombits(binary.LittleEndian.Uint32(data))), nil
	case types.Double:
		x, _ := v.Float64(row)
		return semantics.CompareFloat64(x, math.Float64frombits(binary.LittleEndian.Uint64(data))), nil
	case types.HugeInt, types.Decimal:
		if v.Type.ID == types.Decimal && v.Type.Decimal.Width <= 18 {
			x, _ := v.Int64(row)
			return semantics.CompareInt64(x, int64(binary.LittleEndian.Uint64(data))), nil
		}
		x, _ := v.Int128(row)
		return types.CompareInt128(x, types.Int128{Lo: binary.LittleEndian.Uint64(data[:8]), Hi: int64(binary.LittleEndian.Uint64(data[8:16]))}), nil
	}
	return 0, fmt.Errorf("sort input type %d unsupported", v.Type.ID)
}
