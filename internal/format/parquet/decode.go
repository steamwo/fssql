package parquet

import (
	"encoding/binary"
	"errors"
	"fmt"
	"math"

	coretypes "github.com/steamwo/fssql/internal/core/types"
	"github.com/steamwo/fssql/internal/core/vector"
)

func DecodePlainInto(dst *vector.Vector, start, count int, physical PhysicalType, data []byte) (int, error) {
	if dst == nil {
		return 0, errors.New("parquet: nil destination vector")
	}
	if start < 0 || count < 0 || start+count > dst.Count() {
		return 0, errors.New("parquet: destination bounds")
	}
	off := 0
	switch physical {
	case TypeBoolean:
		need := (count + 7) / 8
		if len(data) < need {
			return 0, errors.New("parquet: truncated boolean plain data")
		}
		for i := 0; i < count; i++ {
			dst.SetBool(start+i, data[i>>3]&(1<<uint(i&7)) != 0)
		}
		return need, nil
	case TypeInt32:
		need := count * 4
		if len(data) < need {
			return 0, errors.New("parquet: truncated int32 plain data")
		}
		for i := 0; i < count; i++ {
			dst.SetInt32(start+i, int32(binary.LittleEndian.Uint32(data[off:off+4])))
			off += 4
		}
	case TypeInt64:
		need := count * 8
		if len(data) < need {
			return 0, errors.New("parquet: truncated int64 plain data")
		}
		for i := 0; i < count; i++ {
			dst.SetInt64(start+i, int64(binary.LittleEndian.Uint64(data[off:off+8])))
			off += 8
		}
	case TypeFloat:
		need := count * 4
		if len(data) < need {
			return 0, errors.New("parquet: truncated float plain data")
		}
		for i := 0; i < count; i++ {
			dst.SetFloat32(start+i, math.Float32frombits(binary.LittleEndian.Uint32(data[off:off+4])))
			off += 4
		}
	case TypeDouble:
		need := count * 8
		if len(data) < need {
			return 0, errors.New("parquet: truncated double plain data")
		}
		for i := 0; i < count; i++ {
			dst.SetFloat64(start+i, math.Float64frombits(binary.LittleEndian.Uint64(data[off:off+8])))
			off += 8
		}
	case TypeByteArray:
		for i := 0; i < count; i++ {
			if off+4 > len(data) {
				return 0, errors.New("parquet: truncated byte-array length")
			}
			n := int(binary.LittleEndian.Uint32(data[off : off+4]))
			off += 4
			if n < 0 || off+n > len(data) {
				return 0, errors.New("parquet: truncated byte-array value")
			}
			if err := dst.SetStringBytes(start+i, data[off:off+n]); err != nil {
				return 0, err
			}
			off += n
		}
	default:
		return 0, fmt.Errorf("parquet: PLAIN physical type %d not implemented", physical)
	}
	return off, nil
}

func CopyDictionaryIndexes(dst *vector.Vector, start int, dict *vector.Vector, indexes []uint32) error {
	if dst == nil || dict == nil {
		return errors.New("parquet: nil dictionary vector")
	}
	if start < 0 || start+len(indexes) > dst.Count() {
		return errors.New("parquet: dictionary destination bounds")
	}
	for i, idx := range indexes {
		if int(idx) >= dict.Count() {
			return fmt.Errorf("parquet: dictionary index %d out of range %d", idx, dict.Count())
		}
		di := int(idx)
		if !dict.IsValid(di) {
			dst.SetNull(start + i)
			continue
		}
		switch dst.Type.ID {
		case coretypes.Integer, coretypes.DateType:
			v, _ := dict.Int32(di)
			dst.SetInt32(start+i, v)
		case coretypes.BigInt, coretypes.Decimal, coretypes.TimestampType:
			v, _ := dict.Int64(di)
			dst.SetInt64(start+i, v)
		case coretypes.Float:
			v, _ := dict.Float32(di)
			dst.SetFloat32(start+i, v)
		case coretypes.Double:
			v, _ := dict.Float64(di)
			dst.SetFloat64(start+i, v)
		case coretypes.Varchar:
			v, _ := dict.String(di)
			if err := dst.SetStringBytes(start+i, []byte(v)); err != nil {
				return err
			}
		case coretypes.Boolean:
			v, _ := dict.Bool(di)
			dst.SetBool(start+i, v)
		default:
			return fmt.Errorf("parquet: dictionary core type %d not implemented", dst.Type.ID)
		}
	}
	return nil
}
