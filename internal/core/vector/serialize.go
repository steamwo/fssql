package vector

import (
	"encoding/binary"
	"errors"
	"fmt"

	"github.com/steamwo/fssql/internal/core/memory"
	"github.com/steamwo/fssql/internal/core/types"
)

const (
	serializedFlagUnsigned byte = 1 << iota
	serializedFlagBinary
	serializedFlagAdjustedUTC
	serializedFlagUnitNanos
	serializedFlagUnitMillis
	serializedFlagNullType
)

func serializedTypeFlags(t types.LogicalType) byte {
	var flags byte
	if t.NullType{flags|=serializedFlagNullType}
	if t.Unsigned {
		flags |= serializedFlagUnsigned
	}
	if t.Binary {
		flags |= serializedFlagBinary
	}
	if t.Timestamp.AdjustedToUTC {
		flags |= serializedFlagAdjustedUTC
	}
	switch t.Timestamp.Unit {
	case types.TimestampNanos:
		flags |= serializedFlagUnitNanos
	case types.TimestampMillis:
		flags |= serializedFlagUnitMillis
	}
	return flags
}

func logicalTypeFromSerialized(id types.LogicalTypeID, flags byte, decimal types.DecimalInfo) (types.LogicalType, error) {
	t := types.LogicalType{ID: id, Decimal: decimal}
	if flags&serializedFlagNullType!=0{if id!=types.Integer{return types.LogicalType{},fmt.Errorf("serialized NULL type must use integer storage")};t.NullType=true}
	if flags&serializedFlagUnsigned != 0 {
		switch id {
		case types.TinyInt, types.SmallInt, types.Integer, types.BigInt:
			t.Unsigned = true
		default:
			return types.LogicalType{}, fmt.Errorf("serialized unsigned modifier on type %d", id)
		}
	}
	if flags&serializedFlagBinary != 0 {
		if id != types.Varchar {
			return types.LogicalType{}, fmt.Errorf("serialized binary modifier on type %d", id)
		}
		t.Binary = true
	}
	if id == types.TimestampType {
		t.Timestamp.AdjustedToUTC = flags&serializedFlagAdjustedUTC != 0
		switch {
		case flags&serializedFlagUnitNanos != 0 && flags&serializedFlagUnitMillis != 0:
			return types.LogicalType{}, errors.New("serialized timestamp has conflicting units")
		case flags&serializedFlagUnitNanos != 0:
			t.Timestamp.Unit = types.TimestampNanos
		case flags&serializedFlagUnitMillis != 0:
			t.Timestamp.Unit = types.TimestampMillis
		default:
			t.Timestamp.Unit = types.TimestampMicros
		}
	} else if flags&(serializedFlagAdjustedUTC|serializedFlagUnitNanos|serializedFlagUnitMillis) != 0 {
		return types.LogicalType{}, fmt.Errorf("serialized timestamp modifier on type %d", id)
	}
	return t, nil
}

func serializedChunkSize(c *DataChunk) (int, error) {
	if c == nil {
		return 0, errors.New("nil chunk")
	}
	size := 4
	for _, v := range c.Columns {
		if v == nil {
			return 0, errors.New("nil vector")
		}
		switch v.Type.ID {
		case types.Boolean, types.TinyInt, types.SmallInt, types.Integer, types.BigInt,
			types.HugeInt, types.Float, types.Double, types.Decimal, types.Varchar,
			types.DateType, types.TimestampType:
		default:
			return 0, fmt.Errorf("serialization unsupported type %v", v.Type.ID)
		}
		size += 2 // logical ID + semantic flags
		if v.Type.ID == types.Decimal {
			size += 2
		}
		fixed, fixedOK := v.Type.FixedSize()
		for i := 0; i < c.Cardinality; i++ {
			size++
			if !v.IsValid(i) {
				continue
			}
			if v.Type.ID == types.Varchar {
				r, _, _ := v.StringRefAt(i)
				size += 4 + int(r.Length)
				continue
			}
			if !fixedOK {
				return 0, fmt.Errorf("serialization unsupported physical type %v", v.Type.Physical())
			}
			size += fixed
		}
	}
	return size, nil
}

func SerializeChunk(c *DataChunk, scope *memory.Scope, bm *memory.BufferManager) (*memory.Buffer, error) {
	n, err := serializedChunkSize(c)
	if err != nil {
		return nil, err
	}
	out, err := bm.Allocate(scope, memory.TagMetadata, n)
	if err != nil {
		return nil, err
	}
	dst := out.Bytes()
	pos := 0
	binary.LittleEndian.PutUint16(dst[pos:pos+2], uint16(len(c.Columns)))
	pos += 2
	binary.LittleEndian.PutUint16(dst[pos:pos+2], uint16(c.Cardinality))
	pos += 2
	for _, v := range c.Columns {
		dst[pos] = byte(v.Type.ID)
		dst[pos+1] = serializedTypeFlags(v.Type)
		pos += 2
		if v.Type.ID == types.Decimal {
			dst[pos] = v.Type.Decimal.Width
			dst[pos+1] = v.Type.Decimal.Scale
			pos += 2
		}
		fixed, _ := v.Type.FixedSize()
		for i := 0; i < c.Cardinality; i++ {
			if !v.IsValid(i) {
				dst[pos] = 0
				pos++
				continue
			}
			dst[pos] = 1
			pos++
			if v.Type.ID == types.Varchar {
				value, _ := v.Bytes(i)
				binary.LittleEndian.PutUint32(dst[pos:pos+4], uint32(len(value)))
				pos += 4
				copy(dst[pos:pos+len(value)], value)
				pos += len(value)
				continue
			}
			idx, base := v.resolve(i)
			copy(dst[pos:pos+fixed], base.data.Bytes()[idx*fixed:(idx+1)*fixed])
			pos += fixed
		}
	}
	return out, nil
}

func DeserializeChunk(data []byte, scope *memory.Scope, bm *memory.BufferManager) (*DataChunk, error) {
	if len(data) < 4 {
		return nil, errors.New("short chunk")
	}
	pos := 0
	cols := int(binary.LittleEndian.Uint16(data[pos : pos+2]))
	pos += 2
	card := int(binary.LittleEndian.Uint16(data[pos : pos+2]))
	pos += 2
	vs := make([]*Vector, 0, cols)
	cleanup := func() {
		for _, v := range vs {
			v.Release()
		}
	}
	need := func(n int) error {
		if n < 0 || pos+n > len(data) {
			return errors.New("truncated chunk")
		}
		return nil
	}
	for ci := 0; ci < cols; ci++ {
		if err := need(2); err != nil {
			cleanup()
			return nil, err
		}
		id := types.LogicalTypeID(data[pos])
		flags := data[pos+1]
		pos += 2
		decimal := types.DecimalInfo{}
		if id == types.Decimal {
			if err := need(2); err != nil {
				cleanup()
				return nil, err
			}
			decimal = types.DecimalInfo{Width: data[pos], Scale: data[pos+1]}
			if _, err := types.NewDecimal(decimal.Width, decimal.Scale); err != nil {
				cleanup()
				return nil, err
			}
			pos += 2
		}
		typ, err := logicalTypeFromSerialized(id, flags, decimal)
		if err != nil {
			cleanup()
			return nil, err
		}
		v, err := NewFlat(scope, bm, typ, card)
		if err != nil {
			cleanup()
			return nil, err
		}
		vs = append(vs, v)
		fixed, fixedOK := typ.FixedSize()
		for i := 0; i < card; i++ {
			if err := need(1); err != nil {
				cleanup()
				return nil, err
			}
			valid := data[pos]
			pos++
			if valid == 0 {
				v.SetNull(i)
				continue
			}
			if valid != 1 {
				cleanup()
				return nil, errors.New("invalid serialized validity byte")
			}
			if typ.ID == types.Varchar {
				if err := need(4); err != nil {
					cleanup()
					return nil, err
				}
				n := int(binary.LittleEndian.Uint32(data[pos : pos+4]))
				pos += 4
				if err := need(n); err != nil {
					cleanup()
					return nil, err
				}
				if err := v.SetStringBytes(i, data[pos:pos+n]); err != nil {
					cleanup()
					return nil, err
				}
				pos += n
				continue
			}
			if !fixedOK {
				cleanup()
				return nil, fmt.Errorf("unsupported type %d", id)
			}
			if err := need(fixed); err != nil {
				cleanup()
				return nil, err
			}
			copy(v.data.Bytes()[i*fixed:(i+1)*fixed], data[pos:pos+fixed])
			v.SetValid(i)
			pos += fixed
		}
	}
	if pos != len(data) {
		cleanup()
		return nil, errors.New("trailing chunk bytes")
	}
	return NewDataChunk(vs...)
}
