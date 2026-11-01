package vector

import (
	"encoding/binary"
	"errors"
	"fmt"

	"github.com/steamwo/fssql/internal/core/memory"
	"github.com/steamwo/fssql/internal/core/types"
)

func serializedChunkSize(c *DataChunk) (int, error) {
	size := 4
	for _, v := range c.Columns {
		switch v.Type.ID {
		case types.BigInt, types.HugeInt, types.Varchar, types.Boolean, types.Decimal:
		default:
			return 0, fmt.Errorf("serialization unsupported type %v", v.Type.ID)
		}
		size++
		if v.Type.ID == types.Decimal {
			size += 2
		}
		for i := 0; i < c.Cardinality; i++ {
			size++
			if !v.IsValid(i) {
				continue
			}
			switch v.Type.ID {
			case types.BigInt:
				size += 8
			case types.HugeInt:
				size += 16
			case types.Boolean:
				size++
			case types.Varchar:
				r, _, _ := v.StringRefAt(i)
				size += 4 + int(r.Length)
			case types.Decimal:
				if v.Type.Physical() == types.PhysicalInt128 {
					size += 16
				} else {
					size += 8
				}
			}
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
		pos++
		if v.Type.ID == types.Decimal {
			dst[pos] = v.Type.Decimal.Width
			dst[pos+1] = v.Type.Decimal.Scale
			pos += 2
		}
		for i := 0; i < c.Cardinality; i++ {
			if !v.IsValid(i) {
				dst[pos] = 0
				pos++
				continue
			}
			dst[pos] = 1
			pos++
			switch v.Type.ID {
			case types.BigInt:
				x, _ := v.Int64(i)
				binary.LittleEndian.PutUint64(dst[pos:pos+8], uint64(x))
				pos += 8
			case types.HugeInt:
				x, _ := v.Int128(i)
				binary.LittleEndian.PutUint64(dst[pos:pos+8], x.Lo)
				binary.LittleEndian.PutUint64(dst[pos+8:pos+16], uint64(x.Hi))
				pos += 16
			case types.Boolean:
				x, _ := v.Bool(i)
				if x {
					dst[pos] = 1
				}
				pos++
			case types.Varchar:
				r, h, _ := v.StringRefAt(i)
				binary.LittleEndian.PutUint32(dst[pos:pos+4], r.Length)
				pos += 4
				var src []byte
				if r.External {
					src = h.ExternalBytes(r)
				} else {
					src = r.Inline[:r.Length]
				}
				copy(dst[pos:pos+len(src)], src)
				pos += len(src)
			case types.Decimal:
				if v.Type.Physical() == types.PhysicalInt128 {
					x, _ := v.Int128(i)
					binary.LittleEndian.PutUint64(dst[pos:pos+8], x.Lo)
					binary.LittleEndian.PutUint64(dst[pos+8:pos+16], uint64(x.Hi))
					pos += 16
				} else {
					x, _ := v.Int64(i)
					binary.LittleEndian.PutUint64(dst[pos:pos+8], uint64(x))
					pos += 8
				}
			}
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
		if err := need(1); err != nil {
			cleanup()
			return nil, err
		}
		id := types.LogicalTypeID(data[pos])
		pos++
		typ := types.LogicalType{ID: id}
		if id == types.Decimal {
			if err := need(2); err != nil {
				cleanup()
				return nil, err
			}
			var err error
			typ, err = types.NewDecimal(data[pos], data[pos+1])
			if err != nil {
				cleanup()
				return nil, err
			}
			pos += 2
		}
		v, err := NewFlat(scope, bm, typ, card)
		if err != nil {
			cleanup()
			return nil, err
		}
		vs = append(vs, v)
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
			switch id {
			case types.BigInt:
				if err := need(8); err != nil {
					cleanup()
					return nil, err
				}
				v.SetInt64(i, int64(binary.LittleEndian.Uint64(data[pos:pos+8])))
				pos += 8
			case types.HugeInt:
				if err := need(16); err != nil {
					cleanup()
					return nil, err
				}
				v.SetInt128(i, types.Int128{Lo: binary.LittleEndian.Uint64(data[pos : pos+8]), Hi: int64(binary.LittleEndian.Uint64(data[pos+8 : pos+16]))})
				pos += 16
			case types.Boolean:
				if err := need(1); err != nil {
					cleanup()
					return nil, err
				}
				v.SetBool(i, data[pos] != 0)
				pos++
			case types.Varchar:
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
			case types.Decimal:
				if typ.Physical() == types.PhysicalInt128 {
					if err := need(16); err != nil {
						cleanup()
						return nil, err
					}
					v.SetInt128(i, types.Int128{Lo: binary.LittleEndian.Uint64(data[pos : pos+8]), Hi: int64(binary.LittleEndian.Uint64(data[pos+8 : pos+16]))})
					pos += 16
				} else {
					if err := need(8); err != nil {
						cleanup()
						return nil, err
					}
					v.SetInt64(i, int64(binary.LittleEndian.Uint64(data[pos:pos+8])))
					pos += 8
				}
			default:
				cleanup()
				return nil, fmt.Errorf("unsupported type %d", id)
			}
		}
	}
	if pos != len(data) {
		cleanup()
		return nil, errors.New("trailing chunk bytes")
	}
	return NewDataChunk(vs...)
}
