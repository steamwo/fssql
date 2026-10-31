package hash

import (
	"encoding/binary"
	"errors"
	"fmt"

	"github.com/steamwo/fssql/internal/core/memory"
	"github.com/steamwo/fssql/internal/core/types"
	"github.com/steamwo/fssql/internal/core/vector"
	"github.com/steamwo/fssql/internal/semantics"
)

func VectorHash(v *vector.Vector, out []uint64) error {
	if len(out) < v.Count() {
		return errors.New("hash output too small")
	}
	for i := 0; i < v.Count(); i++ {
		if !v.IsValid(i) {
			out[i] = semantics.HashUint64(0x9ae16a3b2f90404f)
			continue
		}
		switch v.Type.ID {
		case types.BigInt:
			x, _ := v.Int64(i)
			out[i] = semantics.HashInt64(x)
		case types.Boolean:
			x, _ := v.Bool(i)
			out[i] = semantics.HashBool(x)
		case types.Varchar:
			r, heap, _ := v.StringRefAt(i)
			var data []byte
			if r.External {
				data = heap.ExternalBytes(r)
			} else {
				data = r.Inline[:r.Length]
			}
			out[i] = semantics.HashBytes(data)
		default:
			return fmt.Errorf("hash unsupported type %v", v.Type.ID)
		}
	}
	return nil
}

type ColumnLayout struct {
	Type   types.LogicalType
	Offset int
	Width  int
}
type RowLayout struct {
	Columns  []ColumnLayout
	RowWidth int
}

func NewRowLayout(ts []types.LogicalType) (RowLayout, error) {
	l := RowLayout{}
	for _, t := range ts {
		w, ok := t.FixedSize()
		if !ok {
			return RowLayout{}, fmt.Errorf("row layout requires fixed-width type %v", t.ID)
		}
		l.Columns = append(l.Columns, ColumnLayout{Type: t, Offset: l.RowWidth, Width: w})
		l.RowWidth += w
	}
	return l, nil
}

type TupleDataCollection struct {
	scope                     *memory.Scope
	bm                        *memory.BufferManager
	buf                       *memory.Buffer
	rowWidth, count, capacity int
}

func NewTupleDataCollection(scope *memory.Scope, bm *memory.BufferManager, rowWidth, capacity int) (*TupleDataCollection, error) {
	if rowWidth <= 0 {
		return nil, errors.New("invalid row width")
	}
	if capacity < 1 {
		capacity = 1
	}
	b, err := bm.Allocate(scope, memory.TagHashTable, rowWidth*capacity)
	if err != nil {
		return nil, err
	}
	return &TupleDataCollection{scope: scope, bm: bm, buf: b, rowWidth: rowWidth, capacity: capacity}, nil
}
func (c *TupleDataCollection) Append(row []byte) error {
	if len(row) != c.rowWidth {
		return errors.New("row width mismatch")
	}
	if c.count == c.capacity {
		next := c.capacity * 2
		if err := c.buf.Resize(next*c.rowWidth, nil); err != nil {
			return err
		}
		c.capacity = next
	}
	copy(c.buf.Bytes()[c.count*c.rowWidth:(c.count+1)*c.rowWidth], row)
	c.count++
	return nil
}
func (c *TupleDataCollection) Count() int { return c.count }
func (c *TupleDataCollection) Row(i int) []byte {
	return c.buf.Bytes()[i*c.rowWidth : (i+1)*c.rowWidth]
}
func (c *TupleDataCollection) Serialize() (*memory.Buffer, error) {
	out, err := c.bm.Allocate(c.scope, memory.TagHashTable, 8+c.count*c.rowWidth)
	if err != nil {
		return nil, err
	}
	dst := out.Bytes()
	binary.LittleEndian.PutUint32(dst[:4], uint32(c.rowWidth))
	binary.LittleEndian.PutUint32(dst[4:8], uint32(c.count))
	copy(dst[8:], c.buf.Bytes()[:c.count*c.rowWidth])
	return out, nil
}
func Deserialize(data []byte, scope *memory.Scope, bm *memory.BufferManager) (*TupleDataCollection, error) {
	if len(data) < 8 {
		return nil, errors.New("short tuple data")
	}
	w := int(binary.LittleEndian.Uint32(data[:4]))
	n := int(binary.LittleEndian.Uint32(data[4:8]))
	if w <= 0 || len(data) != 8+w*n {
		return nil, errors.New("invalid tuple data")
	}
	c, err := NewTupleDataCollection(scope, bm, w, n)
	if err != nil {
		return nil, err
	}
	copy(c.buf.Bytes(), data[8:])
	c.count = n
	return c, nil
}
func (c *TupleDataCollection) Release() {
	if c.buf != nil {
		c.buf.Release()
		c.buf = nil
	}
}

const slotWidth = 32

type HashTable struct {
	buf             *memory.Buffer
	capacity, count int
}

func NewHashTable(scope *memory.Scope, bm *memory.BufferManager, capacity int) (*HashTable, error) {
	if capacity < 8 {
		capacity = 8
	}
	cap2 := 1
	for cap2 < capacity {
		cap2 <<= 1
	}
	b, err := bm.Allocate(scope, memory.TagHashTable, cap2*slotWidth)
	if err != nil {
		return nil, err
	}
	return &HashTable{buf: b, capacity: cap2}, nil
}
func (h *HashTable) slot(i int) []byte { return h.buf.Bytes()[i*slotWidth : (i+1)*slotWidth] }
func (h *HashTable) Insert(key, value int64) error {
	if h.count*10 >= h.capacity*7 {
		return errors.New("hash table load factor exceeded")
	}
	hashv := semantics.HashInt64(key)
	mask := h.capacity - 1
	for probe := 0; probe < h.capacity; probe++ {
		idx := (int(hashv) + probe) & mask
		s := h.slot(idx)
		if s[0] == 0 {
			s[0] = 1
			binary.LittleEndian.PutUint64(s[8:16], hashv)
			binary.LittleEndian.PutUint64(s[16:24], uint64(key))
			binary.LittleEndian.PutUint64(s[24:32], uint64(value))
			h.count++
			return nil
		}
		if binary.LittleEndian.Uint64(s[8:16]) == hashv && int64(binary.LittleEndian.Uint64(s[16:24])) == key {
			binary.LittleEndian.PutUint64(s[24:32], uint64(value))
			return nil
		}
	}
	return errors.New("hash table full")
}
func (h *HashTable) Lookup(key int64) (int64, bool) {
	hashv := semantics.HashInt64(key)
	mask := h.capacity - 1
	for probe := 0; probe < h.capacity; probe++ {
		idx := (int(hashv) + probe) & mask
		s := h.slot(idx)
		if s[0] == 0 {
			return 0, false
		}
		if binary.LittleEndian.Uint64(s[8:16]) == hashv && int64(binary.LittleEndian.Uint64(s[16:24])) == key {
			return int64(binary.LittleEndian.Uint64(s[24:32])), true
		}
	}
	return 0, false
}
func (h *HashTable) Release() {
	if h.buf != nil {
		h.buf.Release()
		h.buf = nil
	}
}

type RadixPartitioner struct {
	bits uint8
	mask uint64
}

func NewRadixPartitioner(bits uint8) (RadixPartitioner, error) {
	if bits == 0 || bits > 16 {
		return RadixPartitioner{}, errors.New("radix bits must be 1..16")
	}
	return RadixPartitioner{bits: bits, mask: (uint64(1) << bits) - 1}, nil
}
func (r RadixPartitioner) Partition(hash uint64) int { return int(hash & r.mask) }
func (r RadixPartitioner) Count() int                { return 1 << r.bits }
