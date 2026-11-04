package hash

import (
	"encoding/binary"
	"errors"
	"fmt"
	"math"

	"github.com/steamwo/fssql/internal/core/memory"
	"github.com/steamwo/fssql/internal/core/types"
	"github.com/steamwo/fssql/internal/core/vector"
	"github.com/steamwo/fssql/internal/semantics"
)

func hashValue(v *vector.Vector, row int) (uint64, error) {
	if !v.IsValid(row) {
		return semantics.HashNull(), nil
	}
	if v.Type.ID == types.Varchar {
		view, _ := v.BorrowBytes()
		data, _ := view.Value(row)
		return semantics.HashBytes(data), nil
	}
	h := fixedHasher(v.Type)
	if h == nil {
		return 0, fmt.Errorf("hash unsupported type %v", v.Type.ID)
	}
	view, _ := v.BorrowFixedWidth()
	data, _ := view.Value(row)
	return h(data), nil
}

func VectorHash(v *vector.Vector, out []uint64) error {
	return hashVectorBatch(v, v.Count(), out, false)
}

// HashChunk dispatches once per column and combines its batch hashes.
func HashChunk(chunk *vector.DataChunk, keyColumns []int, out []uint64) error {
	if chunk == nil {
		return errors.New("nil chunk")
	}
	if len(out) < chunk.Cardinality {
		return errors.New("hash output too small")
	}
	for _, col := range keyColumns {
		if col < 0 || col >= len(chunk.Columns) {
			return fmt.Errorf("key column %d out of range", col)
		}
	}
	if len(keyColumns) == 0 {
		for i := 0; i < chunk.Cardinality; i++ {
			out[i] = semantics.HashUint64(0)
		}
		return nil
	}
	for i, col := range keyColumns {
		if err := hashVectorBatch(chunk.Columns[col], chunk.Cardinality, out, i != 0); err != nil {
			return err
		}
	}
	return nil
}
type ColumnLayout struct {
	Type     types.LogicalType
	Offset   int
	Width    int
	Variable bool
}

type RowLayout struct {
	Columns         []ColumnLayout
	RowWidth        int // legacy fixed-width layout width
	NullBytes       int
	FixedWidth      int // relocatable-row fixed prefix width
	VariableColumns int
	relocatable     bool
}

// NewRowLayout preserves the Phase 0 fixed-width row ABI.
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
	l.FixedWidth = l.RowWidth
	return l, nil
}

// NewRelocatableRowLayout creates the pointer-free row representation used by
// Phase 2 hash/join/aggregate spill paths. VARCHAR values use an 8-byte
// offset/length descriptor whose offset is relative to the beginning of row.
func NewRelocatableRowLayout(ts []types.LogicalType) (RowLayout, error) {
	l := RowLayout{NullBytes: (len(ts) + 7) / 8, relocatable: true}
	off := l.NullBytes
	for _, t := range ts {
		w, ok := t.FixedSize()
		variable := false
		if t.ID == types.Varchar {
			w, ok, variable = 8, true, true
		}
		if !ok {
			return RowLayout{}, fmt.Errorf("relocatable row layout unsupported type %v", t.ID)
		}
		l.Columns = append(l.Columns, ColumnLayout{Type: t, Offset: off, Width: w, Variable: variable})
		off += w
		if variable {
			l.VariableColumns++
		}
	}
	l.FixedWidth = off
	l.RowWidth = off
	return l, nil
}

func (l RowLayout) Relocatable() bool { return l.relocatable }

func (l RowLayout) RowSize(chunk *vector.DataChunk, row int) (int, error) {
	if !l.relocatable {
		return l.RowWidth, nil
	}
	if chunk == nil || row < 0 || row >= chunk.Cardinality {
		return 0, errors.New("row out of range")
	}
	if len(chunk.Columns) != len(l.Columns) {
		return 0, errors.New("row layout column count mismatch")
	}
	size := l.FixedWidth
	for i, col := range l.Columns {
		if !col.Variable || !chunk.Columns[i].IsValid(row) {
			continue
		}
		r, _, ok := chunk.Columns[i].StringRefAt(row)
		if !ok {
			continue
		}
		if uint64(size)+uint64(r.Length) > uint64(math.MaxInt) {
			return 0, errors.New("row size overflow")
		}
		size += int(r.Length)
	}
	return size, nil
}

func setNullBit(dst []byte, column int) { dst[column>>3] |= byte(1 << uint(column&7)) }
func (l RowLayout) IsNull(row []byte, column int) bool {
	if !l.relocatable || column < 0 || column >= len(l.Columns) || len(row) < l.NullBytes {
		return false
	}
	return row[column>>3]&(byte(1)<<uint(column&7)) != 0
}

func (l RowLayout) ColumnData(row []byte, column int) ([]byte, bool, error) {
	if column < 0 || column >= len(l.Columns) {
		return nil, false, errors.New("column out of range")
	}
	col := l.Columns[column]
	if l.relocatable && l.IsNull(row, column) {
		return nil, false, nil
	}
	if col.Offset < 0 || col.Offset+col.Width > len(row) {
		return nil, false, errors.New("short row")
	}
	if !col.Variable {
		return row[col.Offset : col.Offset+col.Width], true, nil
	}
	off := int(binary.LittleEndian.Uint32(row[col.Offset : col.Offset+4]))
	length := int(binary.LittleEndian.Uint32(row[col.Offset+4 : col.Offset+8]))
	if off < l.FixedWidth || length < 0 || off > len(row) || length > len(row)-off {
		return nil, false, errors.New("invalid variable row descriptor")
	}
	return row[off : off+length], true, nil
}

func (l RowLayout) WriteRow(chunk *vector.DataChunk, row int, dst []byte) (int, error) {
	if !l.relocatable {
		return 0, errors.New("WriteRow requires relocatable layout")
	}
	need, err := l.RowSize(chunk, row)
	if err != nil {
		return 0, err
	}
	if len(dst) < need {
		return 0, errors.New("row destination too small")
	}
	for i := 0; i < need; i++ {
		dst[i] = 0
	}
	varOff := l.FixedWidth
	for i, col := range l.Columns {
		v := chunk.Columns[i]
		if !v.IsValid(row) {
			setNullBit(dst, i)
			continue
		}
		d := dst[col.Offset : col.Offset+col.Width]
		switch col.Type.ID {
		case types.Boolean:
			x, _ := v.Bool(row)
			if x {
				d[0] = 1
			}
		case types.TinyInt:
			x, _ := v.Int8(row)
			d[0] = byte(x)
		case types.SmallInt:
			x, _ := v.Int16(row)
			binary.LittleEndian.PutUint16(d, uint16(x))
		case types.Integer, types.DateType:
			x, _ := v.Int32(row)
			binary.LittleEndian.PutUint32(d, uint32(x))
		case types.BigInt, types.TimestampType:
			x, _ := v.Int64(row)
			binary.LittleEndian.PutUint64(d, uint64(x))
		case types.HugeInt:
			x, _ := v.Int128(row)
			binary.LittleEndian.PutUint64(d[:8], x.Lo)
			binary.LittleEndian.PutUint64(d[8:16], uint64(x.Hi))
		case types.Float:
			x, _ := v.Float32(row)
			binary.LittleEndian.PutUint32(d, math.Float32bits(x))
		case types.Double:
			x, _ := v.Float64(row)
			binary.LittleEndian.PutUint64(d, math.Float64bits(x))
		case types.Decimal:
			if col.Type.Decimal.Width <= 18 {
				x, _ := v.Int64(row)
				binary.LittleEndian.PutUint64(d, uint64(x))
			} else {
				x, _ := v.Int128(row)
				binary.LittleEndian.PutUint64(d[:8], x.Lo)
				binary.LittleEndian.PutUint64(d[8:16], uint64(x.Hi))
			}
		case types.Varchar:
			r, heap, _ := v.StringRefAt(row)
			var src []byte
			if r.External {
				src = heap.ExternalBytes(r)
			} else {
				src = r.Inline[:r.Length]
			}
			binary.LittleEndian.PutUint32(d[:4], uint32(varOff))
			binary.LittleEndian.PutUint32(d[4:8], uint32(len(src)))
			copy(dst[varOff:varOff+len(src)], src)
			varOff += len(src)
		default:
			return 0, fmt.Errorf("row encoding unsupported type %v", col.Type.ID)
		}
	}
	return need, nil
}

type TupleDataCollection struct {
	scope                           *memory.Scope
	bm                              *memory.BufferManager
	buf                             *memory.Buffer
	offsets                         *memory.Buffer
	layout                          *RowLayout
	rowWidth, count, capacity, used int
}

// BorrowRowReader snapshots the managed backing slices for an exclusive read
// phase. The caller must not append, resize or release the collection until
// the reader is discarded. It acquires buffer locks once, outside comparisons.
func (c *TupleDataCollection) BorrowRowReader() func(int) []byte {
	data := c.buf.Bytes()
	count := c.count
	if c.layout == nil {
		return func(row int) []byte {
			if row < 0 || row >= count {
				return nil
			}
			return data[row*c.rowWidth : (row+1)*c.rowWidth]
		}
	}
	offsets := c.offsets.Bytes()
	return func(row int) []byte {
		if row < 0 || row >= count {
			return nil
		}
		start := binary.LittleEndian.Uint32(offsets[row*4 : row*4+4])
		end := binary.LittleEndian.Uint32(offsets[(row+1)*4 : (row+1)*4+4])
		return data[start:end]
	}
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

func NewTupleDataCollectionForLayout(scope *memory.Scope, bm *memory.BufferManager, layout RowLayout, capacity int) (*TupleDataCollection, error) {
	if !layout.relocatable {
		return nil, errors.New("tuple layout must be relocatable")
	}
	if capacity < 1 {
		capacity = 1
	}
	dataCap := layout.FixedWidth * capacity
	if dataCap < 64 {
		dataCap = 64
	}
	data, err := bm.Allocate(scope, memory.TagHashTable, dataCap)
	if err != nil {
		return nil, err
	}
	offsets, err := bm.Allocate(scope, memory.TagHashTable, (capacity+1)*4)
	if err != nil {
		data.Release()
		return nil, err
	}
	copyLayout := layout
	return &TupleDataCollection{scope: scope, bm: bm, buf: data, offsets: offsets, layout: &copyLayout, capacity: capacity}, nil
}

func (c *TupleDataCollection) Append(row []byte) error {
	if c.layout != nil {
		return errors.New("raw append not allowed for layout collection")
	}
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

func (c *TupleDataCollection) growRows(nextCount int) error {
	if nextCount <= c.capacity {
		return nil
	}
	next := c.capacity * 2
	if next < nextCount {
		next = nextCount
	}
	if err := c.offsets.Resize((next+1)*4, nil); err != nil {
		return err
	}
	c.capacity = next
	return nil
}
func (c *TupleDataCollection) growData(need int) error {
	if need <= c.buf.Len() {
		return nil
	}
	next := c.buf.Len() * 2
	if next < need {
		next = need
	}
	return c.buf.Resize(next, nil)
}

func (c *TupleDataCollection) AppendChunkRow(chunk *vector.DataChunk, row int) error {
	if c.layout == nil {
		return errors.New("collection has no row layout")
	}
	size, err := c.layout.RowSize(chunk, row)
	if err != nil {
		return err
	}
	if uint64(c.used)+uint64(size) > uint64(math.MaxUint32) {
		return errors.New("tuple data exceeds 4GiB offset range")
	}
	if err := c.growRows(c.count + 1); err != nil {
		return err
	}
	if err := c.growData(c.used + size); err != nil {
		return err
	}
	binary.LittleEndian.PutUint32(c.offsets.Bytes()[c.count*4:c.count*4+4], uint32(c.used))
	n, err := c.layout.WriteRow(chunk, row, c.buf.Bytes()[c.used:c.used+size])
	if err != nil {
		return err
	}
	c.used += n
	c.count++
	binary.LittleEndian.PutUint32(c.offsets.Bytes()[c.count*4:c.count*4+4], uint32(c.used))
	return nil
}
func (c *TupleDataCollection) AppendEncodedRow(row []byte) error {
	if c.layout == nil {
		return errors.New("collection has no row layout")
	}
	if len(row) < c.layout.FixedWidth {
		return errors.New("encoded row too short")
	}
	if uint64(c.used)+uint64(len(row)) > uint64(math.MaxUint32) {
		return errors.New("tuple data exceeds 4GiB offset range")
	}
	if err := c.growRows(c.count + 1); err != nil {
		return err
	}
	if err := c.growData(c.used + len(row)); err != nil {
		return err
	}
	binary.LittleEndian.PutUint32(c.offsets.Bytes()[c.count*4:c.count*4+4], uint32(c.used))
	copy(c.buf.Bytes()[c.used:c.used+len(row)], row)
	c.used += len(row)
	c.count++
	binary.LittleEndian.PutUint32(c.offsets.Bytes()[c.count*4:c.count*4+4], uint32(c.used))
	return nil
}
func (c *TupleDataCollection) Count() int { return c.count }
func (c *TupleDataCollection) AllocatedBytes() int {
	if c == nil {
		return 0
	}
	n := 0
	if c.buf != nil {
		n += c.buf.Len()
	}
	if c.offsets != nil {
		n += c.offsets.Len()
	}
	return n
}
func (c *TupleDataCollection) Row(i int) []byte {
	if i < 0 || i >= c.count {
		return nil
	}
	if c.layout == nil {
		return c.buf.Bytes()[i*c.rowWidth : (i+1)*c.rowWidth]
	}
	start := int(binary.LittleEndian.Uint32(c.offsets.Bytes()[i*4 : i*4+4]))
	end := int(binary.LittleEndian.Uint32(c.offsets.Bytes()[(i+1)*4 : (i+1)*4+4]))
	return c.buf.Bytes()[start:end]
}
func (c *TupleDataCollection) Serialize() (*memory.Buffer, error) {
	if c.layout != nil {
		return nil, errors.New("relocatable collection serialization is handled by spill page consumers")
	}
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
	payload := len(data) - 8
	if w <= 0 || payload%w != 0 || payload/w != n {
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
	if c.offsets != nil {
		c.offsets.Release()
		c.offsets = nil
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
func (r RadixPartitioner) PartitionAtDepth(hash uint64, depth int) (int, error) {
	if depth < 0 {
		return 0, errors.New("negative radix depth")
	}
	shift := int(r.bits) * depth
	if shift >= 64 {
		return 0, errors.New("radix depth exceeds hash width")
	}
	return int((hash >> shift) & r.mask), nil
}
