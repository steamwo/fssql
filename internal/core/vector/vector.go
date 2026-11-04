package vector

import (
	"encoding/binary"
	"errors"
	"fmt"
	"math"

	"github.com/steamwo/fssql/internal/core/memory"
	"github.com/steamwo/fssql/internal/core/types"
)

const DefaultVectorSize = 2048

type VectorType uint8

const (
	Flat VectorType = iota
	Constant
	Dictionary
)

type ValidityMask struct {
	valid []uint64
	count int
}

func NewValidityMask(count int) ValidityMask {
	words := (count + 63) / 64
	m := ValidityMask{valid: make([]uint64, words), count: count}
	for i := range m.valid {
		m.valid[i] = ^uint64(0)
	}
	if count%64 != 0 && words > 0 {
		m.valid[words-1] = (uint64(1) << uint(count%64)) - 1
	}
	return m
}
func (m *ValidityMask) IsValid(i int) bool {
	return i >= 0 && i < m.count && (m.valid[i>>6]&(uint64(1)<<uint(i&63))) != 0
}
func (m *ValidityMask) SetValid(i int, valid bool) {
	bit := uint64(1) << uint(i&63)
	if valid {
		m.valid[i>>6] |= bit
	} else {
		m.valid[i>>6] &= ^bit
	}
}
func (m *ValidityMask) Count() int { return m.count }

type SelectionVector struct{ indices []uint32 }

func NewSelectionVector(count int) SelectionVector {
	s := SelectionVector{indices: make([]uint32, count)}
	for i := 0; i < count; i++ {
		s.indices[i] = uint32(i)
	}
	return s
}
func SelectionFrom(indices []uint32) SelectionVector {
	return SelectionVector{indices: append([]uint32(nil), indices...)}
}
func (s SelectionVector) Len() int         { return len(s.indices) }
func (s SelectionVector) Get(i int) int    { return int(s.indices[i]) }
func (s SelectionVector) Set(i, value int) { s.indices[i] = uint32(value) }

type StringRef struct {
	Length   uint32
	Inline   [12]byte
	Offset   uint64
	External bool
}

type StringHeap struct {
	bm    *memory.BufferManager
	scope *memory.Scope
	buf   *memory.Buffer
	used  int
}

func NewStringHeap(scope *memory.Scope, bm *memory.BufferManager, initial int) (*StringHeap, error) {
	if initial < 1 {
		initial = 64
	}
	b, err := bm.Allocate(scope, memory.TagStringHeap, initial)
	if err != nil {
		return nil, err
	}
	return &StringHeap{bm: bm, scope: scope, buf: b}, nil
}
func (h *StringHeap) Put(s string) (StringRef, error) {
	var r StringRef
	r.Length = uint32(len(s))
	if len(s) <= len(r.Inline) {
		copy(r.Inline[:], s)
		return r, nil
	}
	need := h.used + len(s)
	if need > h.buf.Len() {
		newCap := h.buf.Len() * 2
		if newCap < need {
			newCap = need
		}
		if err := h.buf.Resize(newCap, nil); err != nil {
			return StringRef{}, err
		}
	}
	copy(h.buf.Bytes()[h.used:need], s)
	r.Offset = uint64(h.used)
	r.External = true
	h.used = need
	return r, nil
}
func (h *StringHeap) PutBytes(src []byte) (StringRef, error) {
	var r StringRef
	r.Length = uint32(len(src))
	if len(src) <= len(r.Inline) {
		copy(r.Inline[:], src)
		return r, nil
	}
	need := h.used + len(src)
	if need > h.buf.Len() {
		newCap := h.buf.Len() * 2
		if newCap < need {
			newCap = need
		}
		if err := h.buf.Resize(newCap, nil); err != nil {
			return StringRef{}, err
		}
	}
	copy(h.buf.Bytes()[h.used:need], src)
	r.Offset = uint64(h.used)
	r.External = true
	h.used = need
	return r, nil
}
func (h *StringHeap) ExternalBytes(r StringRef) []byte {
	if !r.External {
		return nil
	}
	return h.buf.Bytes()[int(r.Offset) : int(r.Offset)+int(r.Length)]
}
func (h *StringHeap) Get(r StringRef) string {
	if !r.External {
		return string(r.Inline[:r.Length])
	}
	return string(h.buf.Bytes()[int(r.Offset) : int(r.Offset)+int(r.Length)])
}
func (h *StringHeap) Used() int { return h.used }
func (h *StringHeap) Release() {
	if h.buf != nil {
		h.buf.Release()
		h.buf = nil
	}
}

type ListEntry struct {
	Offset uint32
	Length uint32
}

type Vector struct {
	Type        types.LogicalType
	Kind        VectorType
	count       int
	data        *memory.Buffer
	validity    ValidityMask
	heap        *StringHeap
	dictChild   *Vector
	dictSel     SelectionVector
	dictOwned   bool
	dictRes     *memory.Reservation
	children    []*Vector
	listEntries []ListEntry
}

func NewFlat(scope *memory.Scope, bm *memory.BufferManager, typ types.LogicalType, count int) (*Vector, error) {
	if count < 0 {
		return nil, errors.New("negative vector size")
	}
	v := &Vector{Type: typ, Kind: Flat, count: count, validity: NewValidityMask(count)}
	sz, ok := typ.FixedSize()
	switch typ.ID {
	case types.Varchar:
		sz = 24
		ok = true
	case types.List:
		v.listEntries = make([]ListEntry, count)
		return v, nil
	case types.Struct:
		return v, nil
	}
	if ok && count > 0 {
		b, err := bm.Allocate(scope, memory.TagVector, count*sz)
		if err != nil {
			return nil, err
		}
		v.data = b
	}
	if typ.ID == types.Varchar {
		h, err := NewStringHeap(scope, bm, 64)
		if err != nil {
			v.Release()
			return nil, err
		}
		v.heap = h
	}
	return v, nil
}
func NewConstant(scope *memory.Scope, bm *memory.BufferManager, typ types.LogicalType, count int) (*Vector, error) {
	v, err := NewFlat(scope, bm, typ, 1)
	if err != nil {
		return nil, err
	}
	v.Kind = Constant
	v.count = count
	v.validity = NewValidityMask(1)
	return v, nil
}
func NewDictionary(child *Vector, sel SelectionVector) (*Vector, error) {
	for i := 0; i < sel.Len(); i++ {
		if sel.Get(i) < 0 || sel.Get(i) >= child.count {
			return nil, fmt.Errorf("selection index %d out of range", sel.Get(i))
		}
	}
	return &Vector{Type: child.Type, Kind: Dictionary, count: sel.Len(), dictChild: child, dictSel: sel, validity: NewValidityMask(sel.Len())}, nil
}
func (v *Vector) Count() int         { return v.count }
func (v *Vector) IsValid(i int) bool { idx, base := v.resolve(i); return base.validity.IsValid(idx) }
func (v *Vector) SetNull(i int) {
	if v.Kind == Dictionary {
		panic("cannot mutate dictionary validity")
	}
	idx := i
	if v.Kind == Constant {
		idx = 0
	}
	v.validity.SetValid(idx, false)
}
func (v *Vector) SetValid(i int) {
	if v.Kind == Dictionary {
		panic("cannot mutate dictionary validity")
	}
	idx := i
	if v.Kind == Constant {
		idx = 0
	}
	v.validity.SetValid(idx, true)
}
func (v *Vector) resolve(i int) (int, *Vector) {
	switch v.Kind {
	case Constant:
		return 0, v
	case Dictionary:
		return v.dictChild.resolve(v.dictSel.Get(i))
	default:
		return i, v
	}
}
func (v *Vector) SetInt64(i int, x int64) {
	if v.Kind == Constant {
		i = 0
	}
	binary.LittleEndian.PutUint64(v.data.Bytes()[i*8:i*8+8], uint64(x))
	v.validity.SetValid(i, true)
}
func (v *Vector) Int64(i int) (int64, bool) {
	idx, b := v.resolve(i)
	if !b.validity.IsValid(idx) {
		return 0, false
	}
	return int64(binary.LittleEndian.Uint64(b.data.Bytes()[idx*8 : idx*8+8])), true
}
func (v *Vector) SetFloat64(i int, x float64) {
	if v.Kind == Constant {
		i = 0
	}
	binary.LittleEndian.PutUint64(v.data.Bytes()[i*8:i*8+8], math.Float64bits(x))
	v.validity.SetValid(i, true)
}
func (v *Vector) Float64(i int) (float64, bool) {
	idx, b := v.resolve(i)
	if !b.validity.IsValid(idx) {
		return 0, false
	}
	return math.Float64frombits(binary.LittleEndian.Uint64(b.data.Bytes()[idx*8 : idx*8+8])), true
}
func (v *Vector) SetBool(i int, x bool) {
	if v.Kind == Constant {
		i = 0
	}
	if x {
		v.data.Bytes()[i] = 1
	} else {
		v.data.Bytes()[i] = 0
	}
	v.validity.SetValid(i, true)
}
func (v *Vector) Bool(i int) (bool, bool) {
	idx, b := v.resolve(i)
	if !b.validity.IsValid(idx) {
		return false, false
	}
	return b.data.Bytes()[idx] != 0, true
}
func (v *Vector) SetString(i int, s string) error {
	return v.setStringRef(i, func() (StringRef, error) { return v.heap.Put(s) })
}
func (v *Vector) SetStringBytes(i int, src []byte) error {
	return v.setStringRef(i, func() (StringRef, error) { return v.heap.PutBytes(src) })
}
func (v *Vector) setStringRef(i int, put func() (StringRef, error)) error {
	if v.Kind == Constant {
		i = 0
	}
	r, err := put()
	if err != nil {
		return err
	}
	encodeStringRef(v.data.Bytes()[i*24:i*24+24], r)
	v.validity.SetValid(i, true)
	return nil
}
func (v *Vector) StringRefAt(i int) (StringRef, *StringHeap, bool) {
	idx, b := v.resolve(i)
	if !b.validity.IsValid(idx) {
		return StringRef{}, nil, false
	}
	r := decodeStringRef(b.data.Bytes()[idx*24 : idx*24+24])
	return r, b.heap, true
}
func (v *Vector) String(i int) (string, bool) {
	r, h, ok := v.StringRefAt(i)
	if !ok {
		return "", false
	}
	return h.Get(r), true
}
func encodeStringRef(dst []byte, r StringRef) {
	binary.LittleEndian.PutUint32(dst[:4], r.Length)
	dst[16] = 0
	if r.External {
		dst[16] = 1
		binary.LittleEndian.PutUint64(dst[8:16], r.Offset)
	} else {
		copy(dst[4:16], r.Inline[:])
	}
}
func decodeStringRef(src []byte) StringRef {
	r := StringRef{Length: binary.LittleEndian.Uint32(src[:4])}
	if src[16] == 1 {
		r.External = true
		r.Offset = binary.LittleEndian.Uint64(src[8:16])
	} else {
		copy(r.Inline[:], src[4:16])
	}
	return r
}
func (v *Vector) SetChildren(children ...*Vector) { v.children = append([]*Vector(nil), children...) }
func (v *Vector) Children() []*Vector             { return v.children }
func (v *Vector) SetListEntry(i int, e ListEntry) { v.listEntries[i] = e }
func (v *Vector) ListEntry(i int) (ListEntry, bool) {
	idx, b := v.resolve(i)
	if !b.validity.IsValid(idx) {
		return ListEntry{}, false
	}
	return b.listEntries[idx], true
}
func (v *Vector) Release() {
	if v == nil {
		return
	}
	if v.dictOwned && v.dictChild != nil {
		v.dictChild.Release()
		v.dictChild = nil
	}
	if v.dictRes != nil {
		v.dictRes.Release()
		v.dictRes = nil
	}
	if v.data != nil {
		v.data.Release()
		v.data = nil
	}
	if v.heap != nil {
		v.heap.Release()
		v.heap = nil
	}
	for _, c := range v.children {
		c.Release()
	}
}

type UnifiedVectorFormat struct {
	Vector    *Vector
	Selection SelectionVector
	Validity  *ValidityMask
}

func (v *Vector) Unified() UnifiedVectorFormat {
	s := NewSelectionVector(v.count)
	base := v
	for i := 0; i < v.count; i++ {
		idx, b := v.resolve(i)
		s.Set(i, idx)
		base = b
	}
	return UnifiedVectorFormat{Vector: base, Selection: s, Validity: &base.validity}
}

type DataChunk struct {
	Columns     []*Vector
	Cardinality int
}

func NewDataChunk(cols ...*Vector) (*DataChunk, error) {
	c := 0
	if len(cols) > 0 {
		c = cols[0].Count()
	}
	for _, v := range cols {
		if v.Count() != c {
			return nil, errors.New("column cardinality mismatch")
		}
	}
	if c > DefaultVectorSize {
		return nil, fmt.Errorf("chunk cardinality %d exceeds %d", c, DefaultVectorSize)
	}
	return &DataChunk{Columns: append([]*Vector(nil), cols...), Cardinality: c}, nil
}
func (c *DataChunk) Release() {
	for _, v := range c.Columns {
		v.Release()
	}
	c.Columns = nil
	c.Cardinality = 0
}
