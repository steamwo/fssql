package vector

import (
	"encoding/binary"
	"errors"

	"github.com/steamwo/fssql/internal/core/memory"
	"github.com/steamwo/fssql/internal/core/types"
)

// BytesView borrows VARCHAR/BLOB references and heap once for an immutable
// batch. It supports nested dictionaries and constants without materializing.
type BytesView struct {
	source     *Vector
	data, heap []byte
	validity   *ValidityMask
}

// AllValid checks a logical prefix once, outside tight scalar kernels.
func (m *ValidityMask) AllValid(count int) bool {
	if count < 0 || count > m.count {
		return false
	}
	words := count / 64
	for i := 0; i < words; i++ {
		if m.valid[i] != ^uint64(0) {
			return false
		}
	}
	if bits := count % 64; bits != 0 {
		return m.valid[words]&((uint64(1)<<uint(bits))-1) == (uint64(1)<<uint(bits))-1
	}
	return true
}

func (v *Vector) BorrowBytes() (BytesView, bool) {
	if v == nil || v.Type.ID != types.Varchar {
		return BytesView{}, false
	}
	base := v
	for base.Kind == Dictionary {
		base = base.dictChild
	}
	view := BytesView{source: v, validity: &base.validity}
	if base.data != nil {
		view.data = base.data.Bytes()
	}
	if base.heap != nil && base.heap.buf != nil {
		view.heap = base.heap.buf.Bytes()
	}
	return view, true
}

func (v BytesView) Value(row int) ([]byte, bool) {
	if v.source == nil || row < 0 || row >= v.source.count {
		return nil, false
	}
	index := row
	if v.source.Kind != Flat {
		index, _ = v.source.resolve(row)
	}
	if !v.validity.IsValid(index) {
		return nil, false
	}
	ref := v.data[index*24 : (index+1)*24]
	n := int(binary.LittleEndian.Uint32(ref[:4]))
	if ref[16] == 0 {
		return ref[4 : 4+n], true
	}
	off := int(binary.LittleEndian.Uint64(ref[8:16]))
	return v.heap[off : off+n], true
}

// MutableFixedWidth is an exclusive flat-vector batch write boundary. Callers
// must not resize/release the vector while using the returned slices.
func (v *Vector) MutableFixedWidth() ([]byte, *ValidityMask, bool) {
	_, fixed := v.Type.FixedSize()
	if v.Kind != Flat || !fixed {
		return nil, nil, false
	}
	var data []byte
	if v.data != nil {
		data = v.data.Bytes()
	}
	return data, &v.validity, true
}

// Dictionary exposes a read-only dictionary child and immutable selection.
// Their lifetime is bounded by this vector (and the owner of a borrowed child).
func (v *Vector) Dictionary() (*Vector, SelectionVector, bool) {
	if v == nil || v.Kind != Dictionary {
		return nil, SelectionVector{}, false
	}
	return v.dictChild, v.dictSel, true
}

// NewOwnedDictionary transfers child ownership only on success. Its selection
// metadata is accounted even though the bounded index slice lives on Go heap.
func NewOwnedDictionary(scope *memory.Scope, child *Vector, sel SelectionVector) (*Vector, error) {
	if scope == nil || child == nil {
		return nil, errors.New("owned dictionary requires scope and child")
	}
	v, err := NewDictionary(child, sel)
	if err != nil {
		return nil, err
	}
	res, err := scope.Reserve(memory.TagMetadata, int64(sel.Len())*4)
	if err != nil {
		return nil, err
	}
	v.dictOwned, v.dictRes = true, res
	return v, nil
}

// DisownDictionary rolls back a prepared ownership transfer before commit.
func (v *Vector) DisownDictionary() { v.dictOwned = false }

// SelectionFromRows converts a bounded row selection without an intermediate
// uint32 slice and a second copy. Negative rows are rejected by NewDictionary.
func SelectionFromRows(rows []int) SelectionVector {
	indices := make([]uint32, len(rows))
	for i, row := range rows {
		indices[i] = uint32(row)
	}
	return SelectionVector{indices: indices}
}

// SetConstantCount changes only logical cardinality under exclusive ownership.
func (v *Vector) SetConstantCount(count int) error {
	if v == nil || v.Kind != Constant || count < 0 {
		return errors.New("invalid constant cardinality")
	}
	v.count = count
	return nil
}
