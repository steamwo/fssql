package vector

import (
	"errors"

	"github.com/steamwo/fssql/internal/core/memory"
)

// WriteFixedWidth copies already-decoded native little-endian scalar payload
// into an owned flat vector, acquiring its managed buffer only once per batch.
func (v *Vector) WriteFixedWidth(start int, payload []byte) error {
	width, ok := v.Type.FixedSize()
	if v.Kind != Flat || !ok || width <= 0 || len(payload)%width != 0 {
		return errors.New("fixed-width batch requires a flat scalar vector")
	}
	n := len(payload) / width
	if start < 0 || start > v.count || n > v.count-start {
		return errors.New("fixed-width batch out of bounds")
	}
	if n == 0 {
		return nil
	}
	copy(v.data.Bytes()[start*width:(start+n)*width], payload)
	for i := start; i < start+n; i++ {
		v.validity.SetValid(i, true)
	}
	return nil
}

// CopyFixedIndices gathers scalar dictionary values without per-value buffer
// locking. Source and destination retain independent ownership.
func (v *Vector) CopyFixedIndices(start int, source *Vector, indices []uint32) error {
	if source == nil {
		return errors.New("nil dictionary vector")
	}
	w, ok := v.Type.FixedSize()
	if v.Kind != Flat || source.Kind != Flat || !ok || w <= 0 || v.Type.ID != source.Type.ID || v.Type.Decimal != source.Type.Decimal || start < 0 || start > v.count || len(indices) > v.count-start {
		return errors.New("invalid fixed-width dictionary batch")
	}
	if len(indices) == 0 {
		return nil
	}
	if source.count == 0 {
		return errors.New("dictionary index out of range")
	}
	src, dst := source.data.Bytes(), v.data.Bytes()
	for i, index := range indices {
		if uint64(index) >= uint64(source.count) {
			return errors.New("dictionary index out of range")
		}
		row := int(index)
		if !source.validity.IsValid(row) {
			v.SetNull(start + i)
			continue
		}
		copy(dst[(start+i)*w:(start+i+1)*w], src[row*w:(row+1)*w])
		v.validity.SetValid(start+i, true)
	}
	return nil
}

// CopySelection materializes independent owned rows. Constant inputs stay
// constant, particularly unread NULL placeholders from projected scans.
func CopySelection(scope *memory.Scope, bm *memory.BufferManager, source *Vector, rows []int) (*Vector, error) {
	if source == nil {
		return nil, errors.New("nil selection source")
	}
	for _, row := range rows {
		if row < 0 || row >= source.count {
			return nil, errors.New("selection row out of bounds")
		}
	}
	if source.Kind == Constant {
		out, err := NewConstant(scope, bm, source.Type, len(rows))
		if err != nil {
			return nil, err
		}
		if len(rows) > 0 {
			if err := CopyValue(source, rows[0], out, 0); err != nil {
				out.Release()
				return nil, err
			}
		}
		return out, nil
	}
	out, err := NewFlat(scope, bm, source.Type, len(rows))
	if err != nil {
		return nil, err
	}
	if len(rows) == 0 {
		return out, nil
	}
	width, fixed := source.Type.FixedSize()
	if fixed && width > 0 {
		base := source
		for base.Kind == Dictionary {
			base = base.dictChild
		}
		src, dst := base.data.Bytes(), out.data.Bytes()
		for i, row := range rows {
			index, resolved := source.resolve(row)
			if !resolved.validity.IsValid(index) {
				out.SetNull(i)
				continue
			}
			copy(dst[i*width:(i+1)*width], src[index*width:(index+1)*width])
		}
		return out, nil
	}
	for i, row := range rows {
		if err := CopyValue(source, row, out, i); err != nil {
			out.Release()
			return nil, err
		}
	}
	return out, nil
}
