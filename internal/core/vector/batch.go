package vector

import (
	"errors"

	"github.com/steamwo/fssql/internal/core/memory"
	"github.com/steamwo/fssql/internal/core/types"
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

// WriteFixedWidthSelected scatters native fixed-width values into selected
// output rows while acquiring the destination buffer only once.
func (v *Vector) WriteFixedWidthSelected(payload []byte, positions []int) error {
	width, ok := v.Type.FixedSize()
	if v.Kind != Flat || !ok || width <= 0 || len(payload)%width != 0 {
		return errors.New("selected fixed-width batch requires a flat scalar vector")
	}
	n := len(payload) / width
	if len(positions) != n {
		return errors.New("selected fixed-width batch position mismatch")
	}
	if n == 0 {
		return nil
	}
	out := v.data.Bytes()
	for i, row := range positions {
		if row < 0 || row >= v.count {
			return errors.New("selected fixed-width batch row out of bounds")
		}
		copy(out[row*width:(row+1)*width], payload[i*width:(i+1)*width])
		v.validity.SetValid(row, true)
	}
	return nil
}

// CopyDictionaryIndices materializes dictionary indices into consecutive rows.
func CopyDictionaryIndices(dst *Vector, dstStart int, dict *Vector, indices []uint32) error {
	return copyDictionaryIndices(dst, dstStart, dict, indices, nil)
}

// CopyDictionaryIndicesSelected scatters dictionary values into output rows.
func CopyDictionaryIndicesSelected(dst *Vector, dict *Vector, indices []uint32, positions []int) error {
	return copyDictionaryIndices(dst, 0, dict, indices, positions)
}

func copyDictionaryIndices(dst *Vector, dstStart int, dict *Vector, indices []uint32, positions []int) error {
	if dst == nil || dict == nil {
		return errors.New("nil dictionary vector")
	}
	if dst.Kind != Flat {
		return errors.New("dictionary destination must be flat")
	}
	if !dst.Type.Same(dict.Type) {
		return errors.New("dictionary vector type mismatch")
	}
	n := len(indices)
	if positions != nil && len(positions) != n {
		return errors.New("dictionary position count mismatch")
	}
	width, fixed := dict.Type.FixedSize()
	if fixed && width > 0 && dict.Kind == Flat {
		src, out := dict.data.Bytes(), dst.data.Bytes()
		for i, raw := range indices {
			index := int(raw)
			if index < 0 || index >= dict.count {
				return errors.New("dictionary index out of range")
			}
			row := dstStart + i
			if positions != nil {
				row = positions[i]
			}
			if row < 0 || row >= dst.count {
				return errors.New("dictionary destination row out of bounds")
			}
			if !dict.validity.IsValid(index) {
				dst.SetNull(row)
				continue
			}
			copy(out[row*width:(row+1)*width], src[index*width:(index+1)*width])
			dst.validity.SetValid(row, true)
		}
		return nil
	}
	for i, raw := range indices {
		index := int(raw)
		if index < 0 || index >= dict.count {
			return errors.New("dictionary index out of range")
		}
		row := dstStart + i
		if positions != nil {
			row = positions[i]
		}
		if row < 0 || row >= dst.count {
			return errors.New("dictionary destination row out of bounds")
		}
		if err := CopyValue(dict, index, dst, row); err != nil {
			return err
		}
	}
	return nil
}

// CopySelectionInto gathers source rows into a flat destination starting at
// dstOffset. A negative row writes NULL. The destination keeps its ownership.
func CopySelectionInto(dst *Vector, dstOffset int, source *Vector, rows []int) error {
	if dst == nil || source == nil {
		return errors.New("nil selection vector")
	}
	if dst.Kind != Flat {
		return errors.New("selection destination must be flat")
	}
	if !dst.Type.Same(source.Type) {
		return errors.New("selection vector type mismatch")
	}
	if dstOffset < 0 || len(rows) > dst.count-dstOffset {
		return errors.New("selection destination out of bounds")
	}
	if len(rows) == 0 {
		return nil
	}
	if source.Type.ID == types.Varchar {
		view, _ := source.BorrowBytes()
		writer, err := dst.NewStringBatchWriter()
		if err != nil { return err }; defer writer.Commit()
		for i, row := range rows {
			if row >= source.count { return errors.New("selection row out of bounds") }
			data, valid := view.Value(row)
			if !valid { dst.SetNull(dstOffset+i); continue }
			if err := writer.Append(dstOffset+i, data); err != nil { return err }
		}
		return nil
	}
	if source.Kind == Constant {
		for i, row := range rows {
			if row < 0 {
				dst.SetNull(dstOffset + i)
				continue
			}
			if err := CopyValue(source, 0, dst, dstOffset+i); err != nil {
				return err
			}
		}
		return nil
	}
	width, fixed := source.Type.FixedSize()
	if fixed && width > 0 {
		base := source
		for base.Kind == Dictionary {
			base = base.dictChild
		}
		src, out := base.data.Bytes(), dst.data.Bytes()
		for i, row := range rows {
			if row < 0 {
				dst.SetNull(dstOffset + i)
				continue
			}
			if row >= source.count {
				return errors.New("selection row out of bounds")
			}
			index, resolved := source.resolve(row)
			if !resolved.validity.IsValid(index) {
				dst.SetNull(dstOffset + i)
				continue
			}
			copy(out[(dstOffset+i)*width:(dstOffset+i+1)*width], src[index*width:(index+1)*width])
			dst.validity.SetValid(dstOffset+i, true)
		}
		return nil
	}
	for i, row := range rows {
		if row < 0 {
			dst.SetNull(dstOffset + i)
			continue
		}
		if row >= source.count {
			return errors.New("selection row out of bounds")
		}
		if err := CopyValue(source, row, dst, dstOffset+i); err != nil {
			return err
		}
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
	if source.Type.ID == types.Varchar {
		if err := CopySelectionInto(out, 0, source, rows); err != nil { out.Release(); return nil, err }
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
