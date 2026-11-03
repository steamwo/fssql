package vector

// FixedWidthView borrows scalar payload for a batch without repeated managed
// buffer acquisition. It does not own storage. The vector and its dictionary
// children must remain alive and immutable until the last use of the view.
type FixedWidthView struct {
	data     []byte
	source   *Vector
	validity *ValidityMask
	width    int
}

func (v *Vector) BorrowFixedWidth() (FixedWidthView, bool) {
	width, fixed := v.Type.FixedSize()
	if !fixed || width <= 0 {
		return FixedWidthView{}, false
	}
	base := v
	for base.Kind == Dictionary {
		base = base.dictChild
	}
	view := FixedWidthView{source: v, validity: &base.validity, width: width}
	if base.data != nil {
		view.data = base.data.Bytes()
	}
	return view, true
}

// Value returns native little-endian bytes and SQL validity. The returned bytes
// are read-only and share the lifetime of the view. Out-of-range rows are NULL.
func (v FixedWidthView) Value(row int) ([]byte, bool) {
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
	return v.data[index*v.width : (index+1)*v.width], true
}
