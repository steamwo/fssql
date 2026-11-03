package vector

import "encoding/binary"

func (v *Vector) SetUint8(i int, x uint8) {
	if v.Kind == Dictionary {
		panic("cannot mutate dictionary vector")
	}
	if v.Kind == Constant {
		i = 0
	}
	v.data.Bytes()[i] = x
	v.validity.SetValid(i, true)
}

func (v *Vector) Uint8(i int) (uint8, bool) {
	idx, b := v.resolve(i)
	if !b.validity.IsValid(idx) {
		return 0, false
	}
	return b.data.Bytes()[idx], true
}

func (v *Vector) SetUint16(i int, x uint16) {
	if v.Kind == Dictionary {
		panic("cannot mutate dictionary vector")
	}
	if v.Kind == Constant {
		i = 0
	}
	binary.LittleEndian.PutUint16(v.data.Bytes()[i*2:i*2+2], x)
	v.validity.SetValid(i, true)
}

func (v *Vector) Uint16(i int) (uint16, bool) {
	idx, b := v.resolve(i)
	if !b.validity.IsValid(idx) {
		return 0, false
	}
	return binary.LittleEndian.Uint16(b.data.Bytes()[idx*2 : idx*2+2]), true
}

func (v *Vector) SetUint32(i int, x uint32) {
	if v.Kind == Dictionary {
		panic("cannot mutate dictionary vector")
	}
	if v.Kind == Constant {
		i = 0
	}
	binary.LittleEndian.PutUint32(v.data.Bytes()[i*4:i*4+4], x)
	v.validity.SetValid(i, true)
}

func (v *Vector) Uint32(i int) (uint32, bool) {
	idx, b := v.resolve(i)
	if !b.validity.IsValid(idx) {
		return 0, false
	}
	return binary.LittleEndian.Uint32(b.data.Bytes()[idx*4 : idx*4+4]), true
}

func (v *Vector) SetUint64(i int, x uint64) {
	if v.Kind == Dictionary {
		panic("cannot mutate dictionary vector")
	}
	if v.Kind == Constant {
		i = 0
	}
	binary.LittleEndian.PutUint64(v.data.Bytes()[i*8:i*8+8], x)
	v.validity.SetValid(i, true)
}

func (v *Vector) Uint64(i int) (uint64, bool) {
	idx, b := v.resolve(i)
	if !b.validity.IsValid(idx) {
		return 0, false
	}
	return binary.LittleEndian.Uint64(b.data.Bytes()[idx*8 : idx*8+8]), true
}

// SetBytes stores binary data in engine-owned managed string-heap storage.
// BLOB and VARCHAR intentionally share the physical variable-width layout;
// their logical semantics remain distinct in Vector.Type.
func (v *Vector) SetBytes(i int, src []byte) error { return v.SetStringBytes(i, src) }

// Bytes returns a borrowed binary view. It is valid only while the vector is
// alive and must not be retained across chunk release.
func (v *Vector) Bytes(i int) ([]byte, bool) {
	r, h, ok := v.StringRefAt(i)
	if !ok {
		return nil, false
	}
	if r.External {
		return h.ExternalBytes(r), true
	}
	return r.Inline[:r.Length], true
}
