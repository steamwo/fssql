package vector

import "encoding/binary"

// SetInt8 writes an 8-bit integer into TINYINT physical storage.
func (v *Vector) SetInt8(i int, x int8) {
	if v.Kind == Dictionary {
		panic("cannot mutate dictionary vector")
	}
	if v.Kind == Constant {
		i = 0
	}
	v.data.Bytes()[i] = byte(x)
	v.validity.SetValid(i, true)
}

func (v *Vector) Int8(i int) (int8, bool) {
	idx, b := v.resolve(i)
	if !b.validity.IsValid(idx) {
		return 0, false
	}
	return int8(b.data.Bytes()[idx]), true
}

func (v *Vector) SetInt16(i int, x int16) {
	if v.Kind == Dictionary {
		panic("cannot mutate dictionary vector")
	}
	if v.Kind == Constant {
		i = 0
	}
	binary.LittleEndian.PutUint16(v.data.Bytes()[i*2:i*2+2], uint16(x))
	v.validity.SetValid(i, true)
}

func (v *Vector) Int16(i int) (int16, bool) {
	idx, b := v.resolve(i)
	if !b.validity.IsValid(idx) {
		return 0, false
	}
	return int16(binary.LittleEndian.Uint16(b.data.Bytes()[idx*2 : idx*2+2])), true
}
