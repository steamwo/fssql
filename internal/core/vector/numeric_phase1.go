package vector

import (
	"encoding/binary"
	"math"
)

// SetInt32 writes a 32-bit integer into INTEGER/DATE-style flat storage.
func (v *Vector) SetInt32(i int, x int32) {
	if v.Kind == Dictionary {
		panic("cannot mutate dictionary vector")
	}
	if v.Kind == Constant {
		i = 0
	}
	binary.LittleEndian.PutUint32(v.data.Bytes()[i*4:i*4+4], uint32(x))
	v.validity.SetValid(i, true)
}

// Int32 reads a 32-bit integer from INTEGER/DATE-style storage.
func (v *Vector) Int32(i int) (int32, bool) {
	idx, b := v.resolve(i)
	if !b.validity.IsValid(idx) {
		return 0, false
	}
	return int32(binary.LittleEndian.Uint32(b.data.Bytes()[idx*4 : idx*4+4])), true
}

// SetFloat32 writes a FLOAT value into 32-bit physical storage.
func (v *Vector) SetFloat32(i int, x float32) {
	if v.Kind == Dictionary {
		panic("cannot mutate dictionary vector")
	}
	if v.Kind == Constant {
		i = 0
	}
	binary.LittleEndian.PutUint32(v.data.Bytes()[i*4:i*4+4], math.Float32bits(x))
	v.validity.SetValid(i, true)
}

// Float32 reads a FLOAT value from 32-bit physical storage.
func (v *Vector) Float32(i int) (float32, bool) {
	idx, b := v.resolve(i)
	if !b.validity.IsValid(idx) {
		return 0, false
	}
	return math.Float32frombits(binary.LittleEndian.Uint32(b.data.Bytes()[idx*4 : idx*4+4])), true
}
