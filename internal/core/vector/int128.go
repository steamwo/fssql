package vector

import (
	"encoding/binary"

	"github.com/steamwo/fssql/internal/core/types"
)

// SetInt128 writes one canonical signed 128-bit value using the ADR-0001
// Own Vector layout: low unsigned 64-bit limb followed by high signed 64-bit
// limb, both little-endian.
func (v *Vector) SetInt128(i int, value types.Int128) {
	if v.Kind == Dictionary {
		panic("cannot mutate dictionary vector")
	}
	if v.Kind == Constant {
		i = 0
	}
	off := i * 16
	binary.LittleEndian.PutUint64(v.data.Bytes()[off:off+8], value.Lo)
	binary.LittleEndian.PutUint64(v.data.Bytes()[off+8:off+16], uint64(value.Hi))
	v.validity.SetValid(i, true)
}

// Int128 reads one canonical signed 128-bit value, resolving Constant and
// Dictionary vector indirection before accessing the flat managed buffer.
func (v *Vector) Int128(i int) (types.Int128, bool) {
	idx, base := v.resolve(i)
	if !base.validity.IsValid(idx) {
		return types.Int128{}, false
	}
	off := idx * 16
	return types.Int128{
		Lo: binary.LittleEndian.Uint64(base.data.Bytes()[off : off+8]),
		Hi: int64(binary.LittleEndian.Uint64(base.data.Bytes()[off+8 : off+16])),
	}, true
}
