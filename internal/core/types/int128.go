package types

import (
	"encoding/binary"
	"errors"
)

// Int128 is the canonical signed 128-bit core value representation.
//
// Values use two's-complement semantics. Lo contains the low unsigned 64 bits
// and Hi contains the high signed 64 bits. Own Vector stores these limbs as
// low then high, each little-endian, as specified by ADR-0001.
type Int128 struct {
	Lo uint64
	Hi int64
}

// Int128FromInt64 sign-extends a signed 64-bit integer to 128 bits.
func Int128FromInt64(value int64) Int128 {
	hi := int64(0)
	if value < 0 {
		hi = -1
	}
	return Int128{Lo: uint64(value), Hi: hi}
}

// Int128FromBigEndianTwosComplement decodes a big-endian two's-complement
// integer into the canonical Int128 representation. Redundant leading sign
// extension bytes are accepted; values outside signed 128-bit range fail.
func Int128FromBigEndianTwosComplement(src []byte) (Int128, error) {
	if len(src) == 0 {
		return Int128{}, errors.New("int128: empty two's-complement input")
	}

	negative := src[0]&0x80 != 0
	for len(src) > 16 {
		expected := byte(0)
		if negative {
			expected = 0xff
		}
		if src[0] != expected || (src[1]&0x80 != 0) != negative {
			return Int128{}, errors.New("int128: two's-complement overflow")
		}
		src = src[1:]
	}

	var raw [16]byte
	if negative {
		for i := range raw {
			raw[i] = 0xff
		}
	}
	copy(raw[len(raw)-len(src):], src)

	return Int128{
		Lo: binary.BigEndian.Uint64(raw[8:]),
		Hi: int64(binary.BigEndian.Uint64(raw[:8])),
	}, nil
}

// IsNegative reports whether the signed Int128 value is negative.
func (value Int128) IsNegative() bool { return value.Hi < 0 }
