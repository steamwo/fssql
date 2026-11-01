package types

import (
	"encoding/binary"
	"errors"
	"math"
)

var ErrInt128Overflow = errors.New("int128 overflow")

// Int128 is the canonical signed 128-bit core value representation.
// Values use two's-complement semantics. Lo contains the low unsigned 64 bits
// and Hi contains the high signed 64 bits. Own Vector stores low then high,
// each little-endian, as specified by ADR-0001 and reused by ADR-0002 HUGEINT.
type Int128 struct {
	Lo uint64
	Hi int64
}

func Int128FromInt64(value int64) Int128 {
	hi := int64(0)
	if value < 0 {
		hi = -1
	}
	return Int128{Lo: uint64(value), Hi: hi}
}

// Int128FromBigEndianTwosComplement decodes a big-endian two's-complement
// integer into the canonical Int128 representation. Redundant sign extension
// is accepted; values outside signed 128-bit range fail.
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
	return Int128{Lo: binary.BigEndian.Uint64(raw[8:]), Hi: int64(binary.BigEndian.Uint64(raw[:8]))}, nil
}
func (v Int128) IsNegative() bool { return v.Hi < 0 }
func (v Int128) IsZero() bool     { return v.Lo == 0 && v.Hi == 0 }
func CompareInt128(a, b Int128) int {
	if a.Hi < b.Hi {
		return -1
	}
	if a.Hi > b.Hi {
		return 1
	}
	if a.Lo < b.Lo {
		return -1
	}
	if a.Lo > b.Lo {
		return 1
	}
	return 0
}
func AddInt128(a, b Int128) (Int128, error) {
	lo := a.Lo + b.Lo
	carry := uint64(0)
	if lo < a.Lo {
		carry = 1
	}
	out := Int128{Lo: lo, Hi: int64(uint64(a.Hi) + uint64(b.Hi) + carry)}
	if a.Hi >= 0 && b.Hi >= 0 && out.Hi < 0 {
		return Int128{}, ErrInt128Overflow
	}
	if a.Hi < 0 && b.Hi < 0 && out.Hi >= 0 {
		return Int128{}, ErrInt128Overflow
	}
	return out, nil
}
func NegInt128(v Int128) (Int128, error) {
	if v.Hi == math.MinInt64 && v.Lo == 0 {
		return Int128{}, ErrInt128Overflow
	}
	lo := ^v.Lo + 1
	hi := ^uint64(v.Hi)
	if lo == 0 {
		hi++
	}
	return Int128{Lo: lo, Hi: int64(hi)}, nil
}
func Int128ToFloat64(v Int128) float64 {
	if !v.IsNegative() {
		return math.Ldexp(float64(uint64(v.Hi)), 64) + float64(v.Lo)
	}
	lo := ^v.Lo + 1
	hi := ^uint64(v.Hi)
	if lo == 0 {
		hi++
	}
	return -(math.Ldexp(float64(hi), 64) + float64(lo))
}
