package semantics

import (
	"errors"
	"math"
)

var ErrOverflow = errors.New("numeric overflow")

func EqualInt64(a, b int64) bool { return a == b }
func CompareInt64(a, b int64) int {
	if a < b {
		return -1
	}
	if a > b {
		return 1
	}
	return 0
}

func EqualFloat64(a, b float64) bool {
	// SQL comparison treats NaN as not equal; hashing canonicalizes NaNs separately.
	return a == b
}

func HashInt64(v int64) uint64   { return mix64(uint64(v)) }
func HashUint64(v uint64) uint64 { return mix64(v) }
func HashBool(v bool) uint64 {
	if v {
		return mix64(1)
	}
	return mix64(0)
}
func HashFloat64(v float64) uint64 {
	bits := math.Float64bits(v)
	if v == 0 {
		bits = 0
	}
	if math.IsNaN(v) {
		bits = 0x7ff8000000000000
	}
	return mix64(bits)
}
func HashString(s string) uint64 {
	const offset = uint64(14695981039346656037)
	const prime = uint64(1099511628211)
	h := offset
	for i := 0; i < len(s); i++ {
		h = (h ^ uint64(s[i])) * prime
	}
	return mix64(h)
}
func HashBytes(b []byte) uint64 {
	const offset = uint64(14695981039346656037)
	const prime = uint64(1099511628211)
	h := offset
	for _, c := range b {
		h = (h ^ uint64(c)) * prime
	}
	return mix64(h)
}
func mix64(x uint64) uint64 {
	x ^= x >> 30
	x *= 0xbf58476d1ce4e5b9
	x ^= x >> 27
	x *= 0x94d049bb133111eb
	x ^= x >> 31
	return x
}

func AddInt64(a, b int64) (int64, error) {
	c := a + b
	if (b > 0 && c < a) || (b < 0 && c > a) {
		return 0, ErrOverflow
	}
	return c, nil
}
func SubInt64(a, b int64) (int64, error) {
	if b == math.MinInt64 {
		if a >= 0 {
			return 0, ErrOverflow
		}
		return a - b, nil
	}
	return AddInt64(a, -b)
}
func MulInt64(a, b int64) (int64, error) {
	if a == 0 || b == 0 {
		return 0, nil
	}
	if a == math.MinInt64 && b == -1 || b == math.MinInt64 && a == -1 {
		return 0, ErrOverflow
	}
	c := a * b
	if c/b != a {
		return 0, ErrOverflow
	}
	return c, nil
}
