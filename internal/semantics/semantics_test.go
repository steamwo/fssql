package semantics

import (
	"math"
	"testing"
)

func TestEqualsImpliesHashInt64(t *testing.T) {
	vals := []int64{math.MinInt64, -1, 0, 1, math.MaxInt64}
	for _, a := range vals {
		for _, b := range vals {
			if EqualInt64(a, b) && HashInt64(a) != HashInt64(b) {
				t.Fatalf("%d %d", a, b)
			}
		}
	}
}
func TestCheckedArithmetic(t *testing.T) {
	if _, err := AddInt64(math.MaxInt64, 1); err == nil {
		t.Fatal("add overflow")
	}
	if _, err := MulInt64(math.MinInt64, -1); err == nil {
		t.Fatal("mul overflow")
	}
	if v, err := MulInt64(7, 9); err != nil || v != 63 {
		t.Fatalf("%d %v", v, err)
	}
}
func TestHashFloatCanonicalZero(t *testing.T) {
	if HashFloat64(0) != HashFloat64(math.Copysign(0, -1)) {
		t.Fatal("zero hash")
	}
}
func FuzzEqualsHashInt64(f *testing.F) {
	for _, v := range []int64{-1, 0, 1, math.MinInt64, math.MaxInt64} {
		f.Add(v, v)
	}
	f.Fuzz(func(t *testing.T, a, b int64) {
		if EqualInt64(a, b) && HashInt64(a) != HashInt64(b) {
			t.Fatalf("equal values hash differently")
		}
	})
}
