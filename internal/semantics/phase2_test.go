package semantics

import (
	"math"
	"testing"
)

func TestPhase2HashCanonicalization(t *testing.T) {
	if HashFloat32(0) != HashFloat32(float32(math.Copysign(0, -1))) {
		t.Fatal("float32 signed zero hash mismatch")
	}
	if HashFloat64(0) != HashFloat64(math.Copysign(0, -1)) {
		t.Fatal("float64 signed zero hash mismatch")
	}
	if HashInt128(7, -1) != HashInt128(7, -1) {
		t.Fatal("int128 hash unstable")
	}
	if HashNull() != HashNull() {
		t.Fatal("null hash unstable")
	}
}
