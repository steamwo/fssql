package semantics

import (
	"math"
	"testing"
)

func TestPhase2HashCanonicalization(t *testing.T) {
	if HashFloat32(0) != HashFloat32(float32(math.Copysign(0, -1))) { t.Fatal("float32 signed zero hash mismatch") }
	if HashFloat64(0) != HashFloat64(math.Copysign(0, -1)) { t.Fatal("float64 signed zero hash mismatch") }
	if HashInt128(7, -1) != HashInt128(7, -1) { t.Fatal("int128 hash unstable") }
	if HashNull() != HashNull() { t.Fatal("null hash unstable") }
}
func TestPhase2FloatGroupingAndOrderingSemantics(t *testing.T) {
	nan32a:=float32(math.NaN());nan32b:=float32(math.Float32frombits(0x7fc01234));if !EqualFloat32(nan32a,nan32b)||HashFloat32(nan32a)!=HashFloat32(nan32b){t.Fatal("float32 NaN key semantics inconsistent")};if CompareFloat32(nan32a,1)<=0||CompareFloat32(1,nan32a)>=0||CompareFloat32(nan32a,nan32b)!=0{t.Fatal("float32 NaN ordering mismatch")}
	nan64a:=math.NaN();nan64b:=math.Float64frombits(0x7ff8000000001234);if !EqualFloat64(nan64a,nan64b)||HashFloat64(nan64a)!=HashFloat64(nan64b){t.Fatal("float64 NaN key semantics inconsistent")};if CompareFloat64(nan64a,1)<=0||CompareFloat64(1,nan64a)>=0||CompareFloat64(nan64a,nan64b)!=0{t.Fatal("float64 NaN ordering mismatch")}
}
