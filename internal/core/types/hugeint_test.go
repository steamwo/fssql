package types

import (
	"errors"
	"math"
	"testing"
)

func TestHugeIntPhysicalMapping(t *testing.T) {
	typ := LogicalType{ID: HugeInt}
	if typ.Physical() != PhysicalInt128 {
		t.Fatalf("physical=%v", typ.Physical())
	}
	if n, ok := typ.FixedSize(); !ok || n != 16 {
		t.Fatalf("fixed=%d %v", n, ok)
	}
	if !typ.IsNumeric() {
		t.Fatal("HUGEINT must be numeric")
	}
}
func TestInt128CheckedAddBoundaries(t *testing.T) {
	max := Int128{Lo: math.MaxUint64, Hi: math.MaxInt64}
	if _, err := AddInt128(max, Int128FromInt64(1)); !errors.Is(err, ErrInt128Overflow) {
		t.Fatalf("max+1 err=%v", err)
	}
	min := Int128{Lo: 0, Hi: math.MinInt64}
	if _, err := AddInt128(min, Int128FromInt64(-1)); !errors.Is(err, ErrInt128Overflow) {
		t.Fatalf("min-1 err=%v", err)
	}
	got, err := AddInt128(Int128FromInt64(math.MaxInt64), Int128FromInt64(1))
	if err != nil || got != (Int128{Lo: 1 << 63, Hi: 0}) {
		t.Fatalf("widened add=%+v %v", got, err)
	}
}
func TestInt128OriginalBigEndianDecodeStillWorks(t *testing.T) {
	got, err := Int128FromBigEndianTwosComplement([]byte{0xff, 0xfe})
	if err != nil {
		t.Fatal(err)
	}
	if got != Int128FromInt64(-2) {
		t.Fatalf("got=%+v", got)
	}
}

func TestHugeIntAppendPreservesExistingLogicalTypeIDs(t *testing.T) {
	if Boolean != 1 || TinyInt != 2 || SmallInt != 3 || Integer != 4 || BigInt != 5 || Float != 6 || Double != 7 || Decimal != 8 || Varchar != 9 || DateType != 10 || TimestampType != 11 || List != 12 || Struct != 13 {
		t.Fatalf("ADR-0002 must not renumber existing LogicalType IDs")
	}
	if HugeInt != 14 {
		t.Fatalf("HugeInt id=%d, want appended id 14", HugeInt)
	}
}
