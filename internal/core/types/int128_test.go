package types

import "testing"

func TestInt128FromInt64SignExtends(t *testing.T) {
	cases := []struct {
		input int64
		want  Int128
	}{
		{input: 0, want: Int128{}},
		{input: 1, want: Int128{Lo: 1}},
		{input: -1, want: Int128{Lo: ^uint64(0), Hi: -1}},
		{input: -2, want: Int128{Lo: ^uint64(0) - 1, Hi: -1}},
	}
	for _, tc := range cases {
		if got := Int128FromInt64(tc.input); got != tc.want {
			t.Fatalf("Int128FromInt64(%d) = %+v, want %+v", tc.input, got, tc.want)
		}
	}
}

func TestInt128FromBigEndianTwosComplement(t *testing.T) {
	cases := []struct {
		name string
		raw  []byte
		want Int128
	}{
		{name: "zero", raw: []byte{0x00}, want: Int128{}},
		{name: "positive", raw: []byte{0x01, 0x00}, want: Int128{Lo: 256}},
		{name: "negative one", raw: []byte{0xff}, want: Int128{Lo: ^uint64(0), Hi: -1}},
		{name: "negative 256", raw: []byte{0xff, 0x00}, want: Int128{Lo: ^uint64(0) - 255, Hi: -1}},
		{name: "cross limb", raw: []byte{0x01, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00}, want: Int128{Lo: 0, Hi: 1}},
		{name: "max", raw: []byte{0x7f, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff}, want: Int128{Lo: ^uint64(0), Hi: int64(^uint64(0) >> 1)}},
		{name: "min", raw: []byte{0x80, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00}, want: Int128{Lo: 0, Hi: -1 << 63}},
		{name: "redundant positive sign extension", raw: append([]byte{0x00}, []byte{0x7f, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff}...), want: Int128{Lo: ^uint64(0), Hi: int64(^uint64(0) >> 1)}},
		{name: "redundant negative sign extension", raw: append([]byte{0xff}, []byte{0x80, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00}...), want: Int128{Lo: 0, Hi: -1 << 63}},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			got, err := Int128FromBigEndianTwosComplement(tc.raw)
			if err != nil {
				t.Fatalf("decode error: %v", err)
			}
			if got != tc.want {
				t.Fatalf("got %+v, want %+v", got, tc.want)
			}
		})
	}
}

func TestInt128FromBigEndianTwosComplementRejectsOverflow(t *testing.T) {
	if _, err := Int128FromBigEndianTwosComplement(nil); err == nil {
		t.Fatal("empty input should fail")
	}
	positiveOverflow := append([]byte{0x00}, append([]byte{0x80}, make([]byte, 15)...)...)
	if _, err := Int128FromBigEndianTwosComplement(positiveOverflow); err == nil {
		t.Fatal("positive overflow should fail")
	}
	negativeOverflow := append([]byte{0xff}, append([]byte{0x7f}, make([]byte, 15)...)...)
	if _, err := Int128FromBigEndianTwosComplement(negativeOverflow); err == nil {
		t.Fatal("negative overflow should fail")
	}
}
