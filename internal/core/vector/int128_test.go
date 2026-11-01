package vector

import (
	"encoding/binary"
	"testing"

	"github.com/steamwo/fssql/internal/core/memory"
	"github.com/steamwo/fssql/internal/core/types"
)

func TestInt128VectorLayoutAndDictionaryRead(t *testing.T) {
	decimal, err := types.NewDecimal(38, 4)
	if err != nil {
		t.Fatal(err)
	}
	mm := memory.NewManager(1 << 20)
	scope := mm.NewScope()
	bm := memory.NewBufferManager(nil)

	base, err := NewFlat(scope, bm, decimal, 3)
	if err != nil {
		t.Fatal(err)
	}
	values := []types.Int128{
		{Lo: 0x0123456789abcdef, Hi: 0x1020304050607080},
		types.Int128FromInt64(-1),
		{Lo: 7, Hi: -2},
	}
	for i, value := range values {
		base.SetInt128(i, value)
	}

	raw := base.data.Bytes()
	if got := binary.LittleEndian.Uint64(raw[:8]); got != values[0].Lo {
		t.Fatalf("low limb layout = %#x, want %#x", got, values[0].Lo)
	}
	if got := int64(binary.LittleEndian.Uint64(raw[8:16])); got != values[0].Hi {
		t.Fatalf("high limb layout = %#x, want %#x", got, values[0].Hi)
	}

	for i, want := range values {
		got, ok := base.Int128(i)
		if !ok || got != want {
			t.Fatalf("base[%d] = %+v valid=%v, want %+v", i, got, ok, want)
		}
	}

	base.SetNull(2)
	if _, ok := base.Int128(2); ok {
		t.Fatal("null Int128 should report invalid")
	}

	dict, err := NewDictionary(base, SelectionFrom([]uint32{1, 0, 2}))
	if err != nil {
		t.Fatal(err)
	}
	if got, ok := dict.Int128(0); !ok || got != values[1] {
		t.Fatalf("dictionary[0] = %+v valid=%v, want %+v", got, ok, values[1])
	}
	if got, ok := dict.Int128(1); !ok || got != values[0] {
		t.Fatalf("dictionary[1] = %+v valid=%v, want %+v", got, ok, values[0])
	}
	if _, ok := dict.Int128(2); ok {
		t.Fatal("dictionary should preserve source null")
	}

	dict.Release()
	base.Release()
	if scope.Used() != 0 {
		t.Fatalf("managed memory leaked: %d", scope.Used())
	}
}

func TestInt128ConstantVector(t *testing.T) {
	decimal, err := types.NewDecimal(20, 2)
	if err != nil {
		t.Fatal(err)
	}
	mm := memory.NewManager(1 << 20)
	scope := mm.NewScope()
	bm := memory.NewBufferManager(nil)
	v, err := NewConstant(scope, bm, decimal, 4)
	if err != nil {
		t.Fatal(err)
	}
	want := types.Int128{Lo: 42, Hi: 1}
	v.SetInt128(3, want)
	for i := 0; i < v.Count(); i++ {
		got, ok := v.Int128(i)
		if !ok || got != want {
			t.Fatalf("constant[%d] = %+v valid=%v, want %+v", i, got, ok, want)
		}
	}
	v.Release()
	if scope.Used() != 0 {
		t.Fatalf("managed memory leaked: %d", scope.Used())
	}
}
