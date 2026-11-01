package parquet

import (
	"testing"

	"github.com/steamwo/fssql/internal/core/memory"
	coretypes "github.com/steamwo/fssql/internal/core/types"
	"github.com/steamwo/fssql/internal/core/vector"
)

func TestCopyDictionaryIndexesDecimal128(t *testing.T) {
	decimal, err := coretypes.NewDecimal(20, 4)
	if err != nil {
		t.Fatal(err)
	}
	mm := memory.NewManager(1 << 20)
	scope := mm.NewScope()
	bm := memory.NewBufferManager(nil)

	dict, err := vector.NewFlat(scope, bm, decimal, 3)
	if err != nil {
		t.Fatal(err)
	}
	values := []coretypes.Int128{
		{Lo: 12345678901234567890, Hi: 0},
		coretypes.Int128FromInt64(-42),
		{},
	}
	dict.SetInt128(0, values[0])
	dict.SetInt128(1, values[1])
	dict.SetNull(2)

	dst, err := vector.NewFlat(scope, bm, decimal, 4)
	if err != nil {
		dict.Release()
		t.Fatal(err)
	}
	if err := CopyDictionaryIndexes(dst, 0, dict, []uint32{1, 0, 2, 1}); err != nil {
		dst.Release()
		dict.Release()
		t.Fatal(err)
	}

	want := []struct {
		value coretypes.Int128
		valid bool
	}{
		{value: values[1], valid: true},
		{value: values[0], valid: true},
		{valid: false},
		{value: values[1], valid: true},
	}
	for i, expected := range want {
		got, ok := dst.Int128(i)
		if ok != expected.valid {
			t.Fatalf("row %d valid=%v, want %v", i, ok, expected.valid)
		}
		if ok && got != expected.value {
			t.Fatalf("row %d = %+v, want %+v", i, got, expected.value)
		}
	}

	dst.Release()
	dict.Release()
	if scope.Used() != 0 {
		t.Fatalf("managed memory leaked: %d", scope.Used())
	}
}

func TestCopyDictionaryIndexesDecimal64StillUsesInt64(t *testing.T) {
	decimal, err := coretypes.NewDecimal(18, 2)
	if err != nil {
		t.Fatal(err)
	}
	mm := memory.NewManager(1 << 20)
	scope := mm.NewScope()
	bm := memory.NewBufferManager(nil)
	dict, err := vector.NewFlat(scope, bm, decimal, 2)
	if err != nil {
		t.Fatal(err)
	}
	dict.SetInt64(0, 1234)
	dict.SetInt64(1, -5678)
	dst, err := vector.NewFlat(scope, bm, decimal, 2)
	if err != nil {
		dict.Release()
		t.Fatal(err)
	}
	if err := CopyDictionaryIndexes(dst, 0, dict, []uint32{1, 0}); err != nil {
		dst.Release()
		dict.Release()
		t.Fatal(err)
	}
	if got, ok := dst.Int64(0); !ok || got != -5678 {
		t.Fatalf("row 0 = %d valid=%v", got, ok)
	}
	if got, ok := dst.Int64(1); !ok || got != 1234 {
		t.Fatalf("row 1 = %d valid=%v", got, ok)
	}
	dst.Release()
	dict.Release()
	if scope.Used() != 0 {
		t.Fatalf("managed memory leaked: %d", scope.Used())
	}
}
