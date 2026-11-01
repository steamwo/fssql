package vector

import (
	"testing"

	"github.com/steamwo/fssql/internal/core/memory"
	"github.com/steamwo/fssql/internal/core/types"
)

func TestSerializeChunkDecimal64AndDecimal128RoundTrip(t *testing.T) {
	decimal64, err := types.NewDecimal(18, 2)
	if err != nil {
		t.Fatal(err)
	}
	decimal128, err := types.NewDecimal(38, 7)
	if err != nil {
		t.Fatal(err)
	}
	mm := memory.NewManager(1 << 20)
	scope := mm.NewScope()
	bm := memory.NewBufferManager(nil)

	v64, err := NewFlat(scope, bm, decimal64, 3)
	if err != nil {
		t.Fatal(err)
	}
	v128, err := NewFlat(scope, bm, decimal128, 3)
	if err != nil {
		v64.Release()
		t.Fatal(err)
	}
	v64.SetInt64(0, 123456)
	v64.SetInt64(1, -42)
	v64.SetNull(2)
	want128 := []types.Int128{
		{Lo: 0x0123456789abcdef, Hi: 0x0011223344556677},
		types.Int128FromInt64(-99),
		{},
	}
	v128.SetInt128(0, want128[0])
	v128.SetInt128(1, want128[1])
	v128.SetNull(2)

	chunk, err := NewDataChunk(v64, v128)
	if err != nil {
		v64.Release()
		v128.Release()
		t.Fatal(err)
	}
	encoded, err := SerializeChunk(chunk, scope, bm)
	if err != nil {
		chunk.Release()
		t.Fatal(err)
	}
	decoded, err := DeserializeChunk(encoded.Bytes(), scope, bm)
	if err != nil {
		encoded.Release()
		chunk.Release()
		t.Fatal(err)
	}

	if got := decoded.Columns[0].Type; got.ID != types.Decimal || got.Decimal != decimal64.Decimal {
		t.Fatalf("Decimal64 type = %+v, want %+v", got, decimal64)
	}
	if got := decoded.Columns[1].Type; got.ID != types.Decimal || got.Decimal != decimal128.Decimal {
		t.Fatalf("Decimal128 type = %+v, want %+v", got, decimal128)
	}
	if got, ok := decoded.Columns[0].Int64(0); !ok || got != 123456 {
		t.Fatalf("Decimal64[0] = %d valid=%v", got, ok)
	}
	if got, ok := decoded.Columns[0].Int64(1); !ok || got != -42 {
		t.Fatalf("Decimal64[1] = %d valid=%v", got, ok)
	}
	if _, ok := decoded.Columns[0].Int64(2); ok {
		t.Fatal("Decimal64 null should remain null")
	}
	for i := 0; i < 2; i++ {
		got, ok := decoded.Columns[1].Int128(i)
		if !ok || got != want128[i] {
			t.Fatalf("Decimal128[%d] = %+v valid=%v, want %+v", i, got, ok, want128[i])
		}
	}
	if _, ok := decoded.Columns[1].Int128(2); ok {
		t.Fatal("Decimal128 null should remain null")
	}

	decoded.Release()
	encoded.Release()
	chunk.Release()
	if scope.Used() != 0 {
		t.Fatalf("managed memory leaked: %d", scope.Used())
	}
}
