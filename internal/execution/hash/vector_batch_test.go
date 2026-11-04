package hash

import (
	"math"
	"testing"

	"github.com/steamwo/fssql/internal/core/memory"
	"github.com/steamwo/fssql/internal/core/types"
	"github.com/steamwo/fssql/internal/core/vector"
	"github.com/steamwo/fssql/internal/semantics"
)

func TestBatchHashMatchesEncodedRowsAllScalarRepresentations(t *testing.T) {
	cases := []struct {
		typ   types.LogicalType
		write func(*vector.Vector, int)
	}{
		{types.UTinyIntType(), func(v *vector.Vector, i int) { v.SetUint8(i, 255) }},
		{types.USmallIntType(), func(v *vector.Vector, i int) { v.SetUint16(i, 65535) }},
		{types.UIntegerType(), func(v *vector.Vector, i int) { v.SetUint32(i, math.MaxUint32) }},
		{types.UBigIntType(), func(v *vector.Vector, i int) { v.SetUint64(i, math.MaxUint64) }},
		{types.LogicalType{ID: types.BigInt}, func(v *vector.Vector, i int) { v.SetInt64(i, -17) }},
		{types.LogicalType{ID: types.Decimal, Decimal: types.DecimalInfo{Width: 38, Scale: 9}}, func(v *vector.Vector, i int) { v.SetInt128(i, types.Int128{Lo: 23, Hi: 5}) }},
		{types.LogicalType{ID: types.Double}, func(v *vector.Vector, i int) {
			if i == 0 {
				v.SetFloat64(i, math.NaN())
			} else {
				v.SetFloat64(i, math.Copysign(0, -1))
			}
		}},
		{types.BlobType(), func(v *vector.Vector, i int) { _ = v.SetStringBytes(i, []byte{'x', 0, 255}) }},
		{types.LogicalType{ID: types.Varchar}, func(v *vector.Vector, i int) { _ = v.SetString(i, "external string hash payload") }},
		{types.TimestampNSType(), func(v *vector.Vector, i int) { v.SetInt64(i, 1730000000123456789) }},
	}
	for ci, tc := range cases {
		for _, constant := range []bool{false, true} {
			m := memory.NewManager(1 << 20)
			scope := m.NewScope()
			bm := memory.NewBufferManager(nil)
			base, err := vector.NewFlat(scope, bm, tc.typ, 3)
			if err != nil {
				t.Fatal(err)
			}
			if constant {
				base.Release()
				base, err = vector.NewConstant(scope, bm, tc.typ, 3)
				if err != nil {
					t.Fatal(err)
				}
			}
			tc.write(base, 0)
			if !constant {
				tc.write(base, 1)
				base.SetNull(2)
			}
			child, _ := vector.NewDictionary(base, vector.SelectionFrom([]uint32{2, 0, 1}))
			outer, _ := vector.NewDictionary(child, vector.SelectionFrom([]uint32{1, 2, 0}))
			chunk, _ := vector.NewDataChunk(outer)
			layout, err := NewRelocatableRowLayout([]types.LogicalType{tc.typ})
			if err != nil {
				t.Fatal(err)
			}
			got := make([]uint64, 3)
			single := make([]uint64, 3)
			if err := HashChunk(chunk, []int{0}, got); err != nil {
				t.Fatal(err)
			}
			if err := VectorHash(outer, single); err != nil {
				t.Fatal(err)
			}
			for row := 0; row < 3; row++ {
				n, _ := layout.RowSize(chunk, row)
				encoded := make([]byte, n)
				_, _ = layout.WriteRow(chunk, row, encoded)
				want, err := layout.HashRow(encoded)
				if err != nil || got[row] != want || single[row] != want {
					t.Fatalf("case=%d const=%v row=%d hashes=%x/%x want=%x err=%v", ci, constant, row, got[row], single[row], want, err)
				}
			}
			outer.Release()
			child.Release()
			base.Release()
			if err := scope.Close(); err != nil {
				t.Fatal(err)
			}
		}
	}
}

func TestBatchHashUnsupportedPayloadKeepsNullFallback(t *testing.T) {
	m := memory.NewManager(1 << 20)
	scope := m.NewScope()
	bm := memory.NewBufferManager(nil)
	v, err := vector.NewFlat(scope, bm, types.NewList(types.LogicalType{ID: types.BigInt}), 2)
	if err != nil {
		t.Fatal(err)
	}
	v.SetNull(0)
	v.SetNull(1)
	out := make([]uint64, 2)
	if err := VectorHash(v, out); err != nil || out[0] != semantics.HashNull() || out[1] != semantics.HashNull() {
		t.Fatalf("hashes=%v err=%v", out, err)
	}
	v.SetValid(1)
	if err := VectorHash(v, out); err == nil {
		t.Fatal("unsupported non-null payload accepted")
	}
	v.Release()
	if err := scope.Close(); err != nil {
		t.Fatal(err)
	}
}
