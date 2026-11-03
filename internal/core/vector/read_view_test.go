package vector

import (
	"encoding/binary"
	"testing"

	"github.com/steamwo/fssql/internal/core/memory"
	"github.com/steamwo/fssql/internal/core/types"
)

func TestBorrowFixedWidthNestedDictionaryAndConstant(t *testing.T) {
	m := memory.NewManager(1 << 20)
	scope := m.NewScope()
	bm := memory.NewBufferManager(nil)
	for _, constant := range []bool{false, true} {
		base, err := NewFlat(scope, bm, types.LogicalType{ID: types.BigInt}, 3)
		if err != nil {
			t.Fatal(err)
		}
		if constant {
			base.Release()
			base, err = NewConstant(scope, bm, types.LogicalType{ID: types.BigInt}, 3)
			if err != nil {
				t.Fatal(err)
			}
		}
		base.SetInt64(0, 7)
		if !constant {
			base.SetInt64(1, 13)
			base.SetNull(2)
		}
		child, err := NewDictionary(base, SelectionFrom([]uint32{2, 1, 0}))
		if err != nil {
			t.Fatal(err)
		}
		outer, err := NewDictionary(child, SelectionFrom([]uint32{1, 0, 2, 1}))
		if err != nil {
			t.Fatal(err)
		}
		view, ok := outer.BorrowFixedWidth()
		if !ok {
			t.Fatal("fixed view unavailable")
		}
		for row := 0; row < outer.Count(); row++ {
			data, valid := view.Value(row)
			want, wantValid := outer.Int64(row)
			if valid != wantValid || valid && int64(binary.LittleEndian.Uint64(data)) != want {
				t.Fatalf("row=%d valid=%v want=%d/%v", row, valid, want, wantValid)
			}
		}
		if _, valid := view.Value(-1); valid {
			t.Fatal("negative row accepted")
		}
		if _, valid := view.Value(outer.Count()); valid {
			t.Fatal("out of range row accepted")
		}
		outer.Release()
		child.Release()
		base.Release()
	}
	if m.Used() != 0 {
		t.Fatalf("managed leak=%d", m.Used())
	}
}
