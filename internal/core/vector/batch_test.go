package vector

import (
	"encoding/binary"
	"testing"

	"github.com/steamwo/fssql/internal/core/memory"
	"github.com/steamwo/fssql/internal/core/types"
)

func TestBatchSelectionOwnsStorageAndKeepsNullConstants(t *testing.T) {
	m := memory.NewManager(1 << 20)
	scope := m.NewScope()
	bm := memory.NewBufferManager(nil)
	v, err := NewFlat(scope, bm, types.LogicalType{ID: types.BigInt}, 4)
	if err != nil {
		t.Fatal(err)
	}
	for i := 0; i < 4; i++ {
		v.SetInt64(i, int64(10+i))
	}
	v.SetNull(2)
	dict, err := NewDictionary(v, SelectionFrom([]uint32{3, 2, 0}))
	if err != nil {
		t.Fatal(err)
	}
	out, err := CopySelection(scope, bm, dict, []int{0, 1, 2, 0})
	if err != nil {
		t.Fatal(err)
	}
	dict.Release()
	v.Release()
	for i, want := range []int64{13, 0, 10, 13} {
		got, valid := out.Int64(i)
		if valid != (i != 1) || (valid && got != want) {
			t.Fatalf("row=%d got=%d valid=%v", i, got, valid)
		}
	}
	out.Release()
	null, err := NewConstant(scope, bm, types.LogicalType{ID: types.BigInt}, 4)
	if err != nil {
		t.Fatal(err)
	}
	null.SetNull(0)
	out, err = CopySelection(scope, bm, null, []int{3, 0})
	if err != nil {
		t.Fatal(err)
	}
	null.Release()
	if out.Kind != Constant || out.Count() != 2 || out.IsValid(1) {
		t.Fatal("NULL constant materialized incorrectly")
	}
	out.Release()
	if m.Used() != 0 {
		t.Fatalf("managed leak=%d", m.Used())
	}
}

func TestFixedBatchWritesAndDictionaryBounds(t *testing.T) {
	m := memory.NewManager(1 << 20)
	scope := m.NewScope()
	bm := memory.NewBufferManager(nil)
	v, err := NewFlat(scope, bm, types.LogicalType{ID: types.Integer}, 3)
	if err != nil {
		t.Fatal(err)
	}
	var data [12]byte
	for i, x := range []uint32{7, 9, 11} {
		binary.LittleEndian.PutUint32(data[i*4:], x)
	}
	if err := v.WriteFixedWidth(0, data[:]); err != nil {
		t.Fatal(err)
	}
	v.SetNull(1)
	out, err := NewFlat(scope, bm, v.Type, 3)
	if err != nil {
		t.Fatal(err)
	}
	if err := out.CopyFixedIndices(0, v, []uint32{2, 1, 0}); err != nil {
		t.Fatal(err)
	}
	if x, ok := out.Int32(0); !ok || x != 11 {
		t.Fatalf("value=%d valid=%v", x, ok)
	}
	if out.IsValid(1) {
		t.Fatal("dictionary NULL lost")
	}
	if err := out.CopyFixedIndices(0, v, []uint32{3}); err == nil {
		t.Fatal("bad index accepted")
	}
	if err := out.WriteFixedWidth(2, data[:]); err == nil {
		t.Fatal("out-of-bounds batch accepted")
	}
	empty, err := NewFlat(scope, bm, v.Type, 0)
	if err != nil {
		t.Fatal(err)
	}
	if err := out.CopyFixedIndices(0, empty, []uint32{0}); err == nil {
		t.Fatal("empty dictionary accepted")
	}
	empty.Release()
	out.Release()
	v.Release()
	if m.Used() != 0 {
		t.Fatalf("managed leak=%d", m.Used())
	}
}
