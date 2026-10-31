package vector

import (
	"github.com/steamwo/fssql/internal/core/memory"
	"github.com/steamwo/fssql/internal/core/types"
	"testing"
)

func env(t *testing.T) (*memory.Manager, *memory.Scope, *memory.BufferManager) {
	t.Helper()
	m := memory.NewManager(1 << 20)
	s := m.NewScope()
	return m, s, memory.NewBufferManager(nil)
}
func TestFlatConstantDictionaryEquivalent(t *testing.T) {
	m, s, bm := env(t)
	flat, _ := NewFlat(s, bm, types.LogicalType{ID: types.BigInt}, 4)
	for i := 0; i < 4; i++ {
		flat.SetInt64(i, 7)
	}
	constant, _ := NewConstant(s, bm, types.LogicalType{ID: types.BigInt}, 4)
	constant.SetInt64(0, 7)
	dict, _ := NewDictionary(flat, SelectionFrom([]uint32{3, 2, 1, 0}))
	for i := 0; i < 4; i++ {
		a, av := flat.Int64(i)
		b, bv := constant.Int64(i)
		c, cv := dict.Int64(i)
		if !av || !bv || !cv || a != b || b != c {
			t.Fatalf("row %d", i)
		}
	}
	flat.Release()
	constant.Release()
	if m.Used() != 0 {
		t.Fatalf("leak %d", m.Used())
	}
}
func TestNullAndUnifiedDictionary(t *testing.T) {
	_, s, bm := env(t)
	v, _ := NewFlat(s, bm, types.LogicalType{ID: types.BigInt}, 3)
	v.SetInt64(0, 10)
	v.SetNull(1)
	v.SetInt64(2, 30)
	d, _ := NewDictionary(v, SelectionFrom([]uint32{2, 1, 0}))
	u := d.Unified()
	if u.Selection.Get(0) != 2 || u.Selection.Get(2) != 0 {
		t.Fatal("selection")
	}
	if _, ok := d.Int64(1); ok {
		t.Fatal("null")
	}
	v.Release()
}
func TestStringRefSSOAndHeap(t *testing.T) {
	m, s, bm := env(t)
	v, _ := NewFlat(s, bm, types.LogicalType{ID: types.Varchar}, 2)
	if err := v.SetString(0, "short"); err != nil {
		t.Fatal(err)
	}
	long := "this string is longer than twelve bytes"
	if err := v.SetString(1, long); err != nil {
		t.Fatal(err)
	}
	a, _ := v.String(0)
	b, _ := v.String(1)
	if a != "short" || b != long {
		t.Fatalf("%q %q", a, b)
	}
	v.Release()
	if m.Used() != 0 {
		t.Fatalf("leak %d", m.Used())
	}
}
func TestListAndStructVectorCapability(t *testing.T) {
	_, s, bm := env(t)
	child, _ := NewFlat(s, bm, types.LogicalType{ID: types.BigInt}, 2)
	child.SetInt64(0, 1)
	child.SetInt64(1, 2)
	listType := types.NewList(types.LogicalType{ID: types.BigInt})
	list, _ := NewFlat(s, bm, listType, 1)
	list.SetChildren(child)
	list.SetListEntry(0, ListEntry{Offset: 0, Length: 2})
	e, ok := list.ListEntry(0)
	if !ok || e.Length != 2 || len(list.Children()) != 1 {
		t.Fatal("list")
	}
	list.Release()
}
func TestSerializeRoundTrip(t *testing.T) {
	m, s, bm := env(t)
	a, _ := NewFlat(s, bm, types.LogicalType{ID: types.BigInt}, 3)
	b, _ := NewFlat(s, bm, types.LogicalType{ID: types.Varchar}, 3)
	for i, x := range []int64{1, 2, 3} {
		a.SetInt64(i, x)
	}
	b.SetString(0, "a")
	b.SetNull(1)
	b.SetString(2, "a sufficiently long string")
	c, _ := NewDataChunk(a, b)
	raw, err := SerializeChunk(c, s, bm)
	if err != nil {
		t.Fatal(err)
	}
	c.Release()
	d, err := DeserializeChunk(raw.Bytes(), s, bm)
	raw.Release()
	if err != nil {
		t.Fatal(err)
	}
	if x, _ := d.Columns[0].Int64(2); x != 3 {
		t.Fatal(x)
	}
	if _, ok := d.Columns[1].String(1); ok {
		t.Fatal("null lost")
	}
	d.Release()
	if m.Used() != 0 {
		t.Fatalf("leak %d", m.Used())
	}
}
func TestChunkCardinalityBound(t *testing.T) {
	_, s, bm := env(t)
	v, _ := NewFlat(s, bm, types.LogicalType{ID: types.BigInt}, DefaultVectorSize+1)
	if _, err := NewDataChunk(v); err == nil {
		t.Fatal("expected bound")
	}
	v.Release()
}
func TestStringRefInlineMarkerIsUnambiguous(t *testing.T) {
	_, s, bm := env(t)
	v, _ := NewFlat(s, bm, types.LogicalType{ID: types.Varchar}, 1)
	want := "\x01abcdefghij"
	if err := v.SetString(0, want); err != nil {
		t.Fatal(err)
	}
	got, ok := v.String(0)
	if !ok || got != want {
		t.Fatalf("got %q want %q", got, want)
	}
	v.Release()
}
