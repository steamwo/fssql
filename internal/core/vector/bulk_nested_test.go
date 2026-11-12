package vector

import (
	"github.com/steamwo/fssql/internal/core/types"
	"testing"
)

func TestBulkNestedCopySelectionOwnsChildren(t *testing.T) {
	m, scope, bm := env(t)
	text := types.LogicalType{ID: types.Varchar}
	listType := types.NewList(text)
	items, err := NewFlat(scope, bm, text, 2)
	if err != nil {
		t.Fatal(err)
	}
	long := "a long nested string whose backing heap must be copied"
	if err := items.SetString(0, long); err != nil {
		t.Fatal(err)
	}
	items.SetNull(1)
	list, err := NewFlat(scope, bm, listType, 2)
	if err != nil {
		items.Release()
		t.Fatal(err)
	}
	list.SetChildren(items)
	list.SetListEntry(0, ListEntry{Length: 2})
	list.SetNull(1)
	tags, err := NewFlat(scope, bm, text, 2)
	if err != nil {
		list.Release()
		t.Fatal(err)
	}
	tags.SetString(0, "left")
	tags.SetString(1, "right")
	root, err := NewFlat(scope, bm, types.NewStruct(types.StructField{Name: "items", Type: listType}, types.StructField{Name: "tag", Type: text}), 2)
	if err != nil {
		list.Release()
		tags.Release()
		t.Fatal(err)
	}
	root.SetChildren(list, tags)
	dict, err := NewDictionary(root, SelectionFrom([]uint32{1, 0, 0}))
	if err != nil {
		root.Release()
		t.Fatal(err)
	}
	copy, err := CopySelection(scope, bm, dict, []int{1, 2, 0})
	dict.Release()
	root.Release()
	if err != nil {
		t.Fatal(err)
	}
	for row := 0; row < 2; row++ {
		entry, ok := copy.Children()[0].ListEntry(row)
		if !ok || entry.Length != 2 {
			copy.Release()
			t.Fatal(entry)
		}
		child := copy.Children()[0].Children()[0]
		got, ok := child.String(int(entry.Offset))
		if !ok || got != long || child.IsValid(int(entry.Offset)+1) {
			copy.Release()
			t.Fatalf("row %d: %q %v", row, got, ok)
		}
	}
	if copy.Children()[0].IsValid(2) {
		copy.Release()
		t.Fatal("NULL LIST became valid")
	}
	if tag, _ := copy.Children()[1].String(2); tag != "right" {
		copy.Release()
		t.Fatal(tag)
	}
	copy.Release()
	if used := m.Used(); used != 0 {
		t.Fatalf("leaked %d", used)
	}
}

func TestBulkResolvedNullTypeSerialization(t *testing.T) {
	m, scope, bm := env(t)
	typ := types.LogicalType{ID: types.Integer, NullType: true}
	col, err := NewFlat(scope, bm, typ, 3)
	if err != nil {
		t.Fatal(err)
	}
	for i := 0; i < 3; i++ {
		col.SetNull(i)
	}
	chunk, err := NewDataChunk(col)
	if err != nil {
		col.Release()
		t.Fatal(err)
	}
	raw, err := SerializeChunk(chunk, scope, bm)
	chunk.Release()
	if err != nil {
		t.Fatal(err)
	}
	roundtrip, err := DeserializeChunk(raw.Bytes(), scope, bm)
	raw.Release()
	if err != nil {
		t.Fatal(err)
	}
	if !roundtrip.Columns[0].Type.Same(typ) {
		roundtrip.Release()
		t.Fatal("SQLNULL metadata lost")
	}
	for i := 0; i < 3; i++ {
		if roundtrip.Columns[0].IsValid(i) {
			roundtrip.Release()
			t.Fatal("NULL validity lost")
		}
	}
	roundtrip.Release()
	if m.Used() != 0 {
		t.Fatal("NULL round trip leaked")
	}
}
