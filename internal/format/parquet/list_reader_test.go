package parquet

import (
	"context"
	"encoding/binary"
	"testing"

	"github.com/steamwo/fssql/internal/core/memory"
	coretypes "github.com/steamwo/fssql/internal/core/types"
	"github.com/steamwo/fssql/internal/storage/vfs"
)

func standardOptionalInt32ListSchema() (*SchemaNode, *SchemaNode) {
	optional := Optional
	repeatedRep := Repeated
	physical := TypeInt32
	childLogical := coretypes.LogicalType{ID: coretypes.Integer}
	list := &SchemaNode{
		Name: "items",
		Element: SchemaElement{Name: "items", Repetition: &optional, Logical: &LogicalType{Kind: LogicalList}},
		Path: []string{"items"}, MaxDef: 1, MaxRep: 0,
		Logical: coretypes.NewList(childLogical),
	}
	repeated := &SchemaNode{
		Name: "list", Element: SchemaElement{Name: "list", Repetition: &repeatedRep},
		Parent: list, Path: []string{"items", "list"}, MaxDef: 2, MaxRep: 1,
	}
	leaf := &SchemaNode{
		Name: "element", Element: SchemaElement{Name: "element", Type: &physical, Repetition: &optional},
		Parent: repeated, Path: []string{"items", "list", "element"}, MaxDef: 3, MaxRep: 1,
		Logical: childLogical,
	}
	list.Children = []*SchemaNode{repeated}
	repeated.Children = []*SchemaNode{leaf}
	return list, leaf
}

func v1RLELevels(values []int) []byte {
	raw := make([]byte, 0, len(values)*2)
	for _, value := range values {
		// Hybrid RLE header 2 means a one-value RLE run; all LIST levels fit in one byte.
		raw = append(raw, 2, byte(value))
	}
	out := make([]byte, 4, 4+len(raw))
	binary.LittleEndian.PutUint32(out[:4], uint32(len(raw)))
	return append(out, raw...)
}

func standardListDataPage() []byte {
	// Four rows: NULL, [], [NULL,7,8], [9].
	rep := v1RLELevels([]int{0, 0, 0, 1, 1, 0})
	def := v1RLELevels([]int{0, 1, 2, 3, 3, 3})
	values := make([]byte, 12)
	binary.LittleEndian.PutUint32(values[0:4], uint32(7))
	binary.LittleEndian.PutUint32(values[4:8], uint32(8))
	binary.LittleEndian.PutUint32(values[8:12], uint32(9))
	body := append(append(rep, def...), values...)
	header := cursorDataPageHeader(6, len(body), EncodingPlain)
	return append(header, body...)
}

func TestListColumnReaderStandardThreeLevelAcrossBatches(t *testing.T) {
	list, leaf := standardOptionalInt32ListSchema()
	data := standardListDataPage()
	d := &recordingDriver{data: data}
	u, _ := vfs.ParseURI("mem://x/list")
	mm := memory.NewManager(1 << 20)
	scope := mm.NewScope()
	bm := memory.NewBufferManager(nil)
	meta := &ColumnMetaData{
		Type: TypeInt32, Codec: CodecUncompressed, NumValues: 6,
		TotalCompressedSize: int64(len(data)), DataPageOffset: 0, PathInSchema: leaf.Path,
	}
	reader, err := NewListColumnReader(d, u, vfs.ObjectIdentity{ETag: "e", Size: int64(len(data))}, meta, list, 4, scope, bm)
	if err != nil {
		t.Fatal(err)
	}

	first, err := reader.Read(context.Background(), 2)
	if err != nil {
		t.Fatal(err)
	}
	if first.IsValid(0) {
		t.Fatal("row 0 should be a NULL list")
	}
	entry, ok := first.ListEntry(1)
	if !ok || entry.Length != 0 {
		t.Fatalf("row 1 entry=%+v valid=%v, want empty list", entry, ok)
	}
	if len(first.Children()) != 1 || first.Children()[0].Count() != 0 {
		t.Fatal("first batch should have an empty child vector")
	}
	first.Release()

	second, err := reader.Read(context.Background(), 2)
	if err != nil {
		t.Fatal(err)
	}
	entry, ok = second.ListEntry(0)
	if !ok || entry.Offset != 0 || entry.Length != 3 {
		t.Fatalf("row 2 entry=%+v valid=%v", entry, ok)
	}
	entry, ok = second.ListEntry(1)
	if !ok || entry.Offset != 3 || entry.Length != 1 {
		t.Fatalf("row 3 entry=%+v valid=%v", entry, ok)
	}
	child := second.Children()[0]
	if child.Count() != 4 {
		t.Fatalf("child count=%d, want 4", child.Count())
	}
	if child.IsValid(0) {
		t.Fatal("first child should be NULL")
	}
	for i, want := range []int32{7, 8, 9} {
		got, valid := child.Int32(i + 1)
		if !valid || got != want {
			t.Fatalf("child %d=%d valid=%v, want %d", i+1, got, valid, want)
		}
	}
	second.Release()
	reader.Close()

	if reader.RowPosition() != 4 {
		t.Fatalf("row position=%d, want 4", reader.RowPosition())
	}
	if reader.Metrics().DataValues != 3 || reader.Metrics().PagesRead != 1 {
		t.Fatalf("metrics=%+v", reader.Metrics())
	}
	if scope.Used() != 0 {
		t.Fatalf("managed memory leaked: %d", scope.Used())
	}
}

func TestValidateStandardListRejectsNonStandardRepeatedPrimitive(t *testing.T) {
	physical := TypeInt32
	repeated := Repeated
	list := &SchemaNode{
		Name: "items",
		Element: SchemaElement{Name: "items", Logical: &LogicalType{Kind: LogicalList}},
		Logical: coretypes.NewList(coretypes.LogicalType{ID: coretypes.Integer}),
		Children: []*SchemaNode{{Name: "element", Element: SchemaElement{Name: "element", Type: &physical, Repetition: &repeated}}},
	}
	if _, _, err := validateStandardList(list); err != ErrUnsupportedListLayout {
		t.Fatalf("error=%v, want ErrUnsupportedListLayout", err)
	}
}
