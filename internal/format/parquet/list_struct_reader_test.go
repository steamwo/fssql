package parquet

import (
	"context"
	"encoding/binary"
	"testing"

	"github.com/steamwo/fssql/internal/core/memory"
	coretypes "github.com/steamwo/fssql/internal/core/types"
	"github.com/steamwo/fssql/internal/storage/vfs"
)

func listStructRLELevels(levels []int, bitWidth int) []byte {
	byteWidth := (bitWidth + 7) / 8
	out := make([]byte, 0, len(levels)*(1+byteWidth))
	for _, level := range levels {
		out = append(out, 2) // one-value RLE run: (1 << 1)
		for i := 0; i < byteWidth; i++ {
			out = append(out, byte(uint32(level)>>uint(8*i)))
		}
	}
	return out
}

func listStructPage(rep, def []int, maxRep, maxDef int, values ...int32) []byte {
	repData := listStructRLELevels(rep, levelBitWidth(maxRep))
	defData := listStructRLELevels(def, levelBitWidth(maxDef))
	body := make([]byte, 4+len(repData)+4+len(defData)+4*len(values))
	off := 0
	binary.LittleEndian.PutUint32(body[off:off+4], uint32(len(repData)))
	off += 4
	copy(body[off:off+len(repData)], repData)
	off += len(repData)
	binary.LittleEndian.PutUint32(body[off:off+4], uint32(len(defData)))
	off += 4
	copy(body[off:off+len(defData)], defData)
	off += len(defData)
	for _, value := range values {
		binary.LittleEndian.PutUint32(body[off:off+4], uint32(value))
		off += 4
	}
	header := cursorDataPageHeader(len(def), len(body), EncodingPlain)
	return append(append([]byte(nil), header...), body...)
}

func listStructSchema(t *testing.T) *SchemaTree {
	t.Helper()
	rootChildren := int32(1)
	listChildren := int32(1)
	repeatedChildren := int32(1)
	structChildren := int32(2)
	optional := Optional
	repeated := Repeated
	required := Required
	int32Type := TypeInt32
	listLogical := &LogicalType{Kind: LogicalList}
	flat := []SchemaElement{
		{Name: "schema", NumChildren: &rootChildren},
		{Name: "items", NumChildren: &listChildren, Repetition: &optional, Logical: listLogical},
		{Name: "list", NumChildren: &repeatedChildren, Repetition: &repeated},
		{Name: "element", NumChildren: &structChildren, Repetition: &optional},
		{Name: "a", Type: &int32Type, Repetition: &required},
		{Name: "b", Type: &int32Type, Repetition: &optional},
	}
	tree, err := BuildSchemaTree(flat)
	if err != nil {
		t.Fatal(err)
	}
	if got := tree.Root.Children[0].Logical; got.ID != coretypes.List || got.Child == nil || got.Child.ID != coretypes.Struct {
		t.Fatalf("items logical type=%+v", got)
	}
	return tree
}

func TestListStructColumnReaderAndScannerPreserveNestedNullSemantics(t *testing.T) {
	tree := listStructSchema(t)
	items := tree.Root.Children[0]
	leafA := tree.Find("items", "list", "element", "a")
	leafB := tree.Find("items", "list", "element", "b")
	if leafA == nil || leafB == nil {
		t.Fatal("LIST<STRUCT> leaves not found")
	}
	rep := []int{0, 0, 0, 1, 1, 0}
	pageA := listStructPage(rep, []int{0, 1, 2, 3, 3, 3}, leafA.MaxRep, leafA.MaxDef, 1, 2, 3)
	pageB := listStructPage(rep, []int{0, 1, 2, 3, 4, 4}, leafB.MaxRep, leafB.MaxDef, 20, 30)
	offsetB := int64(len(pageA))
	data := append(append([]byte(nil), pageA...), pageB...)
	rg := RowGroup{NumRows: 4, Columns: []ColumnChunk{
		{MetaData: &ColumnMetaData{Type: TypeInt32, Codec: CodecUncompressed, NumValues: 6, TotalCompressedSize: int64(len(pageA)), DataPageOffset: 0, PathInSchema: leafA.Path}},
		{MetaData: &ColumnMetaData{Type: TypeInt32, Codec: CodecUncompressed, NumValues: 6, TotalCompressedSize: int64(len(pageB)), DataPageOffset: offsetB, PathInSchema: leafB.Path}},
	}}

	d := &recordingDriver{data: data}
	u, _ := vfs.ParseURI("mem://x/list-struct")
	mm := memory.NewManager(1 << 20)
	scope := mm.NewScope()
	bm := memory.NewBufferManager(nil)
	scanner, err := NewRowGroupScanner(d, u, vfs.ObjectIdentity{ETag: "ls", Size: int64(len(data))}, tree, rg, []string{"items"}, nil, scope, bm, 2)
	if err != nil {
		t.Fatal(err)
	}

	first, err := scanner.Next(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	if len(first.Columns) != 1 || first.Columns[0].Type.ID != coretypes.List {
		t.Fatalf("first output type=%v", first.Columns[0].Type.ID)
	}
	list := first.Columns[0]
	if list.IsValid(0) {
		t.Fatal("row 0 list should be NULL")
	}
	if entry, ok := list.ListEntry(1); !ok || entry.Length != 0 {
		t.Fatalf("row 1 should be empty list: entry=%+v valid=%v", entry, ok)
	}
	first.Release()

	second, err := scanner.Next(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	list = second.Columns[0]
	entry0, ok := list.ListEntry(0)
	if !ok || entry0.Length != 3 {
		t.Fatalf("row 2 entry=%+v valid=%v", entry0, ok)
	}
	entry1, ok := list.ListEntry(1)
	if !ok || entry1.Length != 1 {
		t.Fatalf("row 3 entry=%+v valid=%v", entry1, ok)
	}
	children := list.Children()
	if len(children) != 1 || children[0].Type.ID != coretypes.Struct {
		t.Fatalf("list child type=%v children=%d", children[0].Type.ID, len(children))
	}
	structs := children[0]
	if structs.IsValid(int(entry0.Offset)) {
		t.Fatal("first element of row 2 should be NULL struct")
	}
	fields := structs.Children()
	if len(fields) != 2 {
		t.Fatalf("struct fields=%d", len(fields))
	}
	if got, ok := fields[0].Int32(int(entry0.Offset + 1)); !ok || got != 1 {
		t.Fatalf("row2 element1 a=%d valid=%v", got, ok)
	}
	if _, ok := fields[1].Int32(int(entry0.Offset + 1)); ok {
		t.Fatal("row2 element1 b should be NULL")
	}
	if got, ok := fields[0].Int32(int(entry0.Offset + 2)); !ok || got != 2 {
		t.Fatalf("row2 element2 a=%d valid=%v", got, ok)
	}
	if got, ok := fields[1].Int32(int(entry0.Offset + 2)); !ok || got != 20 {
		t.Fatalf("row2 element2 b=%d valid=%v", got, ok)
	}
	if got, ok := fields[0].Int32(int(entry1.Offset)); !ok || got != 3 {
		t.Fatalf("row3 element a=%d valid=%v", got, ok)
	}
	if got, ok := fields[1].Int32(int(entry1.Offset)); !ok || got != 30 {
		t.Fatalf("row3 element b=%d valid=%v", got, ok)
	}
	second.Release()

	metrics := scanner.Metrics()
	if metrics.RowsProduced != 4 || metrics.Columns != 1 || metrics.UsefulBytes != int64(len(pageA)+len(pageB)) {
		t.Fatalf("metrics=%+v", metrics)
	}
	scanner.Close()
	if scope.Used() != 0 {
		t.Fatalf("managed memory leaked: %d", scope.Used())
	}
	_ = items
}
