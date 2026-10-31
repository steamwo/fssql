package parquet

import (
	"context"
	"encoding/binary"
	"testing"

	"github.com/steamwo/fssql/internal/core/memory"
	"github.com/steamwo/fssql/internal/storage/vfs"
)

func structPage(defLevels []byte, values ...int32) []byte {
	body := make([]byte, 4+len(defLevels)+4*len(values))
	binary.LittleEndian.PutUint32(body[:4], uint32(len(defLevels)))
	copy(body[4:], defLevels)
	off := 4 + len(defLevels)
	for _, value := range values {
		binary.LittleEndian.PutUint32(body[off:off+4], uint32(value))
		off += 4
	}
	header := cursorDataPageHeader(3, len(body), EncodingPlain)
	return append(append([]byte(nil), header...), body...)
}

func TestStructColumnReaderReconstructsParentAndChildNulls(t *testing.T) {
	rootChildren := int32(1)
	structChildren := int32(2)
	optional := Optional
	required := Required
	int32Type := TypeInt32
	flat := []SchemaElement{
		{Name: "schema", NumChildren: &rootChildren},
		{Name: "s", NumChildren: &structChildren, Repetition: &optional},
		{Name: "a", Type: &int32Type, Repetition: &required},
		{Name: "b", Type: &int32Type, Repetition: &optional},
	}
	tree, err := BuildSchemaTree(flat)
	if err != nil {
		t.Fatal(err)
	}
	node := tree.Root.Children[0]
	// s.a definitions [1,0,1]: present, parent NULL, present.
	pageA := structPage([]byte{2, 1, 2, 0, 2, 1}, 10, 30)
	// s.b definitions [2,0,1]: value, parent NULL, child NULL. maxDef=2 -> bit width 2.
	pageB := structPage([]byte{2, 2, 2, 0, 2, 1}, 100)
	offsetB := int64(len(pageA))
	data := append(append([]byte(nil), pageA...), pageB...)
	rg := RowGroup{NumRows: 3, Columns: []ColumnChunk{
		{MetaData: &ColumnMetaData{Type: TypeInt32, Codec: CodecUncompressed, NumValues: 3, TotalCompressedSize: int64(len(pageA)), DataPageOffset: 0, PathInSchema: []string{"s", "a"}}},
		{MetaData: &ColumnMetaData{Type: TypeInt32, Codec: CodecUncompressed, NumValues: 3, TotalCompressedSize: int64(len(pageB)), DataPageOffset: offsetB, PathInSchema: []string{"s", "b"}}},
	}}
	d := &recordingDriver{data: data}
	u, _ := vfs.ParseURI("mem://x/struct")
	mm := memory.NewManager(1 << 20)
	scope := mm.NewScope()
	bm := memory.NewBufferManager(nil)
	reader, err := NewStructColumnReader(d, u, vfs.ObjectIdentity{ETag: "s", Size: int64(len(data))}, rg, node, scope, bm)
	if err != nil {
		t.Fatal(err)
	}
	out, err := reader.Read(context.Background(), 3)
	if err != nil {
		t.Fatal(err)
	}
	if !out.IsValid(0) || out.IsValid(1) || !out.IsValid(2) {
		t.Fatalf("parent validity = [%v %v %v]", out.IsValid(0), out.IsValid(1), out.IsValid(2))
	}
	children := out.Children()
	if len(children) != 2 {
		t.Fatalf("children=%d", len(children))
	}
	if got, ok := children[0].Int32(0); !ok || got != 10 {
		t.Fatalf("a[0]=%d valid=%v", got, ok)
	}
	if _, ok := children[0].Int32(1); ok {
		t.Fatal("a[1] should be NULL because parent is NULL")
	}
	if got, ok := children[0].Int32(2); !ok || got != 30 {
		t.Fatalf("a[2]=%d valid=%v", got, ok)
	}
	if got, ok := children[1].Int32(0); !ok || got != 100 {
		t.Fatalf("b[0]=%d valid=%v", got, ok)
	}
	if _, ok := children[1].Int32(1); ok {
		t.Fatal("b[1] should be NULL because parent is NULL")
	}
	if _, ok := children[1].Int32(2); ok {
		t.Fatal("b[2] should be NULL while parent remains valid")
	}
	out.Release()
	reader.Close()
	if reader.RowPosition() != 3 {
		t.Fatalf("row position=%d", reader.RowPosition())
	}
	if scope.Used() != 0 {
		t.Fatalf("managed memory leaked: %d", scope.Used())
	}
}
