package parquet

import (
	"context"
	"testing"

	"github.com/steamwo/fssql/internal/core/memory"
	coretypes "github.com/steamwo/fssql/internal/core/types"
	"github.com/steamwo/fssql/internal/storage/vfs"
)

func TestRowGroupScannerExplicitStructProjectionReturnsStructVector(t *testing.T) {
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
	pageA := structPage([]byte{2, 1, 2, 0, 2, 1}, 10, 30)
	pageB := structPage([]byte{2, 2, 2, 0, 2, 1}, 100)
	offsetB := int64(len(pageA))
	data := append(append([]byte(nil), pageA...), pageB...)
	rg := RowGroup{NumRows: 3, Columns: []ColumnChunk{
		{MetaData: &ColumnMetaData{Type: TypeInt32, Codec: CodecUncompressed, NumValues: 3, TotalCompressedSize: int64(len(pageA)), DataPageOffset: 0, PathInSchema: []string{"s", "a"}}},
		{MetaData: &ColumnMetaData{Type: TypeInt32, Codec: CodecUncompressed, NumValues: 3, TotalCompressedSize: int64(len(pageB)), DataPageOffset: offsetB, PathInSchema: []string{"s", "b"}}},
	}}
	d := &recordingDriver{data: data}
	u, _ := vfs.ParseURI("mem://x/struct-scan")
	mm := memory.NewManager(1 << 20)
	scope := mm.NewScope()
	bm := memory.NewBufferManager(nil)
	scanner, err := NewRowGroupScanner(d, u, vfs.ObjectIdentity{ETag: "s", Size: int64(len(data))}, tree, rg, []string{"s"}, nil, scope, bm, 3)
	if err != nil {
		t.Fatal(err)
	}
	chunk, err := scanner.Next(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	if len(chunk.Columns) != 1 || chunk.Columns[0].Type.ID != coretypes.Struct {
		t.Fatalf("columns=%d type=%v", len(chunk.Columns), chunk.Columns[0].Type.ID)
	}
	if chunk.Columns[0].IsValid(1) {
		t.Fatal("row 1 struct should be NULL")
	}
	children := chunk.Columns[0].Children()
	if got, ok := children[0].Int32(2); !ok || got != 30 {
		t.Fatalf("s.a row2=%d valid=%v", got, ok)
	}
	if _, ok := children[1].Int32(2); ok {
		t.Fatal("s.b row2 should be NULL")
	}
	chunk.Release()
	if scanner.Metrics().Columns != 1 || scanner.Metrics().UsefulBytes != int64(len(pageA)+len(pageB)) {
		t.Fatalf("metrics=%+v", scanner.Metrics())
	}
	scanner.Close()
	if scope.Used() != 0 {
		t.Fatalf("managed memory leaked: %d", scope.Used())
	}
}
