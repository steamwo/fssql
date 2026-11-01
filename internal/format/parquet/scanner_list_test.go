package parquet

import (
	"context"
	"testing"

	"github.com/steamwo/fssql/internal/core/memory"
	"github.com/steamwo/fssql/internal/storage/vfs"
)

func TestRowGroupScannerReadsProjectedStandardList(t *testing.T) {
	_, leaf := standardOptionalInt32ListSchema()
	data := standardListDataPage()
	meta := &ColumnMetaData{
		Type: TypeInt32, Codec: CodecUncompressed, NumValues: 6,
		TotalCompressedSize: int64(len(data)), DataPageOffset: 0, PathInSchema: leaf.Path,
	}
	rg := RowGroup{NumRows: 4, Columns: []ColumnChunk{{MetaData: meta}}}
	tree := &SchemaTree{Leaves: []*SchemaNode{leaf}}
	d := &recordingDriver{data: data}
	u, _ := vfs.ParseURI("mem://x/list-row-group")
	mm := memory.NewManager(1 << 20)
	scope := mm.NewScope()
	bm := memory.NewBufferManager(nil)

	scanner, err := NewRowGroupScanner(d, u, vfs.ObjectIdentity{ETag: "e", Size: int64(len(data))}, tree, rg, []string{"items"}, nil, scope, bm, 2)
	if err != nil {
		t.Fatal(err)
	}
	first, err := scanner.Next(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	if first.Cardinality != 2 || len(first.Columns) != 1 {
		t.Fatalf("first cardinality=%d columns=%d", first.Cardinality, len(first.Columns))
	}
	if first.Columns[0].IsValid(0) {
		t.Fatal("first list row should be NULL")
	}
	entry, ok := first.Columns[0].ListEntry(1)
	if !ok || entry.Length != 0 {
		t.Fatalf("empty list entry=%+v valid=%v", entry, ok)
	}
	first.Release()

	second, err := scanner.Next(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	entry, ok = second.Columns[0].ListEntry(0)
	if !ok || entry.Length != 3 {
		t.Fatalf("row 2 entry=%+v valid=%v", entry, ok)
	}
	child := second.Columns[0].Children()[0]
	if child.Count() != 4 || child.IsValid(0) {
		t.Fatalf("child count=%d child0-valid=%v", child.Count(), child.IsValid(0))
	}
	second.Release()
	scanner.Close()

	if scanner.Position() != 4 || scanner.Metrics().RowsProduced != 4 {
		t.Fatalf("position=%d metrics=%+v", scanner.Position(), scanner.Metrics())
	}
	if scope.Used() != 0 {
		t.Fatalf("managed memory leaked: %d", scope.Used())
	}
}
