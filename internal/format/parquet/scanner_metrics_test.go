package parquet

import (
	"context"
	"testing"

	"github.com/steamwo/fssql/internal/core/memory"
	"github.com/steamwo/fssql/internal/storage/vfs"
)

func TestRowGroupScannerUsefulBytesOnlyAfterCompleteProjectedScan(t *testing.T) {
	pageA := scannerRequiredInt32Page(1, 2, 3)
	pageB := scannerRequiredInt32Page(10, 20, 30)
	data := append(append([]byte(nil), pageA...), pageB...)
	bOffset := int64(len(pageA))
	metaA := &ColumnMetaData{Type: TypeInt32, Codec: CodecUncompressed, NumValues: 3, TotalCompressedSize: int64(len(pageA)), DataPageOffset: 0, PathInSchema: []string{"a"}}
	metaB := &ColumnMetaData{Type: TypeInt32, Codec: CodecUncompressed, NumValues: 3, TotalCompressedSize: int64(len(pageB)), DataPageOffset: bOffset, PathInSchema: []string{"b"}}
	rg := RowGroup{NumRows: 3, Columns: []ColumnChunk{{MetaData: metaA}, {MetaData: metaB}}}
	tree := &SchemaTree{Leaves: []*SchemaNode{scannerInt32Leaf("a"), scannerInt32Leaf("b")}}

	d := &recordingDriver{data: data}
	u, _ := vfs.ParseURI("mem://x/useful")
	mm := memory.NewManager(1 << 20)
	scope := mm.NewScope()
	bm := memory.NewBufferManager(nil)
	scanner, err := NewRowGroupScanner(d, u, vfs.ObjectIdentity{ETag: "e", Size: int64(len(data))}, tree, rg, []string{"b"}, nil, scope, bm, 2)
	if err != nil {
		t.Fatal(err)
	}
	defer scanner.Close()

	if got := scanner.Metrics().UsefulBytes; got != 0 {
		t.Fatalf("useful before scan=%d", got)
	}
	chunk, err := scanner.Next(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	chunk.Release()
	if got := scanner.Metrics().UsefulBytes; got != 0 {
		t.Fatalf("useful for partial scan=%d", got)
	}
	chunk, err = scanner.Next(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	chunk.Release()
	metrics := scanner.Metrics()
	if metrics.UsefulBytes != int64(len(pageB)) {
		t.Fatalf("useful=%d want=%d metrics=%+v", metrics.UsefulBytes, len(pageB), metrics)
	}
	if metrics.TransferredBytes < metrics.UsefulBytes {
		t.Fatalf("transferred=%d useful=%d", metrics.TransferredBytes, metrics.UsefulBytes)
	}
	if scope.Used() != 0 {
		t.Fatalf("managed memory leaked: %d", scope.Used())
	}
}
