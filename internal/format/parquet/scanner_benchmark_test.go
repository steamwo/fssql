package parquet

import (
	"context"
	"testing"

	"github.com/steamwo/fssql/internal/core/memory"
	"github.com/steamwo/fssql/internal/storage/vfs"
)

func BenchmarkRowGroupScannerInt32_1024(b *testing.B) {
	const rows = 1024
	values := make([]int32, rows)
	for i := range values {
		values[i] = int32(i)
	}
	page := scannerRequiredInt32Page(values...)
	meta := &ColumnMetaData{
		Type:                TypeInt32,
		Codec:               CodecUncompressed,
		NumValues:           rows,
		TotalCompressedSize: int64(len(page)),
		DataPageOffset:      0,
		PathInSchema:        []string{"a"},
	}
	rg := RowGroup{NumRows: rows, Columns: []ColumnChunk{{MetaData: meta}}}
	tree := &SchemaTree{Leaves: []*SchemaNode{scannerInt32Leaf("a")}}
	uri, err := vfs.ParseURI("mem://benchmark/row-group")
	if err != nil {
		b.Fatal(err)
	}
	identity := vfs.ObjectIdentity{ETag: "benchmark", Size: int64(len(page))}
	bm := memory.NewBufferManager(nil)
	ctx := context.Background()

	b.ReportAllocs()
	b.SetBytes(rows * 4)
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		mm := memory.NewManager(1 << 20)
		scope := mm.NewScope()
		driver := &recordingDriver{data: page}
		scanner, err := NewRowGroupScanner(driver, uri, identity, tree, rg, []string{"a"}, nil, scope, bm, rows)
		if err != nil {
			b.Fatal(err)
		}
		chunk, err := scanner.Next(ctx)
		if err != nil {
			scanner.Close()
			b.Fatal(err)
		}
		if chunk.Cardinality != rows {
			chunk.Release()
			scanner.Close()
			b.Fatalf("cardinality=%d, want %d", chunk.Cardinality, rows)
		}
		chunk.Release()
		scanner.Close()
		if scope.Used() != 0 {
			b.Fatalf("managed memory leaked: %d", scope.Used())
		}
	}
}
