package parquet

import (
	"context"
	"encoding/binary"
	"io"
	"testing"

	"github.com/steamwo/fssql/internal/core/memory"
	coretypes "github.com/steamwo/fssql/internal/core/types"
	"github.com/steamwo/fssql/internal/format/scan"
	"github.com/steamwo/fssql/internal/storage/vfs"
)

func scannerInt32Leaf(name string) *SchemaNode {
	physical := TypeInt32
	required := Required
	return &SchemaNode{
		Name:    name,
		Element: SchemaElement{Name: name, Type: &physical, Repetition: &required},
		Path:    []string{name},
		MaxDef:  0,
		MaxRep:  0,
		Logical: coretypes.LogicalType{ID: coretypes.Integer},
	}
}

func scannerRequiredInt32Page(values ...int32) []byte {
	body := make([]byte, 4*len(values))
	for i, value := range values {
		binary.LittleEndian.PutUint32(body[i*4:], uint32(value))
	}
	header := cursorDataPageHeader(len(values), len(body), EncodingPlain)
	return append(append([]byte(nil), header...), body...)
}

func TestRowGroupScannerProjectionAvoidsUnprojectedRanges(t *testing.T) {
	pageA := scannerRequiredInt32Page(1, 2, 3)
	pageB := scannerRequiredInt32Page(10, 20, 30)
	data := append(append([]byte(nil), pageA...), pageB...)
	bOffset := int64(len(pageA))

	metaA := &ColumnMetaData{Type: TypeInt32, Codec: CodecUncompressed, NumValues: 3, TotalCompressedSize: int64(len(pageA)), DataPageOffset: 0, PathInSchema: []string{"a"}}
	metaB := &ColumnMetaData{Type: TypeInt32, Codec: CodecUncompressed, NumValues: 3, TotalCompressedSize: int64(len(pageB)), DataPageOffset: bOffset, PathInSchema: []string{"b"}}
	rg := RowGroup{NumRows: 3, Columns: []ColumnChunk{{MetaData: metaA}, {MetaData: metaB}}}
	tree := &SchemaTree{Leaves: []*SchemaNode{scannerInt32Leaf("a"), scannerInt32Leaf("b")}}

	d := &recordingDriver{data: data}
	u, _ := vfs.ParseURI("mem://x/row-group")
	mm := memory.NewManager(1 << 20)
	scope := mm.NewScope()
	bm := memory.NewBufferManager(nil)
	scanner, err := NewRowGroupScanner(d, u, vfs.ObjectIdentity{ETag: "e", Size: int64(len(data))}, tree, rg, []string{"b"}, nil, scope, bm, 2)
	if err != nil {
		t.Fatal(err)
	}

	chunk, err := scanner.Next(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	if chunk.Cardinality != 2 || len(chunk.Columns) != 1 {
		t.Fatalf("chunk cardinality=%d columns=%d", chunk.Cardinality, len(chunk.Columns))
	}
	if got, ok := chunk.Columns[0].Int32(1); !ok || got != 20 {
		t.Fatalf("row 1=%d valid=%v", got, ok)
	}
	chunk.Release()

	chunk, err = scanner.Next(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	if got, ok := chunk.Columns[0].Int32(0); !ok || got != 30 {
		t.Fatalf("row 2=%d valid=%v", got, ok)
	}
	chunk.Release()
	if _, err := scanner.Next(context.Background()); err != io.EOF {
		t.Fatalf("final Next error=%v, want EOF", err)
	}
	scanner.Close()

	for _, r := range d.ranges {
		if r.Offset < bOffset {
			t.Fatalf("projection read unprojected column: %+v", r)
		}
	}
	metrics := scanner.Metrics()
	if metrics.Columns != 1 || metrics.RowsProduced != 3 || metrics.PagesRead != 1 {
		t.Fatalf("metrics=%+v", metrics)
	}
	if scope.Used() != 0 {
		t.Fatalf("managed memory leaked: %d", scope.Used())
	}
}

func TestRowGroupScannerAlwaysFalsePrunesBeforeIO(t *testing.T) {
	page := scannerRequiredInt32Page(1, 2, 3)
	zero := int64(0)
	minValue := make([]byte, 4)
	maxValue := make([]byte, 4)
	binary.LittleEndian.PutUint32(minValue, uint32(1))
	binary.LittleEndian.PutUint32(maxValue, uint32(3))
	meta := &ColumnMetaData{
		Type: TypeInt32, Codec: CodecUncompressed, NumValues: 3,
		TotalCompressedSize: int64(len(page)), DataPageOffset: 0, PathInSchema: []string{"a"},
		Statistics: &Statistics{MinValue: minValue, MaxValue: maxValue, NullCount: &zero},
	}
	rg := RowGroup{NumRows: 3, Columns: []ColumnChunk{{MetaData: meta}}}
	tree := &SchemaTree{Leaves: []*SchemaNode{scannerInt32Leaf("a")}}
	filter := scan.Filter{
		Column: "a",
		Op:     scan.GreaterThan,
		Value:  scan.Literal{Type: coretypes.LogicalType{ID: coretypes.Integer}, Int64: 100},
	}

	d := &recordingDriver{data: page}
	u, _ := vfs.ParseURI("mem://x/pruned")
	mm := memory.NewManager(1 << 20)
	scope := mm.NewScope()
	bm := memory.NewBufferManager(nil)
	scanner, err := NewRowGroupScanner(d, u, vfs.ObjectIdentity{}, tree, rg, []string{"a"}, []scan.Filter{filter}, scope, bm, 2)
	if err != nil {
		t.Fatal(err)
	}
	if scanner.PruneResult().Truth != scan.AlwaysFalse {
		t.Fatalf("prune=%v", scanner.PruneResult().Truth)
	}
	if _, err := scanner.Next(context.Background()); err != io.EOF {
		t.Fatalf("Next error=%v, want EOF", err)
	}
	if len(d.ranges) != 0 {
		t.Fatalf("pruned row group performed I/O: %+v", d.ranges)
	}
	if scanner.Metrics().RowGroupsPruned != 1 {
		t.Fatalf("metrics=%+v", scanner.Metrics())
	}
	scanner.Close()
	if scope.Used() != 0 {
		t.Fatalf("managed memory leaked: %d", scope.Used())
	}
}
