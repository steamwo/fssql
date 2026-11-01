package parquet

import (
	"context"
	"encoding/binary"
	"io"
	"testing"

	"github.com/steamwo/fssql/internal/core/memory"
	coretypes "github.com/steamwo/fssql/internal/core/types"
	"github.com/steamwo/fssql/internal/storage/vfs"
)

func scannerDecimal128Leaf(name string, precision, scale uint8) *SchemaNode {
	physical := TypeFixedLenByteArray
	required := Required
	length := int32(16)
	logical, err := coretypes.NewDecimal(precision, scale)
	if err != nil {
		panic(err)
	}
	return &SchemaNode{
		Name: name,
		Element: SchemaElement{
			Name:       name,
			Type:       &physical,
			TypeLength: &length,
			Repetition: &required,
		},
		Path:    []string{name},
		MaxDef:  0,
		MaxRep:  0,
		Logical: logical,
	}
}

func scannerRequiredDecimal128Page(values ...coretypes.Int128) []byte {
	body := make([]byte, 0, 16*len(values))
	for _, value := range values {
		var raw [16]byte
		binary.BigEndian.PutUint64(raw[:8], uint64(value.Hi))
		binary.BigEndian.PutUint64(raw[8:], value.Lo)
		body = append(body, raw[:]...)
	}
	header := cursorDataPageHeader(len(values), len(body), EncodingPlain)
	return append(append([]byte(nil), header...), body...)
}

func TestRowGroupScannerDecimal128ControlledDecode(t *testing.T) {
	decimal, err := coretypes.NewDecimal(20, 4)
	if err != nil {
		t.Fatal(err)
	}
	positive := coretypes.Int128{Lo: 12345678901234567890, Hi: 0}
	negative := negateUnsigned128Magnitude(12345678901234567890)
	page := scannerRequiredDecimal128Page(positive, negative)

	meta := &ColumnMetaData{
		Type:                TypeFixedLenByteArray,
		Codec:               CodecUncompressed,
		NumValues:           2,
		TotalCompressedSize: int64(len(page)),
		DataPageOffset:      0,
		PathInSchema:        []string{"amount"},
	}
	rg := RowGroup{NumRows: 2, Columns: []ColumnChunk{{MetaData: meta}}}
	leaf := scannerDecimal128Leaf("amount", decimal.Decimal.Width, decimal.Decimal.Scale)
	tree := &SchemaTree{Leaves: []*SchemaNode{leaf}}

	d := &recordingDriver{data: page}
	u, _ := vfs.ParseURI("mem://x/decimal128-row-group")
	mm := memory.NewManager(1 << 20)
	scope := mm.NewScope()
	bm := memory.NewBufferManager(nil)
	scanner, err := NewRowGroupScanner(
		d,
		u,
		vfs.ObjectIdentity{ETag: "decimal128", Size: int64(len(page))},
		tree,
		rg,
		[]string{"amount"},
		nil,
		scope,
		bm,
		2,
	)
	if err != nil {
		t.Fatal(err)
	}

	chunk, err := scanner.Next(context.Background())
	if err != nil {
		scanner.Close()
		t.Fatal(err)
	}
	if chunk.Cardinality != 2 || len(chunk.Columns) != 1 {
		chunk.Release()
		scanner.Close()
		t.Fatalf("chunk cardinality=%d columns=%d", chunk.Cardinality, len(chunk.Columns))
	}
	gotType := chunk.Columns[0].Type
	if gotType.ID != coretypes.Decimal || gotType.Decimal != decimal.Decimal {
		chunk.Release()
		scanner.Close()
		t.Fatalf("column type=%+v, want DECIMAL(%d,%d)", gotType, decimal.Decimal.Width, decimal.Decimal.Scale)
	}
	want := []coretypes.Int128{positive, negative}
	for i, expected := range want {
		got, ok := chunk.Columns[0].Int128(i)
		if !ok || got != expected {
			chunk.Release()
			scanner.Close()
			t.Fatalf("row %d=%+v valid=%v, want %+v", i, got, ok, expected)
		}
	}
	chunk.Release()
	if _, err := scanner.Next(context.Background()); err != io.EOF {
		scanner.Close()
		t.Fatalf("final Next error=%v, want EOF", err)
	}
	scanner.Close()

	metrics := scanner.Metrics()
	if metrics.Columns != 1 || metrics.RowsProduced != 2 || metrics.PagesRead != 1 {
		t.Fatalf("metrics=%+v", metrics)
	}
	if len(d.ranges) == 0 {
		t.Fatal("controlled scanner should perform range reads")
	}
	if scope.Used() != 0 {
		t.Fatalf("managed memory leaked: %d", scope.Used())
	}
}
