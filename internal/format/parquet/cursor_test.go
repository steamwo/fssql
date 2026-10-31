package parquet

import (
	"context"
	"encoding/binary"
	"errors"
	"testing"

	"github.com/steamwo/fssql/internal/core/memory"
	coretypes "github.com/steamwo/fssql/internal/core/types"
	"github.com/steamwo/fssql/internal/storage/vfs"
)

func cursorDataPageHeader(numValues, bodyBytes int, enc Encoding) []byte {
	w := &compactWriter{}
	w.i32Field(1, int32(PageData))
	w.i32Field(2, int32(bodyBytes))
	w.i32Field(3, int32(bodyBytes))
	w.field(5, ctStruct)
	w.b = appendStruct(w.b, func(s *compactWriter) {
		s.i32Field(1, int32(numValues))
		s.i32Field(2, int32(enc))
		s.i32Field(3, int32(EncodingRLE))
		s.i32Field(4, int32(EncodingRLE))
	})
	w.stop()
	return w.b
}

func cursorDictionaryPageHeader(numValues, bodyBytes int) []byte {
	w := &compactWriter{}
	w.i32Field(1, int32(PageDictionary))
	w.i32Field(2, int32(bodyBytes))
	w.i32Field(3, int32(bodyBytes))
	w.field(7, ctStruct)
	w.b = appendStruct(w.b, func(s *compactWriter) {
		s.i32Field(1, int32(numValues))
		s.i32Field(2, int32(EncodingPlain))
	})
	w.stop()
	return w.b
}

func optionalInt32Leaf() *SchemaNode {
	physical := TypeInt32
	optional := Optional
	return &SchemaNode{
		Name:    "x",
		Element: SchemaElement{Name: "x", Type: &physical, Repetition: &optional},
		Path:    []string{"x"},
		MaxDef:  1,
		MaxRep:  0,
		Logical: coretypes.LogicalType{ID: coretypes.Integer},
	}
}

func requiredInt32Leaf() *SchemaNode {
	physical := TypeInt32
	required := Required
	return &SchemaNode{
		Name:    "x",
		Element: SchemaElement{Name: "x", Type: &physical, Repetition: &required},
		Path:    []string{"x"},
		MaxDef:  0,
		MaxRep:  0,
		Logical: coretypes.LogicalType{ID: coretypes.Integer},
	}
}

func TestColumnCursorOptionalPlainAcrossBatches(t *testing.T) {
	// Definition levels are [1,0,1], encoded as three one-value RLE runs.
	levels := []byte{2, 1, 2, 0, 2, 1}
	body := make([]byte, 4+len(levels)+8)
	binary.LittleEndian.PutUint32(body[:4], uint32(len(levels)))
	copy(body[4:], levels)
	binary.LittleEndian.PutUint32(body[4+len(levels):], uint32(10))
	binary.LittleEndian.PutUint32(body[8+len(levels):], uint32(30))
	header := cursorDataPageHeader(3, len(body), EncodingPlain)
	data := append(append([]byte(nil), header...), body...)

	d := &recordingDriver{data: data}
	u, _ := vfs.ParseURI("mem://x/column")
	mm := memory.NewManager(1 << 20)
	scope := mm.NewScope()
	bm := memory.NewBufferManager(nil)
	meta := &ColumnMetaData{Type: TypeInt32, Codec: CodecUncompressed, NumValues: 3, TotalCompressedSize: int64(len(data)), DataPageOffset: 0}
	identity := vfs.ObjectIdentity{ETag: "e", Size: int64(len(data))}
	cursor, err := NewColumnCursor(d, u, identity, meta, optionalInt32Leaf(), scope, bm)
	if err != nil {
		t.Fatal(err)
	}

	first, err := cursor.ReadScalar(context.Background(), 2)
	if err != nil {
		t.Fatal(err)
	}
	if v, ok := first.Int32(0); !ok || v != 10 {
		t.Fatalf("first[0]=%d valid=%v", v, ok)
	}
	if _, ok := first.Int32(1); ok {
		t.Fatal("first[1] should be NULL")
	}
	first.Release()

	second, err := cursor.ReadScalar(context.Background(), 2)
	if err != nil {
		t.Fatal(err)
	}
	if second.Count() != 1 {
		t.Fatalf("second count=%d", second.Count())
	}
	if v, ok := second.Int32(0); !ok || v != 30 {
		t.Fatalf("second[0]=%d valid=%v", v, ok)
	}
	second.Release()
	cursor.Close()

	if cursor.Consumed() != 3 || cursor.Metrics().DataValues != 2 || cursor.Metrics().PagesRead != 1 {
		t.Fatalf("metrics=%+v consumed=%d", cursor.Metrics(), cursor.Consumed())
	}
	for _, r := range d.ranges {
		if r.Length == int64(len(data)) {
			t.Fatalf("cursor requested entire column chunk: %+v", r)
		}
		if r.Expected == nil || r.Expected.ETag != "e" {
			t.Fatalf("range missing stable identity guard: %+v", r)
		}
	}
	if scope.Used() != 0 {
		t.Fatalf("managed memory leaked: %d", scope.Used())
	}
}

func TestColumnCursorDictionaryPage(t *testing.T) {
	dictBody := make([]byte, 8)
	binary.LittleEndian.PutUint32(dictBody[:4], uint32(7))
	binary.LittleEndian.PutUint32(dictBody[4:], uint32(9))
	dictHeader := cursorDictionaryPageHeader(2, len(dictBody))

	// Dictionary indexes [0,1,1,0], bit width 1, encoded as four one-value RLE runs.
	dataBody := []byte{1, 2, 0, 2, 1, 2, 1, 2, 0}
	dataHeader := cursorDataPageHeader(4, len(dataBody), EncodingRLEDictionary)
	dataPageOffset := int64(len(dictHeader) + len(dictBody))
	data := make([]byte, 0, len(dictHeader)+len(dictBody)+len(dataHeader)+len(dataBody))
	data = append(data, dictHeader...)
	data = append(data, dictBody...)
	data = append(data, dataHeader...)
	data = append(data, dataBody...)

	d := &recordingDriver{data: data}
	u, _ := vfs.ParseURI("mem://x/dictionary")
	mm := memory.NewManager(1 << 20)
	scope := mm.NewScope()
	bm := memory.NewBufferManager(nil)
	dictOffset := int64(0)
	meta := &ColumnMetaData{Type: TypeInt32, Codec: CodecUncompressed, NumValues: 4, TotalCompressedSize: int64(len(data)), DataPageOffset: dataPageOffset, DictionaryPageOffset: &dictOffset}
	cursor, err := NewColumnCursor(d, u, vfs.ObjectIdentity{ETag: "d", Size: int64(len(data))}, meta, requiredInt32Leaf(), scope, bm)
	if err != nil {
		t.Fatal(err)
	}
	out, err := cursor.ReadScalar(context.Background(), 4)
	if err != nil {
		t.Fatal(err)
	}
	want := []int32{7, 9, 9, 7}
	for i, expected := range want {
		got, ok := out.Int32(i)
		if !ok || got != expected {
			t.Fatalf("row %d = %d valid=%v want=%d", i, got, ok, expected)
		}
	}
	out.Release()
	cursor.Close()
	if cursor.Metrics().PagesRead != 2 || cursor.Metrics().DataValues != 4 {
		t.Fatalf("metrics=%+v", cursor.Metrics())
	}
	if scope.Used() != 0 {
		t.Fatalf("managed memory leaked: %d", scope.Used())
	}
}

func TestColumnCursorPropagatesObjectChangedAndReleases(t *testing.T) {
	body := make([]byte, 4)
	binary.LittleEndian.PutUint32(body, uint32(42))
	header := cursorDataPageHeader(1, len(body), EncodingPlain)
	data := append(append([]byte(nil), header...), body...)
	d := &recordingDriver{data: data, mutateAt: 1}
	u, _ := vfs.ParseURI("mem://x/mutating")
	mm := memory.NewManager(1 << 20)
	scope := mm.NewScope()
	bm := memory.NewBufferManager(nil)
	meta := &ColumnMetaData{Type: TypeInt32, Codec: CodecUncompressed, NumValues: 1, TotalCompressedSize: int64(len(data)), DataPageOffset: 0}
	cursor, err := NewColumnCursor(d, u, vfs.ObjectIdentity{ETag: "before", Size: int64(len(data))}, meta, requiredInt32Leaf(), scope, bm)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := cursor.ReadScalar(context.Background(), 1); !errors.Is(err, vfs.ErrObjectChanged) {
		t.Fatalf("error=%v, want ErrObjectChanged", err)
	}
	cursor.Close()
	if scope.Used() != 0 {
		t.Fatalf("managed memory leaked: %d", scope.Used())
	}
}
