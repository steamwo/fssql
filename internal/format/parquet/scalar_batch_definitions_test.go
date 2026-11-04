package parquet

import (
	"context"
	"encoding/binary"
	"testing"

	"github.com/steamwo/fssql/internal/core/memory"
	"github.com/steamwo/fssql/internal/core/types"
	"github.com/steamwo/fssql/internal/core/vector"
	"github.com/steamwo/fssql/internal/storage/vfs"
)

func rleLevels(levels []int) []byte {
	var out []byte
	for _, level := range levels {
		out = append(out, 2, byte(level))
	}
	return out
}

func packedDictionaryIndexes(indexes []uint32, bitWidth int) []byte {
	out := []byte{byte(bitWidth), 3}
	group := make([]byte, bitWidth)
	for row, index := range indexes {
		for bit := 0; bit < bitWidth; bit++ {
			if index&(1<<uint(bit)) != 0 {
				position := row*bitWidth + bit
				group[position>>3] |= 1 << uint(position&7)
			}
		}
	}
	return append(out, group...)
}

func byteArrayValue(value string) []byte {
	out := make([]byte, 4+len(value))
	binary.LittleEndian.PutUint32(out[:4], uint32(len(value)))
	copy(out[4:], value)
	return out
}

func requiredVarcharLeaf() *SchemaNode {
	physical := TypeByteArray
	required := Required
	return &SchemaNode{
		Name:    "s",
		Element: SchemaElement{Name: "s", Type: &physical, Repetition: &required},
		Path:    []string{"s"},
		MaxDef:  0,
		MaxRep:  0,
		Logical: types.LogicalType{ID: types.Varchar},
	}
}

func optionalVarcharLeaf() *SchemaNode {
	physical := TypeByteArray
	optional := Optional
	return &SchemaNode{
		Name:    "s",
		Element: SchemaElement{Name: "s", Type: &physical, Repetition: &optional},
		Path:    []string{"s"},
		MaxDef:  1,
		MaxRep:  0,
		Logical: types.LogicalType{ID: types.Varchar},
	}
}

func cursorDataPageV2Header(numValues, numNulls, numRows, defBytes, repBytes, valueBytes int, enc Encoding) []byte {
	total := repBytes + defBytes + valueBytes
	w := &compactWriter{}
	w.i32Field(1, int32(PageDataV2))
	w.i32Field(2, int32(total))
	w.i32Field(3, int32(total))
	w.field(8, ctStruct)
	w.b = appendStruct(w.b, func(s *compactWriter) {
		s.i32Field(1, int32(numValues))
		s.i32Field(2, int32(numNulls))
		s.i32Field(3, int32(numRows))
		s.i32Field(4, int32(enc))
		s.i32Field(5, int32(defBytes))
		s.i32Field(6, int32(repBytes))
		s.field(7, ctBoolFalse)
	})
	w.stop()
	return w.b
}

func TestScalarBatchPlainVarcharContiguous(t *testing.T) {
	values := []string{"", "short", "abcdefghijkl", "abcdefghijklm"}
	var body []byte
	for _, value := range values {
		body = append(body, byteArrayValue(value)...)
	}
	data := append(cursorDataPageHeader(len(values), len(body), EncodingPlain), body...)
	driver := &recordingDriver{data: data}
	uri, _ := vfs.ParseURI("mem://x/plain-varchar")
	manager := memory.NewManager(1 << 20)
	scope := manager.NewScope()
	bm := memory.NewBufferManager(nil)
	cursor, err := NewColumnCursor(driver, uri, vfs.ObjectIdentity{ETag: "v", Size: int64(len(data))}, &ColumnMetaData{Type: TypeByteArray, Codec: CodecUncompressed, NumValues: int64(len(values)), TotalCompressedSize: int64(len(data))}, requiredVarcharLeaf(), scope, bm)
	if err != nil {
		t.Fatal(err)
	}
	out, err := vector.NewFlat(scope, bm, types.LogicalType{ID: types.Varchar}, len(values))
	if err != nil {
		t.Fatal(err)
	}
	if n, err := cursor.readScalarBatch(context.Background(), out, 0, 2); err != nil || n != 2 {
		t.Fatalf("first batch n=%d err=%v", n, err)
	}
	if n, err := cursor.readScalarBatch(context.Background(), out, 2, 2); err != nil || n != 2 {
		t.Fatalf("second batch n=%d err=%v", n, err)
	}
	for row, want := range values {
		got, valid := out.String(row)
		if !valid || got != want {
			t.Fatalf("row %d value=%q valid=%v want=%q", row, got, valid, want)
		}
	}
	out.Release()
	cursor.Close()
	if manager.Used() != 0 {
		t.Fatalf("managed leak=%d", manager.Used())
	}
}

func TestScalarBatchPlainVarcharMixedDefinitions(t *testing.T) {
	defs := rleLevels([]int{1, 0, 1, 1, 0, 1})
	var body []byte
	body = append(body, make([]byte, 4)...)
	binary.LittleEndian.PutUint32(body[:4], uint32(len(defs)))
	body = append(body, defs...)
	mixedValues := []string{"aa", "abcdefghijklmnopqrstuvwxyz", "bbb", "aa"}
	for _, value := range mixedValues {
		body = append(body, byteArrayValue(value)...)
	}
	data := append(cursorDataPageHeader(6, len(body), EncodingPlain), body...)
	driver := &recordingDriver{data: data}
	uri, _ := vfs.ParseURI("mem://x/plain-varchar-mixed")
	manager := memory.NewManager(1 << 20)
	scope := manager.NewScope()
	bm := memory.NewBufferManager(nil)
	cursor, err := NewColumnCursor(driver, uri, vfs.ObjectIdentity{ETag: "vm", Size: int64(len(data))}, &ColumnMetaData{Type: TypeByteArray, Codec: CodecUncompressed, NumValues: 6, TotalCompressedSize: int64(len(data))}, optionalVarcharLeaf(), scope, bm)
	if err != nil {
		t.Fatal(err)
	}
	out, err := vector.NewFlat(scope, bm, types.LogicalType{ID: types.Varchar}, 6)
	if err != nil {
		t.Fatal(err)
	}
	if n, err := cursor.readScalarBatch(context.Background(), out, 0, 6); err != nil || n != 6 {
		t.Fatalf("batch n=%d err=%v", n, err)
	}
	want := []struct {
		value string
		valid bool
	}{{"aa", true}, {"", false}, {mixedValues[1], true}, {"bbb", true}, {"", false}, {"aa", true}}
	for row, expected := range want {
		got, valid := out.String(row)
		if valid != expected.valid || valid && got != expected.value {
			t.Fatalf("row %d value=%q valid=%v want=%+v", row, got, valid, expected)
		}
	}
	out.Release()
	cursor.Close()
	if cursor.Metrics().DataValues != 4 || cursor.Consumed() != 6 {
		t.Fatalf("values=%d consumed=%d", cursor.Metrics().DataValues, cursor.Consumed())
	}
	if manager.Used() != 0 {
		t.Fatalf("managed leak=%d", manager.Used())
	}
}

func TestScalarBatchMixedDefinitionsV2(t *testing.T) {
	defs := rleLevels([]int{1, 0, 1, 1, 0, 1, 1, 0})
	var values []byte
	for _, value := range []uint32{10, 20, 30, 40, 50} {
		var raw [4]byte
		binary.LittleEndian.PutUint32(raw[:], value)
		values = append(values, raw[:]...)
	}
	body := append(append([]byte(nil), defs...), values...)
	data := append(cursorDataPageV2Header(8, 3, 8, len(defs), 0, len(values), EncodingPlain), body...)
	driver := &recordingDriver{data: data}
	uri, _ := vfs.ParseURI("mem://x/plain-mixed-v2")
	manager := memory.NewManager(1 << 20)
	scope := manager.NewScope()
	bm := memory.NewBufferManager(nil)
	cursor, err := NewColumnCursor(driver, uri, vfs.ObjectIdentity{ETag: "p2", Size: int64(len(data))}, &ColumnMetaData{Type: TypeInt32, Codec: CodecUncompressed, NumValues: 8, TotalCompressedSize: int64(len(data))}, optionalInt32Leaf(), scope, bm)
	if err != nil {
		t.Fatal(err)
	}
	out, err := cursor.ReadScalar(context.Background(), 8)
	if err != nil {
		t.Fatal(err)
	}
	want := []struct {
		value int32
		valid bool
	}{{10, true}, {0, false}, {20, true}, {30, true}, {0, false}, {40, true}, {50, true}, {0, false}}
	for row, expected := range want {
		got, valid := out.Int32(row)
		if valid != expected.valid || valid && got != expected.value {
			t.Fatalf("row %d value=%d valid=%v want=%+v", row, got, valid, expected)
		}
	}
	out.Release()
	cursor.Close()
	if manager.Used() != 0 {
		t.Fatalf("managed leak=%d", manager.Used())
	}
}

func TestScalarBatchMixedDefinitionsPlainInt32(t *testing.T) {
	defs := rleLevels([]int{1, 0, 1, 0, 1, 1, 0, 1})
	dataBody := make([]byte, 4)
	binary.LittleEndian.PutUint32(dataBody, uint32(len(defs)))
	dataBody = append(dataBody, defs...)
	for _, value := range []uint32{10, 20, 30, 40, 50} {
		var raw [4]byte
		binary.LittleEndian.PutUint32(raw[:], value)
		dataBody = append(dataBody, raw[:]...)
	}
	data := append(cursorDataPageHeader(8, len(dataBody), EncodingPlain), dataBody...)
	driver := &recordingDriver{data: data}
	uri, _ := vfs.ParseURI("mem://x/plain-mixed")
	manager := memory.NewManager(1 << 20)
	scope := manager.NewScope()
	bm := memory.NewBufferManager(nil)
	cursor, err := NewColumnCursor(driver, uri, vfs.ObjectIdentity{ETag: "p", Size: int64(len(data))}, &ColumnMetaData{Type: TypeInt32, Codec: CodecUncompressed, NumValues: 8, TotalCompressedSize: int64(len(data))}, optionalInt32Leaf(), scope, bm)
	if err != nil {
		t.Fatal(err)
	}
	out, err := vector.NewFlat(scope, bm, types.LogicalType{ID: types.Integer}, 8)
	if err != nil {
		t.Fatal(err)
	}
	if n, err := cursor.readScalarBatch(context.Background(), out, 0, 8); err != nil || n != 8 {
		t.Fatalf("batch n=%d err=%v", n, err)
	}
	want := []struct {
		value int32
		valid bool
	}{{10, true}, {0, false}, {20, true}, {0, false}, {30, true}, {40, true}, {0, false}, {50, true}}
	for row, expected := range want {
		got, valid := out.Int32(row)
		if valid != expected.valid || valid && got != expected.value {
			t.Fatalf("row %d value=%d valid=%v want=%+v", row, got, valid, expected)
		}
	}
	out.Release()
	cursor.Close()
	if manager.Used() != 0 {
		t.Fatalf("managed leak=%d", manager.Used())
	}
}

func TestScalarBatchMixedDefinitionsDictionaryInt32(t *testing.T) {
	dictBody := make([]byte, 8)
	binary.LittleEndian.PutUint32(dictBody[:4], 7)
	binary.LittleEndian.PutUint32(dictBody[4:], 9)
	dictHeader := cursorDictionaryPageHeader(2, len(dictBody))

	defs := rleLevels([]int{1, 0, 1, 1, 0, 1})
	dataBody := make([]byte, 4)
	binary.LittleEndian.PutUint32(dataBody, uint32(len(defs)))
	dataBody = append(dataBody, defs...)
	dataBody = append(dataBody, packedDictionaryIndexes([]uint32{0, 1, 1, 0}, 1)...)
	dataHeader := cursorDataPageHeader(6, len(dataBody), EncodingRLEDictionary)
	dataPageOffset := int64(len(dictHeader) + len(dictBody))
	data := append(append(append(append([]byte(nil), dictHeader...), dictBody...), dataHeader...), dataBody...)

	driver := &recordingDriver{data: data}
	uri, _ := vfs.ParseURI("mem://x/dictionary-mixed")
	manager := memory.NewManager(1 << 20)
	scope := manager.NewScope()
	bm := memory.NewBufferManager(nil)
	dictOffset := int64(0)
	meta := &ColumnMetaData{Type: TypeInt32, Codec: CodecUncompressed, NumValues: 6, TotalCompressedSize: int64(len(data)), DataPageOffset: dataPageOffset, DictionaryPageOffset: &dictOffset}
	cursor, err := NewColumnCursor(driver, uri, vfs.ObjectIdentity{ETag: "d", Size: int64(len(data))}, meta, optionalInt32Leaf(), scope, bm)
	if err != nil {
		t.Fatal(err)
	}
	out, err := vector.NewFlat(scope, bm, types.LogicalType{ID: types.Integer}, 6)
	if err != nil {
		t.Fatal(err)
	}
	if n, err := cursor.readScalarBatch(context.Background(), out, 0, 4); err != nil || n != 4 {
		t.Fatalf("batch n=%d err=%v", n, err)
	}
	if n, err := cursor.readScalarBatch(context.Background(), out, 4, 2); err != nil || n != 2 {
		t.Fatalf("batch n=%d err=%v", n, err)
	}
	want := []struct {
		value int32
		valid bool
	}{{7, true}, {0, false}, {9, true}, {9, true}, {0, false}, {7, true}}
	for row, expected := range want {
		got, valid := out.Int32(row)
		if valid != expected.valid || valid && got != expected.value {
			t.Fatalf("row %d value=%d valid=%v want=%+v", row, got, valid, expected)
		}
	}
	out.Release()
	cursor.Close()
	if cursor.Metrics().DataValues != 4 || cursor.Consumed() != 6 {
		t.Fatalf("values=%d consumed=%d", cursor.Metrics().DataValues, cursor.Consumed())
	}
	if manager.Used() != 0 {
		t.Fatalf("managed leak=%d", manager.Used())
	}
}

func TestScalarBatchMixedDefinitionsDictionaryVarchar(t *testing.T) {
	dictBody := append(byteArrayValue("aa"), byteArrayValue("bbb")...)
	dictHeader := cursorDictionaryPageHeader(2, len(dictBody))

	defs := rleLevels([]int{1, 0, 1, 1, 0, 1})
	dataBody := make([]byte, 4)
	binary.LittleEndian.PutUint32(dataBody, uint32(len(defs)))
	dataBody = append(dataBody, defs...)
	dataBody = append(dataBody, packedDictionaryIndexes([]uint32{0, 1, 1, 0}, 1)...)
	dataHeader := cursorDataPageHeader(6, len(dataBody), EncodingRLEDictionary)
	dataPageOffset := int64(len(dictHeader) + len(dictBody))
	data := append(append(append(append([]byte(nil), dictHeader...), dictBody...), dataHeader...), dataBody...)

	driver := &recordingDriver{data: data}
	uri, _ := vfs.ParseURI("mem://x/dictionary-varchar")
	manager := memory.NewManager(1 << 20)
	scope := manager.NewScope()
	bm := memory.NewBufferManager(nil)
	dictOffset := int64(0)
	meta := &ColumnMetaData{Type: TypeByteArray, Codec: CodecUncompressed, NumValues: 6, TotalCompressedSize: int64(len(data)), DataPageOffset: dataPageOffset, DictionaryPageOffset: &dictOffset}
	cursor, err := NewColumnCursor(driver, uri, vfs.ObjectIdentity{ETag: "d", Size: int64(len(data))}, meta, optionalVarcharLeaf(), scope, bm)
	if err != nil {
		t.Fatal(err)
	}
	out, err := cursor.ReadScalar(context.Background(), 6)
	if err != nil {
		t.Fatal(err)
	}
	want := []struct {
		value string
		valid bool
	}{{"aa", true}, {"", false}, {"bbb", true}, {"bbb", true}, {"", false}, {"aa", true}}
	for row, expected := range want {
		got, valid := out.String(row)
		if valid != expected.valid || valid && got != expected.value {
			t.Fatalf("row %d value=%q valid=%v want=%+v", row, got, valid, expected)
		}
	}
	out.Release()
	cursor.Close()
	if cursor.Metrics().DataValues != 4 || cursor.Consumed() != 6 {
		t.Fatalf("values=%d consumed=%d", cursor.Metrics().DataValues, cursor.Consumed())
	}
	if manager.Used() != 0 {
		t.Fatalf("managed leak=%d", manager.Used())
	}
}
