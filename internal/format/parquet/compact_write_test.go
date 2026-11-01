package parquet

import (
	"bytes"
	"testing"
)

func TestEncodePageHeaderCompactBytes(t *testing.T) {
	h := PageHeader{Type: PageData, UncompressedPageSize: 3, CompressedPageSize: 3, Data: &DataPageHeader{NumValues: 1, Encoding: EncodingPlain, DefinitionLevelEncoding: EncodingRLE, RepetitionLevelEncoding: EncodingRLE}}
	got, err := EncodePageHeader(h)
	if err != nil {
		t.Fatal(err)
	}
	want := []byte{0x15, 0x00, 0x15, 0x06, 0x15, 0x06, 0x2c, 0x15, 0x02, 0x15, 0x00, 0x15, 0x06, 0x15, 0x06, 0x00, 0x00}
	if !bytes.Equal(got, want) {
		t.Fatalf("page header bytes=%x want=%x", got, want)
	}
}

func TestEncodeFileMetaDataDeterministic(t *testing.T) {
	rootChildren := int32(1)
	rep := Optional
	pt := TypeInt64
	meta := FileMetaData{Version: 1, Schema: []SchemaElement{{Name: "schema", NumChildren: &rootChildren}, {Name: "id", Type: &pt, Repetition: &rep}}, NumRows: 0, CreatedBy: "fssql"}
	a, err := EncodeFileMetaData(meta)
	if err != nil {
		t.Fatal(err)
	}
	b, err := EncodeFileMetaData(meta)
	if err != nil {
		t.Fatal(err)
	}
	if len(a) == 0 || !bytes.Equal(a, b) {
		t.Fatalf("metadata encoding is not deterministic: %x %x", a, b)
	}
}

func TestEncodeRejectsInvalidMetadata(t *testing.T) {
	if _, err := EncodeFileMetaData(FileMetaData{}); err == nil {
		t.Fatal("expected empty schema error")
	}
	if _, err := EncodePageHeader(PageHeader{Type: PageData}); err == nil {
		t.Fatal("expected missing data header error")
	}
}
