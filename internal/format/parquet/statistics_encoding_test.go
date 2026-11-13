package parquet

import (
	"testing"

	"github.com/steamwo/fssql/internal/core/types"
	"github.com/steamwo/fssql/internal/format/scan"
)

func TestStatisticsEmptyBoundsAndNaNCountRoundTrip(t *testing.T) {
	zero, nans := int64(0), int64(2)
	header := PageHeader{Type: PageData, Data: &DataPageHeader{NumValues: 3, Statistics: &Statistics{MinValue: []byte{}, MaxValue: []byte{}, NullCount: &zero, NaNCount: &nans}}}
	encoded, err := EncodePageHeader(header)
	if err != nil {
		t.Fatal(err)
	}
	decoded, _, err := DecodePageHeader(encoded)
	if err != nil {
		t.Fatal(err)
	}
	stats := decoded.Data.Statistics
	if stats.MinValue == nil || stats.MaxValue == nil || stats.NaNCount == nil || *stats.NaNCount != nans {
		t.Fatalf("statistics=%+v", stats)
	}
	stats.NaNCount = nil // The following separate pruning case is a string column.
	leaf := scannerInt32Leaf("s")
	physical := TypeByteArray
	leaf.Element.Type = &physical
	leaf.Logical = types.LogicalType{ID: types.Varchar}
	filter := scan.Filter{Column: "s", Op: scan.Equal, Value: scan.Literal{Type: leaf.Logical, String: "x"}}
	if pageStatisticsTruth(leaf, stats, 3, filter) != scan.AlwaysFalse {
		t.Fatal("empty string bounds were ignored")
	}
}

func TestColumnOrderPlainAndEncryptedEncoding(t *testing.T) {
	rootChildren := int32(1)
	physical := TypeInt64
	meta := FileMetaData{Version: 1, Schema: []SchemaElement{{Name: "schema", NumChildren: &rootChildren}, {Name: "id", Type: &physical}}, ColumnOrders: []ColumnOrder{TypeDefinedOrder}}
	for _, encode := range []func(FileMetaData) ([]byte, error){EncodeFileMetaData, EncodeEncryptedFileMetaData} {
		data, err := encode(meta)
		if err != nil {
			t.Fatal(err)
		}
		decoded, err := DecodeFileMetaData(data)
		if err != nil {
			t.Fatal(err)
		}
		if len(decoded.ColumnOrders) != 1 || decoded.ColumnOrders[0] != TypeDefinedOrder {
			t.Fatalf("orders=%v", decoded.ColumnOrders)
		}
	}
}
