package introspection

import (
	"testing"

	coretypes "github.com/steamwo/fssql/internal/core/types"
)

func TestSourceReportCopiesSchemaAndFormatsNestedTypes(t *testing.T) {
	decimal, err := coretypes.NewDecimal(20, 4)
	if err != nil {
		t.Fatal(err)
	}
	item := coretypes.NewStruct(
		coretypes.StructField{Name: "amount", Type: decimal},
		coretypes.StructField{Name: "ts", Type: coretypes.LogicalType{ID: coretypes.TimestampType}},
	)
	fields := []coretypes.StructField{
		{Name: "id", Type: coretypes.LogicalType{ID: coretypes.BigInt}},
		{Name: "items", Type: coretypes.NewList(item)},
	}

	report := NewSourceReport(fields, SchemaSourceFileMetadata, ConfidenceExact, 7, 1, "fp", false, false)
	cols := report.Columns()
	if len(cols) != 2 || cols[0].Name != "id" || cols[0].TypeName != "BIGINT" {
		t.Fatalf("columns=%+v", cols)
	}
	if got, want := cols[1].TypeName, "LIST<STRUCT<amount DECIMAL(20,4), ts TIMESTAMP>>"; got != want {
		t.Fatalf("type name=%q want=%q", got, want)
	}
	if report.SchemaSource != SchemaSourceFileMetadata || report.Confidence != ConfidenceExact || report.MatchedFiles != 7 || report.SchemaFiles != 1 || report.DatasetFingerprint != "fp" || report.UnionByName || report.FullValidation {
		t.Fatalf("report=%+v", report)
	}

	// Mutating caller-owned input or returned copies must not change the report.
	fields[0].Name = "changed"
	cols[0].Name = "changed"
	cols[1].Type.Child.Fields[0].Name = "changed"
	cols2 := report.Columns()
	if cols2[0].Name != "id" || cols2[1].Type.Child == nil || cols2[1].Type.Child.Fields[0].Name != "amount" {
		t.Fatalf("report schema was mutated: %+v", cols2)
	}
	resolved := report.Fields()
	if len(resolved) != 2 || resolved[0].Name != "id" || resolved[1].Name != "items" {
		t.Fatalf("fields=%+v", resolved)
	}
}

func TestTypeNameInvalidNestedList(t *testing.T) {
	if got := TypeName(coretypes.LogicalType{ID: coretypes.List}); got != "LIST<INVALID>" {
		t.Fatalf("got %q", got)
	}
	if got := TypeName(coretypes.LogicalType{}); got != "INVALID" {
		t.Fatalf("got %q", got)
	}
}
