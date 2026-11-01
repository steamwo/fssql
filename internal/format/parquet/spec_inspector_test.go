package parquet

import (
	"context"
	"testing"

	"github.com/steamwo/fssql/internal/introspection"
	"github.com/steamwo/fssql/internal/storage/fileset"
)

func TestSpecInspectorMatchesDirectBindBackedInspection(t *testing.T) {
	driver := newBindDriver(t, map[string][]byte{
		"mem://x/a.parquet": parquetFile(metadataWithField("id")),
	})
	binder, _, scope := newTestBinder(t, driver)
	base := SourceBindOptions{FileSet: fileset.PlanOptions{MaxFiles: 10}}
	adapter, err := NewSpecInspector(binder, base)
	if err != nil {
		t.Fatal(err)
	}
	direct, err := binder.InspectSource(context.Background(), "mem://x/*.parquet", base)
	if err != nil {
		t.Fatal(err)
	}
	throughSpec, err := adapter.InspectSource(context.Background(), introspection.SourceSpec{Format: introspection.SourceFormatParquet, Pattern: "mem://x/*.parquet"})
	if err != nil {
		t.Fatal(err)
	}
	gotColumns, wantColumns := throughSpec.Columns(), direct.Columns()
	if len(gotColumns) != len(wantColumns) {
		t.Fatalf("columns len=%d want=%d", len(gotColumns), len(wantColumns))
	}
	for i := range gotColumns {
		if gotColumns[i].Name != wantColumns[i].Name || gotColumns[i].TypeName != wantColumns[i].TypeName {
			t.Fatalf("column %d got=%+v want=%+v", i, gotColumns[i], wantColumns[i])
		}
	}
	if throughSpec.DatasetFingerprint != direct.DatasetFingerprint ||
		throughSpec.SchemaSource != direct.SchemaSource ||
		throughSpec.Confidence != direct.Confidence ||
		throughSpec.MatchedFiles != direct.MatchedFiles ||
		throughSpec.SchemaFiles != direct.SchemaFiles ||
		throughSpec.UnionByName != direct.UnionByName ||
		throughSpec.FullValidation != direct.FullValidation {
		t.Fatalf("report=%+v direct=%+v", throughSpec, direct)
	}
	if scope.Used() != 0 {
		t.Fatalf("managed bytes leaked: %d", scope.Used())
	}
}

func TestSpecInspectorMapsExplicitFullValidationToRealBind(t *testing.T) {
	driver := newBindDriver(t, map[string][]byte{
		"mem://x/a.parquet": parquetFile(metadataWithField("id")),
		"mem://x/b.parquet": parquetFile(metadataWithField("id")),
	})
	binder, _, scope := newTestBinder(t, driver)
	adapter, err := NewSpecInspector(binder, SourceBindOptions{FileSet: fileset.PlanOptions{MaxFiles: 10}})
	if err != nil {
		t.Fatal(err)
	}
	report, err := adapter.InspectSource(context.Background(), introspection.SourceSpec{
		Format:         introspection.SourceFormatParquet,
		Pattern:        "mem://x/*.parquet",
		FullValidation: true,
	})
	if err != nil {
		t.Fatal(err)
	}
	if !report.FullValidation || report.SchemaFiles != 2 || report.MatchedFiles != 2 {
		t.Fatalf("report=%+v", report)
	}
	if scope.Used() != 0 {
		t.Fatalf("managed bytes leaked: %d", scope.Used())
	}
}
