package parquet

import (
	"context"
	"testing"

	"github.com/steamwo/fssql/internal/introspection"
)

func TestInspectAndDescribeReuseQueryBindingSchema(t *testing.T) {
	d := newBindDriver(t, map[string][]byte{
		"mem://x/a.parquet": parquetFile(metadataWithField("id")),
		"mem://x/b.parquet": parquetFile(metadataWithField("id")),
	})
	binder, cache, scope := newTestBinder(t, d)
	const pattern = "mem://x/*.parquet"

	binding, err := binder.Bind(context.Background(), pattern, SourceBindOptions{})
	if err != nil {
		t.Fatal(err)
	}
	readsAfterBind := len(d.ranges)
	if readsAfterBind != 2 {
		t.Fatalf("initial bind ranges=%d", readsAfterBind)
	}

	report, err := binder.InspectSource(context.Background(), pattern, SourceBindOptions{})
	if err != nil {
		t.Fatal(err)
	}
	if len(d.ranges) != readsAfterBind {
		t.Fatalf("inspection bypassed metadata cache: before=%d after=%d", readsAfterBind, len(d.ranges))
	}
	if !sameFields(binding.Schema().Fields, report.Fields()) {
		t.Fatalf("bind schema=%+v inspect schema=%+v", binding.Schema().Fields, report.Fields())
	}
	if report.SchemaSource != introspection.SchemaSourceFileMetadata || report.Confidence != introspection.ConfidenceExact {
		t.Fatalf("source/confidence=%s/%s", report.SchemaSource, report.Confidence)
	}
	if report.MatchedFiles != binding.MatchedFiles() || report.SchemaFiles != binding.SchemaFiles() || report.DatasetFingerprint != binding.Fingerprint() || report.UnionByName || report.FullValidation {
		t.Fatalf("report=%+v", report)
	}

	columns, describeReport, err := binder.DescribeSource(context.Background(), pattern, SourceBindOptions{})
	if err != nil {
		t.Fatal(err)
	}
	if len(columns) != 1 || columns[0].Name != "id" || columns[0].TypeName != "BIGINT" {
		t.Fatalf("describe columns=%+v", columns)
	}
	if !sameFields(binding.Schema().Fields, describeReport.Fields()) {
		t.Fatalf("describe schema=%+v bind schema=%+v", describeReport.Fields(), binding.Schema().Fields)
	}
	if len(d.ranges) != readsAfterBind {
		t.Fatalf("describe bypassed metadata cache: before=%d after=%d", readsAfterBind, len(d.ranges))
	}
	if cache.Metrics().Hits < 2 {
		t.Fatalf("cache metrics=%+v", cache.Metrics())
	}
	if scope.Used() != 0 {
		t.Fatalf("managed memory leaked: %d", scope.Used())
	}
}

func TestInspectReportsFullValidationScope(t *testing.T) {
	d := newBindDriver(t, map[string][]byte{
		"mem://x/a.parquet": parquetFile(metadataWithField("id")),
		"mem://x/b.parquet": parquetFile(metadataWithField("id")),
	})
	binder, _, scope := newTestBinder(t, d)
	report, err := binder.InspectSource(context.Background(), "mem://x/*.parquet", SourceBindOptions{ValidationMode: SchemaValidationFull})
	if err != nil {
		t.Fatal(err)
	}
	if !report.FullValidation || report.SchemaFiles != 2 || report.MatchedFiles != 2 {
		t.Fatalf("report=%+v", report)
	}
	if len(d.ranges) != 4 {
		t.Fatalf("full validation ranges=%d", len(d.ranges))
	}
	if scope.Used() != 0 {
		t.Fatalf("managed memory leaked: %d", scope.Used())
	}
}
