package parquet

import (
	"context"
	"strings"
	"testing"

	coretypes "github.com/steamwo/fssql/internal/core/types"
	"github.com/steamwo/fssql/internal/format/scan"
)

func TestBuildScanProfileAggregatesPhase1Evidence(t *testing.T) {
	d := newBindDriver(t, map[string][]byte{
		"mem://x/a.parquet": parquetFile(metadataWithField("id")),
		"mem://x/b.parquet": parquetFile(metadataWithField("id")),
	})
	binder, _, scope := newTestBinder(t, d)
	binding, err := binder.Bind(context.Background(), "mem://x/*.parquet", SourceBindOptions{})
	if err != nil {
		t.Fatal(err)
	}
	filter := scan.Filter{
		Column: "id",
		Op:     scan.GreaterThan,
		Value:  scan.Literal{Type: coretypes.LogicalType{ID: coretypes.BigInt}, Int64: 10},
	}
	p, err := BuildScanProfile(ScanProfileInput{
		Binding:          binding,
		ProjectedColumns: []string{"id"},
		Filters:          []scan.Filter{filter},
		RowGroupsTotal:   2,
		FooterMetrics: []FooterMetrics{
			{Requests: 2, PlannedBytes: 20, TransferredBytes: 20, UsefulBytes: 12},
		},
		RowGroupMetrics: []RowGroupScanMetrics{
			{RowGroupsPruned: 1},
			{RowsProduced: 3, Requests: 4, PlannedBytes: 80, TransferredBytes: 70, UsefulBytes: 40, PagesRead: 2},
		},
	})
	if err != nil {
		t.Fatal(err)
	}
	if p.MatchedFiles != 2 || p.DatasetFingerprint != binding.Fingerprint() {
		t.Fatalf("binding evidence=%+v", p)
	}
	if len(p.ProjectedColumns) != 1 || p.ProjectedColumns[0] != "id" {
		t.Fatalf("projection=%v", p.ProjectedColumns)
	}
	if len(p.PushedFilters) != 1 || p.PushedFilters[0] != "id > 10" {
		t.Fatalf("filters=%v", p.PushedFilters)
	}
	if p.RowGroupsTotal != 2 || p.RowGroupsPruned != 1 || p.RowGroupsScanned != 1 {
		t.Fatalf("rowgroups=%+v", p)
	}
	if p.Requests != 6 || p.PlannedBytes != 100 || p.TransferredBytes != 90 || p.UsefulBytes != 52 || p.RowsProduced != 3 || p.PagesRead != 2 {
		t.Fatalf("metrics=%+v", p)
	}
	for _, want := range []string{"matched_files: 2", "projected_columns: [id]", "pushed_filters: [id > 10]", "rowgroups: total=2 pruned=1 scanned=1", "bytes: planned=100 transferred=90 useful=52"} {
		if !strings.Contains(p.Explain(false), want) {
			t.Fatalf("EXPLAIN missing %q: %s", want, p.Explain(false))
		}
	}
	if scope.Used() != 0 {
		t.Fatalf("managed memory leaked: %d", scope.Used())
	}
}

func TestBuildScanProfileRejectsImpossibleRowGroupAccounting(t *testing.T) {
	d := newBindDriver(t, map[string][]byte{"mem://x/a.parquet": parquetFile(metadataWithField("id"))})
	binder, _, _ := newTestBinder(t, d)
	binding, err := binder.Bind(context.Background(), "mem://x/*.parquet", SourceBindOptions{})
	if err != nil {
		t.Fatal(err)
	}
	_, err = BuildScanProfile(ScanProfileInput{
		Binding:        binding,
		RowGroupsTotal: 1,
		RowGroupMetrics: []RowGroupScanMetrics{
			{RowGroupsPruned: 1},
			{},
		},
	})
	if err == nil {
		t.Fatal("expected invalid row group accounting")
	}
}
