package profile

import (
	"strings"
	"testing"
)

func TestScanProfileValidateCloneAndExplain(t *testing.T) {
	p := ScanProfile{
		MatchedFiles:       3,
		DatasetFingerprint: "fp",
		ProjectedColumns:   []string{"id", "amount"},
		PushedFilters:      []string{"id > 10"},
		RowGroupsTotal:     4,
		RowGroupsPruned:    1,
		RowGroupsScanned:   3,
		Requests:           12,
		PlannedBytes:       1000,
		TransferredBytes:   900,
		UsefulBytes:        700,
		RowsProduced:       42,
		PagesRead:          8,
	}
	if err := p.Validate(); err != nil {
		t.Fatal(err)
	}
	clone := p.Clone()
	clone.ProjectedColumns[0] = "changed"
	clone.PushedFilters[0] = "changed"
	if p.ProjectedColumns[0] != "id" || p.PushedFilters[0] != "id > 10" {
		t.Fatal("clone mutated source slices")
	}
	plan := p.Explain(false)
	if strings.Contains(plan, "runtime:") {
		t.Fatalf("EXPLAIN unexpectedly contains runtime metrics: %s", plan)
	}
	for _, want := range []string{
		"matched_files: 3",
		"projected_columns: [id, amount]",
		"pushed_filters: [id > 10]",
		"rowgroups: total=4 pruned=1 scanned=3",
		"bytes: planned=1000 transferred=900 useful=700",
	} {
		if !strings.Contains(plan, want) {
			t.Fatalf("EXPLAIN missing %q: %s", want, plan)
		}
	}
	analyze := p.Explain(true)
	if !strings.Contains(analyze, "runtime: requests=12 pages_read=8 rows_produced=42") {
		t.Fatalf("EXPLAIN ANALYZE missing runtime metrics: %s", analyze)
	}
}

func TestScanProfileRejectsInvalidAccounting(t *testing.T) {
	cases := []ScanProfile{
		{MatchedFiles: -1},
		{RowGroupsTotal: 1, RowGroupsPruned: 1, RowGroupsScanned: 1},
		{TransferredBytes: 5, UsefulBytes: 6},
	}
	for i, p := range cases {
		if err := p.Validate(); err == nil {
			t.Fatalf("case %d unexpectedly valid: %+v", i, p)
		}
	}
}
