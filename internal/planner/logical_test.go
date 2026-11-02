package planner

import (
	"testing"

	"github.com/steamwo/fssql/internal/core/stats"
	"github.com/steamwo/fssql/internal/core/types"
	"github.com/steamwo/fssql/internal/sql/binder"
)

func TestLogicalPlanValidate(t *testing.T) {
	scan := &LogicalPlan{
		Kind: LogicalScan,
		Scan: &ScanSpec{RelationID: "r", Function: "read_parquet", Fingerprint: stats.DatasetFingerprint("fp")},
		Output: []Column{{Name: "id", Type: types.LogicalType{ID: types.BigInt}}},
	}
	pred := &binder.BoundExpression{Kind: binder.BoundLiteral, Type: types.LogicalType{ID: types.Boolean}}
	root := &LogicalPlan{Kind: LogicalFilter, Children: []*LogicalPlan{scan}, Predicate: pred, Output: scan.Output}
	if err := root.Validate(); err != nil {
		t.Fatal(err)
	}
}

func TestLogicalPlanRejectsMalformedJoin(t *testing.T) {
	s := &LogicalPlan{Kind: LogicalScan, Scan: &ScanSpec{RelationID: "r", Function: "read_csv"}}
	j := &LogicalPlan{Kind: LogicalJoin, Join: JoinLeft, Children: []*LogicalPlan{s, s}}
	if err := j.Validate(); err == nil {
		t.Fatal("expected predicate validation")
	}
}

func TestScanPushdownIsGeneric(t *testing.T) {
	p := ScanPushdown{Projection: []int{0, 2}, Limit: 5}
	if len(p.Projection) != 2 || p.Limit != 5 {
		t.Fatalf("pushdown=%+v", p)
	}
}
