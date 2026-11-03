package session

import (
	"testing"

	"github.com/steamwo/fssql/internal/core/types"
	"github.com/steamwo/fssql/internal/planner"
	"github.com/steamwo/fssql/internal/sql/binder"
)

func TestTopNRowsForPhysicalRoot(t *testing.T) {
	scan := &planner.PhysicalPlan{Kind: planner.PhysicalScan, Scan: &planner.ScanSpec{RelationID: "r", Function: "read_parquet", Source: "x", Fingerprint: "f"}}
	column := planner.Column{Name: "id", Type: types.LogicalType{ID: types.BigInt}}
	scan.Output = []planner.Column{column}
	key := &binder.BoundExpression{Kind: binder.BoundColumn, Type: column.Type, ColumnIndex: 0}
	sort := &planner.PhysicalPlan{Kind: planner.PhysicalSort, Children: []*planner.PhysicalPlan{scan}, Output: []planner.Column{column}, Sort: []planner.SortKey{{Expr: key}}}
	limit := &planner.PhysicalPlan{Kind: planner.PhysicalLimit, Children: []*planner.PhysicalPlan{sort}, Output: []planner.Column{column}, Limit: 100, Offset: 25}
	rows, ok := topNRowsForPhysicalRoot(limit)
	if !ok || rows != 125 {
		t.Fatalf("rows=%d ok=%v", rows, ok)
	}
}

func TestTopNRowsForPhysicalRootRejectsAmbiguousOrUnsafeShapes(t *testing.T) {
	sort := &planner.PhysicalPlan{Kind: planner.PhysicalSort, Children: []*planner.PhysicalPlan{{Kind: planner.PhysicalOneRow}}, Sort: []planner.SortKey{{Expr: &binder.BoundExpression{Kind: binder.BoundColumn, ColumnIndex: 0}}}}
	cases := []*planner.PhysicalPlan{
		{Kind: planner.PhysicalLimit, Children: []*planner.PhysicalPlan{sort}, Limit: planner.UnlimitedRows},
		{Kind: planner.PhysicalLimit, Children: []*planner.PhysicalPlan{sort}, Limit: 0},
		{Kind: planner.PhysicalProject, Children: []*planner.PhysicalPlan{sort}},
		{Kind: planner.PhysicalLimit, Children: []*planner.PhysicalPlan{{Kind: planner.PhysicalProject, Children: []*planner.PhysicalPlan{sort}}}, Limit: 10},
	}
	for i, plan := range cases {
		if rows, ok := topNRowsForPhysicalRoot(plan); ok || rows != 0 {
			t.Fatalf("case %d rows=%d ok=%v", i, rows, ok)
		}
	}
}
