package optimizer

import (
	"context"
	"testing"

	"github.com/steamwo/fssql/internal/core/types"
	"github.com/steamwo/fssql/internal/planner"
	"github.com/steamwo/fssql/internal/sql/binder"
)

func TestProjectionPushdownCrossesResidualFilterAndKeepsFilterColumns(t *testing.T) {
	scan, _ := testScan("projection-filter", 1000)
	filterColumn := &binder.BoundExpression{Kind: binder.BoundColumn, ColumnIndex: 4, Type: types.LogicalType{ID: types.BigInt}}
	literal := &binder.BoundExpression{Kind: binder.BoundLiteral, Literal: "10", Type: types.LogicalType{ID: types.Integer}}
	predicate := &binder.BoundExpression{Kind: binder.BoundBinary, Op: "=", Type: types.LogicalType{ID: types.Boolean}, Children: []*binder.BoundExpression{filterColumn, literal}}
	filter := &planner.LogicalPlan{Kind: planner.LogicalFilter, Children: []*planner.LogicalPlan{scan}, Predicate: predicate}
	projectColumn := &binder.BoundExpression{Kind: binder.BoundColumn, ColumnIndex: 1, Type: types.LogicalType{ID: types.BigInt}}
	project := &planner.LogicalPlan{Kind: planner.LogicalProject, Children: []*planner.LogicalPlan{filter}, Projection: []*binder.BoundExpression{projectColumn}}

	physical, err := (&Optimizer{}).Optimize(context.Background(), project)
	if err != nil {
		t.Fatal(err)
	}
	physicalScan := physical.Children[0].Children[0]
	got := physicalScan.Scan.Pushdown.Projection
	if len(got) != 2 || got[0] != 1 || got[1] != 4 {
		t.Fatalf("projection=%v want [1 4]", got)
	}
	if len(physicalScan.Scan.Pushdown.Filters) != 1 {
		t.Fatalf("filter candidates=%d", len(physicalScan.Scan.Pushdown.Filters))
	}
}

func TestProjectionPushdownKeepsColumnsFromUnsupportedResidualPredicate(t *testing.T) {
	scan, _ := testScan("projection-or", 1000)
	leftColumn := &binder.BoundExpression{Kind: binder.BoundColumn, ColumnIndex: 3, Type: types.LogicalType{ID: types.BigInt}}
	rightColumn := &binder.BoundExpression{Kind: binder.BoundColumn, ColumnIndex: 5, Type: types.LogicalType{ID: types.BigInt}}
	predicate := &binder.BoundExpression{Kind: binder.BoundBinary, Op: "OR", Type: types.LogicalType{ID: types.Boolean}, Children: []*binder.BoundExpression{leftColumn, rightColumn}}
	filter := &planner.LogicalPlan{Kind: planner.LogicalFilter, Children: []*planner.LogicalPlan{scan}, Predicate: predicate}
	projectColumn := &binder.BoundExpression{Kind: binder.BoundColumn, ColumnIndex: 1, Type: types.LogicalType{ID: types.BigInt}}
	project := &planner.LogicalPlan{Kind: planner.LogicalProject, Children: []*planner.LogicalPlan{filter}, Projection: []*binder.BoundExpression{projectColumn}}

	physical, err := (&Optimizer{}).Optimize(context.Background(), project)
	if err != nil {
		t.Fatal(err)
	}
	got := physical.Children[0].Children[0].Scan.Pushdown.Projection
	if len(got) != 3 || got[0] != 1 || got[1] != 3 || got[2] != 5 {
		t.Fatalf("projection=%v want [1 3 5]", got)
	}
}

func TestLimitPushdownCrossesOnlyPureColumnProject(t *testing.T) {
	scan, _ := testScan("limit-project", 1000)
	column := &binder.BoundExpression{Kind: binder.BoundColumn, ColumnIndex: 2, Type: types.LogicalType{ID: types.BigInt}}
	project := &planner.LogicalPlan{Kind: planner.LogicalProject, Children: []*planner.LogicalPlan{scan}, Projection: []*binder.BoundExpression{column}}
	limit := &planner.LogicalPlan{Kind: planner.LogicalLimit, Children: []*planner.LogicalPlan{project}, Limit: 100}

	physical, err := (&Optimizer{}).Optimize(context.Background(), limit)
	if err != nil {
		t.Fatal(err)
	}
	physicalScan := physical.Children[0].Children[0]
	if physicalScan.Scan.Pushdown.Limit != 100 {
		t.Fatalf("limit candidate=%d", physicalScan.Scan.Pushdown.Limit)
	}
	if got := physicalScan.Scan.Pushdown.Projection; len(got) != 1 || got[0] != 2 {
		t.Fatalf("projection=%v", got)
	}
}

func TestLimitPushdownDoesNotCrossFilterOrComputedProject(t *testing.T) {
	predicate := &binder.BoundExpression{Kind: binder.BoundLiteral, Type: types.LogicalType{ID: types.Boolean}, Literal: "true"}
	for name, child := range map[string]*planner.LogicalPlan{
		"filter": func() *planner.LogicalPlan {
			scan, _ := testScan("limit-filter", 1000)
			return &planner.LogicalPlan{Kind: planner.LogicalFilter, Children: []*planner.LogicalPlan{scan}, Predicate: predicate}
		}(),
		"computed-project": func() *planner.LogicalPlan {
			scan, _ := testScan("limit-computed", 1000)
			literal := &binder.BoundExpression{Kind: binder.BoundLiteral, Type: types.LogicalType{ID: types.Integer}, Literal: "1"}
			return &planner.LogicalPlan{Kind: planner.LogicalProject, Children: []*planner.LogicalPlan{scan}, Projection: []*binder.BoundExpression{literal}}
		}(),
	} {
		t.Run(name, func(t *testing.T) {
			limit := &planner.LogicalPlan{Kind: planner.LogicalLimit, Children: []*planner.LogicalPlan{child}, Limit: 10}
			physical, err := (&Optimizer{}).Optimize(context.Background(), limit)
			if err != nil {
				t.Fatal(err)
			}
			var scan *planner.PhysicalPlan
			if physical.Children[0].Kind == planner.PhysicalScan {
				scan = physical.Children[0]
			} else {
				scan = physical.Children[0].Children[0]
			}
			if scan.Scan.Pushdown.Limit != 0 {
				t.Fatalf("unsafe limit candidate=%d", scan.Scan.Pushdown.Limit)
			}
		})
	}
}
