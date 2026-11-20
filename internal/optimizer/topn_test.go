package optimizer

import (
	"context"
	"math"
	"testing"

	"github.com/steamwo/fssql/internal/core/types"
	"github.com/steamwo/fssql/internal/planner"
	"github.com/steamwo/fssql/internal/sql/binder"
)

func TestTopNBoundsAreLocalToEachSort(t *testing.T) {
	typ := types.LogicalType{ID: types.BigInt}
	key := &binder.BoundExpression{Kind: binder.BoundColumn, Type: typ, ColumnIndex: 0}
	columns := []planner.Column{{Name: "id", Type: typ}}
	left, _ := testScan("left", 100)
	right, _ := testScan("right", 100)
	makeSort := func(child *planner.LogicalPlan) *planner.LogicalPlan {
		return &planner.LogicalPlan{Kind: planner.LogicalSort, Children: []*planner.LogicalPlan{child}, Output: columns, Sort: []planner.SortKey{{Expr: key}}}
	}
	leftSort := makeSort(left)
	projection := &planner.LogicalPlan{Kind: planner.LogicalProject, Children: []*planner.LogicalPlan{leftSort}, Output: columns, Projection: []*binder.BoundExpression{key}}
	leftLimit := &planner.LogicalPlan{Kind: planner.LogicalLimit, Children: []*planner.LogicalPlan{projection}, Output: columns, Limit: 11, Offset: 3}
	rightLimit := &planner.LogicalPlan{Kind: planner.LogicalLimit, Children: []*planner.LogicalPlan{makeSort(right)}, Output: columns, Limit: 17}
	union := &planner.LogicalPlan{Kind: planner.LogicalUnionAll, Children: []*planner.LogicalPlan{leftLimit, rightLimit}, Output: columns}
	root := &planner.LogicalPlan{Kind: planner.LogicalLimit, Children: []*planner.LogicalPlan{makeSort(union)}, Limit: 5, Offset: 2}
	physical, err := (&Optimizer{}).Optimize(context.Background(), root)
	if err != nil {
		t.Fatal(err)
	}
	outer := physical.Children[0]
	branches := outer.Children[0].Children
	if got := [3]uint64{outer.TopNRows, branches[0].Children[0].Children[0].TopNRows, branches[1].Children[0].TopNRows}; got != [3]uint64{7, 14, 17} {
		t.Fatalf("sort bounds=%v want [7 14 17]", got)
	}
}

func TestTopNBoundStopsAtUnsafeOperators(t *testing.T) {
	for _, tc := range []struct {
		name          string
		limit, offset uint64
		intermediate  planner.LogicalKind
		marker        bool
	}{
		{name: "zero", limit: 0},
		{name: "unlimited", limit: planner.UnlimitedRows, offset: 10},
		{name: "overflow", limit: 3, offset: math.MaxUint64 - 1},
		{name: "filter", limit: 3, intermediate: planner.LogicalFilter},
		{name: "computed projection", limit: 3, intermediate: planner.LogicalProject},
		{name: "sort marker", limit: 3, marker: true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			scan, _ := testScan("scan", 100)
			key := &binder.BoundExpression{Kind: binder.BoundColumn, Type: types.LogicalType{ID: types.BigInt}, ColumnIndex: 0}
			if tc.marker {
				key.Kind = binder.BoundFunction
			}
			sorted := &planner.LogicalPlan{Kind: planner.LogicalSort, Children: []*planner.LogicalPlan{scan}, Sort: []planner.SortKey{{Expr: key}}}
			tail := sorted
			switch tc.intermediate {
			case planner.LogicalFilter:
				tail = &planner.LogicalPlan{Kind: tc.intermediate, Children: []*planner.LogicalPlan{sorted}, Predicate: &binder.BoundExpression{Kind: binder.BoundLiteral, Type: types.LogicalType{ID: types.Boolean}}}
			case planner.LogicalProject:
				tail = &planner.LogicalPlan{Kind: tc.intermediate, Children: []*planner.LogicalPlan{sorted}, Projection: []*binder.BoundExpression{{Kind: binder.BoundLiteral, Type: key.Type}}}
			}
			root := &planner.LogicalPlan{Kind: planner.LogicalLimit, Children: []*planner.LogicalPlan{tail}, Limit: tc.limit, Offset: tc.offset}
			physical, err := (&Optimizer{}).Optimize(context.Background(), root)
			if err != nil {
				t.Fatal(err)
			}
			sort := physical.Children[0]
			if tail != sorted {
				sort = sort.Children[0]
			}
			if sort.TopNRows != 0 {
				t.Fatalf("unsafe sort bound=%d", sort.TopNRows)
			}
		})
	}
}
