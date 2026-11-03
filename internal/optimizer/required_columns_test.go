package optimizer

import (
	"context"
	"reflect"
	"testing"

	"github.com/steamwo/fssql/internal/core/types"
	"github.com/steamwo/fssql/internal/function"
	"github.com/steamwo/fssql/internal/planner"
	"github.com/steamwo/fssql/internal/sql/binder"
)

func TestAggregateAndComputedProjectionKeepOnlyRequiredScanColumns(t *testing.T) {
	column := func(index int) *binder.BoundExpression {
		return &binder.BoundExpression{Kind: binder.BoundColumn, ColumnIndex: index, Type: types.LogicalType{ID: types.BigInt}}
	}
	for _, aggregate := range []bool{false, true} {
		scan, _ := testScan("required", 1000)
		filter := &planner.LogicalPlan{Kind: planner.LogicalFilter, Children: []*planner.LogicalPlan{scan}, Predicate: &binder.BoundExpression{Kind: binder.BoundBinary, Op: ">=", Children: []*binder.BoundExpression{column(15), {Kind: binder.BoundLiteral, Literal: "9000"}}}}
		var root *planner.LogicalPlan
		want := []int{2, 15}
		if aggregate {
			root = &planner.LogicalPlan{Kind: planner.LogicalAggregate, Children: []*planner.LogicalPlan{filter}, Aggregates: []planner.AggregateSpec{{FunctionID: uint16(function.CountID)}}}
			want = []int{15}
		} else {
			root = &planner.LogicalPlan{Kind: planner.LogicalProject, Children: []*planner.LogicalPlan{filter}, Projection: []*binder.BoundExpression{{Kind: binder.BoundBinary, Op: "+", Children: []*binder.BoundExpression{column(2), {Kind: binder.BoundLiteral, Literal: "1"}}}}}
		}
		physical, err := (&Optimizer{}).Optimize(context.Background(), root)
		if err != nil {
			t.Fatal(err)
		}
		got := physical.Children[0].Children[0].Scan.Pushdown.Projection
		if !reflect.DeepEqual(got, want) {
			t.Fatalf("aggregate=%v columns=%v want=%v", aggregate, got, want)
		}
	}
}
