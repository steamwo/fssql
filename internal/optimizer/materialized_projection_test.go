package optimizer

import (
	"context"
	"testing"

	"github.com/steamwo/fssql/internal/core/types"
	"github.com/steamwo/fssql/internal/function"
	"github.com/steamwo/fssql/internal/planner"
	"github.com/steamwo/fssql/internal/sql/binder"
)

func TestSharedMaterializationKeepsFullJoinSchema(t *testing.T) {
	left, _ := testScan("materialized-left", 10)
	right, _ := testScan("materialized-right", 10)
	column := planner.Column{Name: "id", Type: types.LogicalType{ID: types.BigInt}}
	left.Output, right.Output = []planner.Column{column}, []planner.Column{column}
	ref := func(index int) *binder.BoundExpression {
		return &binder.BoundExpression{Kind: binder.BoundColumn, Type: column.Type, ColumnIndex: index}
	}
	join := &planner.LogicalPlan{Kind: planner.LogicalJoin, Join: planner.JoinInner, Children: []*planner.LogicalPlan{left, right}, Output: []planner.Column{column, column}, Predicate: &binder.BoundExpression{Kind: binder.BoundBinary, Op: "=", Type: types.LogicalType{ID: types.Boolean}, Children: []*binder.BoundExpression{ref(0), ref(1)}}}
	marker := &binder.BoundExpression{Kind: binder.BoundFunction, FunctionID: function.CTEMarkerID, FunctionKind: function.Scalar, Type: types.LogicalType{ID: types.Boolean}, Children: []*binder.BoundExpression{{Kind: binder.BoundLiteral, Type: types.LogicalType{ID: types.Varchar}, Literal: "shared-join"}}}
	cache := &planner.LogicalPlan{Kind: planner.LogicalSort, Children: []*planner.LogicalPlan{join}, Output: join.Output, Sort: []planner.SortKey{{Expr: marker}}}
	project := &planner.LogicalPlan{Kind: planner.LogicalProject, Children: []*planner.LogicalPlan{cache}, Output: []planner.Column{column}, Projection: []*binder.BoundExpression{ref(0)}}
	physical, err := (&Optimizer{}).Optimize(context.Background(), project)
	if err != nil {
		t.Fatal(err)
	}
	mask := physical.Children[0].Children[0].RequiredOutput
	for i, needed := range mask {
		if !needed {
			t.Fatalf("cache column %d pruned for another reader: %v", i, mask)
		}
	}
}
