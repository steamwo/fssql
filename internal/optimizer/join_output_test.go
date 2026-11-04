package optimizer

import (
	"reflect"
	"testing"

	"github.com/steamwo/fssql/internal/core/types"
	"github.com/steamwo/fssql/internal/planner"
	"github.com/steamwo/fssql/internal/sql/binder"
)

func TestJoinOutputRequirementsPreservePredicateAndParentInputs(t *testing.T) {
	for _, kind := range []planner.JoinType{planner.JoinInner, planner.JoinLeft, planner.JoinRight, planner.JoinFull, planner.JoinSemi, planner.JoinAnti} {
		t.Run(string(rune('0'+kind)), func(t *testing.T) {
			left, _ := testScan("left", 100)
			right, _ := testScan("right", 100)
			ts := types.LogicalType{ID: types.BigInt}
			cols := []planner.Column{{Name: "key", Type: ts}, {Name: "value", Type: ts}, {Name: "other", Type: ts}}
			left.Output = cols
			right.Output = cols
			column := func(i int) *binder.BoundExpression {
				return &binder.BoundExpression{Kind: binder.BoundColumn, ColumnIndex: i, Type: ts}
			}
			pred := &binder.BoundExpression{Kind: binder.BoundBinary, Op: "=", Type: types.LogicalType{ID: types.Boolean}, Children: []*binder.BoundExpression{column(0), column(3)}}
			output := append(append([]planner.Column(nil), cols...), cols...)
			if kind == planner.JoinSemi || kind == planner.JoinAnti {
				output = cols
			}
			join := &planner.LogicalPlan{Kind: planner.LogicalJoin, Join: kind, Children: []*planner.LogicalPlan{left, right}, Output: output, Predicate: pred}
			// Parent WHERE and ORDER BY need columns beyond the projected column.
			filter := &planner.LogicalPlan{Kind: planner.LogicalFilter, Children: []*planner.LogicalPlan{join}, Predicate: column(2)}
			sort := &planner.LogicalPlan{Kind: planner.LogicalSort, Children: []*planner.LogicalPlan{filter}, Sort: []planner.SortKey{{Expr: column(1)}}}
			(&Optimizer{}).markRequiredScanColumns(sort, map[int]struct{}{1: {}})
			want := make([]bool, len(output))
			want[1] = true
			want[2] = true
			if !reflect.DeepEqual(join.RequiredOutput, want) {
				t.Fatalf("mask=%v want=%v", join.RequiredOutput, want)
			}
			if got := left.Scan.Pushdown.Projection; !reflect.DeepEqual(got, []int{0, 1, 2}) {
				t.Fatalf("left inputs=%v", got)
			}
			if got := right.Scan.Pushdown.Projection; !reflect.DeepEqual(got, []int{0}) {
				t.Fatalf("right inputs=%v", got)
			}
			// COUNT(*) needs cardinality only; equijoin keys stay scan inputs.
			(&Optimizer{}).markRequiredScanColumns(join, map[int]struct{}{})
			if !reflect.DeepEqual(join.RequiredOutput, make([]bool, len(output))) {
				t.Fatalf("count mask=%v", join.RequiredOutput)
			}
		})
	}
}

func TestUnknownJoinShapeKeepsAllOutputColumns(t *testing.T) {
	left, _ := testScan("left", 100)
	right, _ := testScan("right", 100)
	join := &planner.LogicalPlan{Kind: planner.LogicalJoin, Children: []*planner.LogicalPlan{left, right}}
	(&Optimizer{}).markRequiredScanColumns(join, map[int]struct{}{})
	if join.RequiredOutput != nil {
		t.Fatalf("unknown output pruned: %v", join.RequiredOutput)
	}
}
