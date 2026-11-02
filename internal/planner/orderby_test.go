package planner

import (
	"context"
	"testing"

	"github.com/steamwo/fssql/internal/core/stats"
	"github.com/steamwo/fssql/internal/core/types"
	"github.com/steamwo/fssql/internal/function"
	"github.com/steamwo/fssql/internal/sql/binder"
)

func hiddenOrderQuery() *binder.BoundQuery {
	a := binder.RelationColumn{Name: "a", Type: types.LogicalType{ID: types.BigInt}}
	b := binder.RelationColumn{Name: "b", Type: types.LogicalType{ID: types.BigInt}, Nullable: true}
	return &binder.BoundQuery{
		Source: &binder.BoundRelation{
			Kind: binder.RelationTableFunction, ID: "tablefunc:read_csv:fp-order", Columns: []binder.RelationColumn{a, b},
			FunctionID: function.ReadCSVID, Source: "mem://order.csv", Fingerprint: stats.DatasetFingerprint("fp-order"),
		},
		Projection: []binder.BoundSelectItem{{
			Expression: &binder.BoundExpression{Kind: binder.BoundColumn, Type: a.Type, ColumnIndex: 0},
			Output:     a,
		}},
		Output: []binder.RelationColumn{a},
		OrderBy: []binder.BoundOrderTerm{{
			Expression: &binder.BoundExpression{Kind: binder.BoundColumn, Type: b.Type, Nullable: true, ColumnIndex: 1},
		}},
	}
}

// TestPlanStableMaterializesHiddenOrderExpression 验证非输出 ORDER BY expression 在 sort 前 materialize、sort 后从结果 ABI 移除。
func TestPlanStableMaterializesHiddenOrderExpression(t *testing.T) {
	query := hiddenOrderQuery()
	nullsFirst := true
	query.OrderBy[0].Descending = true
	query.OrderBy[0].NullsFirst = &nullsFirst
	plan, err := NewQueryPlanner(nil, nil).PlanStable(context.Background(), query)
	if err != nil {
		t.Fatal(err)
	}
	if plan.Kind != LogicalProject || len(plan.Output) != 1 || len(plan.Projection) != 1 || plan.Projection[0].ColumnIndex != 0 {
		t.Fatalf("final project=%+v", plan)
	}
	sortPlan := plan.Children[0]
	if sortPlan.Kind != LogicalSort || len(sortPlan.Output) != 2 || len(sortPlan.Sort) != 1 {
		t.Fatalf("sort=%+v", sortPlan)
	}
	key := sortPlan.Sort[0]
	if key.Expr == nil || key.Expr.Kind != binder.BoundColumn || key.Expr.ColumnIndex != 1 || !key.Descending || key.NullsFirst == nil || !*key.NullsFirst {
		t.Fatalf("sort key=%+v", key)
	}
	materialize := sortPlan.Children[0]
	if materialize.Kind != LogicalProject || len(materialize.Projection) != 2 || len(materialize.Output) != 2 {
		t.Fatalf("hidden project=%+v", materialize)
	}
	if materialize.Children[0].Kind != LogicalScan {
		t.Fatalf("hidden child=%+v", materialize.Children[0])
	}
	// Planner 不得回写 Binder IR；hidden column 只存在于本次 LogicalPlan。
	if len(query.Projection) != 1 || len(query.Output) != 1 {
		t.Fatalf("BoundQuery mutated projection=%d output=%d", len(query.Projection), len(query.Output))
	}
}

// TestPlanStableDistinctRejectsHiddenOrderKey 验证 DISTINCT 不会把非输出 sort key 偷偷加入去重 key 集合。
func TestPlanStableDistinctRejectsHiddenOrderKey(t *testing.T) {
	query := hiddenOrderQuery()
	query.Distinct = true
	if _, err := NewQueryPlanner(nil, nil).PlanStable(context.Background(), query); err == nil {
		t.Fatal("expected DISTINCT + non-output ORDER BY rejection")
	}
}

// TestPlanStableModifierOrderIncludesSort 验证 DISTINCT -> ORDER BY -> LIMIT 的 output modifier 顺序。
func TestPlanStableModifierOrderIncludesSort(t *testing.T) {
	query := stablePlannerQuery()
	query.Distinct = true
	query.OrderBy = []binder.BoundOrderTerm{{Expression: query.Projection[0].Expression, UsesOutput: true, OutputIndex: 0}}
	query.Limit = plannerInteger("3")
	plan, err := NewQueryPlanner(nil, nil).PlanStable(context.Background(), query)
	if err != nil {
		t.Fatal(err)
	}
	if plan.Kind != LogicalLimit || plan.Children[0].Kind != LogicalSort || plan.Children[0].Children[0].Kind != LogicalDistinct || plan.Children[0].Children[0].Children[0].Kind != LogicalProject {
		t.Fatalf("modifier chain=%+v", plan)
	}
}
