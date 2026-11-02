package planner

import (
	"context"
	"testing"

	"github.com/steamwo/fssql/internal/core/stats"
	"github.com/steamwo/fssql/internal/core/types"
	"github.com/steamwo/fssql/internal/function"
	"github.com/steamwo/fssql/internal/sql/binder"
)

func stablePlannerQuery() *binder.BoundQuery {
	column := binder.RelationColumn{Name: "id", Type: types.LogicalType{ID: types.BigInt}}
	expression := &binder.BoundExpression{Kind: binder.BoundColumn, Type: column.Type, ColumnIndex: 0}
	return &binder.BoundQuery{
		Source: &binder.BoundRelation{
			Kind: binder.RelationTableFunction, ID: "tablefunc:read_csv:fp", Columns: []binder.RelationColumn{column},
			FunctionID: function.ReadCSVID, Source: "mem://rows.csv", Fingerprint: stats.DatasetFingerprint("fp"),
		},
		Projection: []binder.BoundSelectItem{{Expression: expression, Output: column}},
		Output:     []binder.RelationColumn{column},
	}
}

func plannerInteger(value string) *binder.BoundExpression {
	return &binder.BoundExpression{Kind: binder.BoundLiteral, Type: types.LogicalType{ID: types.Integer}, Literal: value}
}

// TestPlanStableOrdersDistinctBeforeLimit 验证 SQL output modifier 顺序为 Project -> DISTINCT -> LIMIT。
func TestPlanStableOrdersDistinctBeforeLimit(t *testing.T) {
	query := stablePlannerQuery()
	query.Distinct = true
	query.Limit = plannerInteger("2")
	query.Offset = plannerInteger("1")
	plan, err := NewQueryPlanner(nil, nil).PlanStable(context.Background(), query)
	if err != nil {
		t.Fatal(err)
	}
	if plan.Kind != LogicalLimit || plan.Limit != 2 || plan.Offset != 1 || len(plan.Children) != 1 {
		t.Fatalf("limit=%+v", plan)
	}
	distinct := plan.Children[0]
	if distinct.Kind != LogicalDistinct || len(distinct.Children) != 1 {
		t.Fatalf("distinct=%+v", distinct)
	}
	project := distinct.Children[0]
	if project.Kind != LogicalProject || len(project.Children) != 1 || project.Children[0].Kind != LogicalScan {
		t.Fatalf("project=%+v", project)
	}
}

// TestPlanStablePreservesLimitZeroAfterDistinct 验证 DISTINCT + LIMIT 0 仍保留精确零行语义。
func TestPlanStablePreservesLimitZeroAfterDistinct(t *testing.T) {
	query := stablePlannerQuery()
	query.Distinct = true
	query.Limit = plannerInteger("0")
	plan, err := NewQueryPlanner(nil, nil).PlanStable(context.Background(), query)
	if err != nil {
		t.Fatal(err)
	}
	if plan.Kind != LogicalLimit || plan.Limit != 0 || plan.Children[0].Kind != LogicalDistinct {
		t.Fatalf("plan=%+v", plan)
	}
}

// TestPlanStableOffsetZeroWithoutLimitIsNoOp 验证 OFFSET 0 不制造多余 operator，但 DISTINCT 仍保留。
func TestPlanStableOffsetZeroWithoutLimitIsNoOp(t *testing.T) {
	query := stablePlannerQuery()
	query.Distinct = true
	query.Offset = plannerInteger("0")
	plan, err := NewQueryPlanner(nil, nil).PlanStable(context.Background(), query)
	if err != nil {
		t.Fatal(err)
	}
	if plan.Kind != LogicalDistinct {
		t.Fatalf("plan=%+v", plan)
	}
}

// TestPlanStableLeavesOrderByFailClosed 验证尚未接通的 ORDER BY 不会因统一入口而被静默跳过。
func TestPlanStableLeavesOrderByFailClosed(t *testing.T) {
	query := stablePlannerQuery()
	query.OrderBy = []binder.BoundOrderTerm{{Expression: query.Projection[0].Expression}}
	if _, err := NewQueryPlanner(nil, nil).PlanStable(context.Background(), query); err == nil {
		t.Fatal("expected ORDER BY lowering rejection")
	}
}
