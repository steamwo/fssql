package planner

import (
	"context"
	"testing"

	"github.com/steamwo/fssql/internal/core/stats"
	"github.com/steamwo/fssql/internal/core/types"
	"github.com/steamwo/fssql/internal/function"
	"github.com/steamwo/fssql/internal/sql/binder"
)

func aggregatePlannerSource() *binder.BoundRelation {
	columns := []binder.RelationColumn{
		{Name: "a", Type: types.LogicalType{ID: types.BigInt}},
		{Name: "b", Type: types.LogicalType{ID: types.BigInt}, Nullable: true},
	}
	return &binder.BoundRelation{
		Kind: binder.RelationTableFunction, ID: "tablefunc:read_csv:agg-fp", Columns: columns,
		FunctionID: function.ReadCSVID, Source: "mem://agg.csv", Fingerprint: stats.DatasetFingerprint("agg-fp"),
	}
}

func aggregateColumn(index int, nullable bool) *binder.BoundExpression {
	return &binder.BoundExpression{Kind: binder.BoundColumn, Type: types.LogicalType{ID: types.BigInt}, Nullable: nullable, ColumnIndex: index}
}

func aggregateSum(child *binder.BoundExpression) *binder.BoundExpression {
	return &binder.BoundExpression{
		Kind: binder.BoundFunction, Type: types.LogicalType{ID: types.HugeInt}, Nullable: true,
		FunctionID: function.SumID, FunctionKind: function.Aggregate, Children: []*binder.BoundExpression{child},
	}
}

func aggregateCountStar() *binder.BoundExpression {
	return &binder.BoundExpression{
		Kind: binder.BoundFunction, Type: types.LogicalType{ID: types.BigInt}, Nullable: false,
		FunctionID: function.CountID, FunctionKind: function.Aggregate,
	}
}

// TestPlanStableAggregateRewritesGroupHavingProjection 验证 pre-project -> aggregate -> HAVING -> final-project 的列 provenance。
func TestPlanStableAggregateRewritesGroupHavingProjection(t *testing.T) {
	group := aggregateColumn(0, false)
	sumB := aggregateSum(aggregateColumn(1, true))
	ten := &binder.BoundExpression{Kind: binder.BoundLiteral, Type: types.LogicalType{ID: types.HugeInt}, Literal: "10"}
	having := &binder.BoundExpression{Kind: binder.BoundBinary, Type: types.LogicalType{ID: types.Boolean}, Nullable: true, Op: ">", Children: []*binder.BoundExpression{sumB, ten}}
	query := &binder.BoundQuery{
		Source: aggregatePlannerSource(), GroupBy: []*binder.BoundExpression{group}, Having: having,
		Projection: []binder.BoundSelectItem{
			{Expression: group, Output: binder.RelationColumn{Name: "a", Type: types.LogicalType{ID: types.BigInt}}},
			{Expression: sumB, Output: binder.RelationColumn{Name: "s", Type: types.LogicalType{ID: types.HugeInt}, Nullable: true}},
		},
		Output: []binder.RelationColumn{
			{Name: "a", Type: types.LogicalType{ID: types.BigInt}},
			{Name: "s", Type: types.LogicalType{ID: types.HugeInt}, Nullable: true},
		},
	}
	plan, err := NewQueryPlanner(nil, nil).PlanStable(context.Background(), query)
	if err != nil {
		t.Fatal(err)
	}
	if plan.Kind != LogicalProject || len(plan.Projection) != 2 {
		t.Fatalf("final=%+v", plan)
	}
	if plan.Projection[0].Kind != binder.BoundColumn || plan.Projection[0].ColumnIndex != 0 || plan.Projection[1].Kind != binder.BoundColumn || plan.Projection[1].ColumnIndex != 1 {
		t.Fatalf("final projection=%+v", plan.Projection)
	}
	havingPlan := plan.Children[0]
	if havingPlan.Kind != LogicalFilter || havingPlan.Predicate == nil || havingPlan.Predicate.Children[0].Kind != binder.BoundColumn || havingPlan.Predicate.Children[0].ColumnIndex != 1 {
		t.Fatalf("having=%+v", havingPlan)
	}
	agg := havingPlan.Children[0]
	if agg.Kind != LogicalAggregate || len(agg.GroupBy) != 1 || len(agg.Aggregates) != 1 || len(agg.Output) != 2 {
		t.Fatalf("aggregate=%+v", agg)
	}
	if agg.GroupBy[0].Kind != binder.BoundColumn || agg.GroupBy[0].ColumnIndex != 0 {
		t.Fatalf("group=%+v", agg.GroupBy)
	}
	if agg.Aggregates[0].FunctionID != uint16(function.SumID) || len(agg.Aggregates[0].Arguments) != 1 || agg.Aggregates[0].Arguments[0].ColumnIndex != 1 {
		t.Fatalf("aggregate spec=%+v", agg.Aggregates[0])
	}
	pre := agg.Children[0]
	if pre.Kind != LogicalProject || len(pre.Projection) != 2 || pre.Projection[0].ColumnIndex != 0 || pre.Projection[1].ColumnIndex != 1 || pre.Children[0].Kind != LogicalScan {
		t.Fatalf("pre-project=%+v", pre)
	}
}

// TestPlanStableAggregateDeduplicatesRepeatedCall 验证 SELECT/HAVING 重复使用同一 aggregate 时只创建一个 state。
func TestPlanStableAggregateDeduplicatesRepeatedCall(t *testing.T) {
	sumB := aggregateSum(aggregateColumn(1, true))
	zero := &binder.BoundExpression{Kind: binder.BoundLiteral, Type: types.LogicalType{ID: types.HugeInt}, Literal: "0"}
	query := &binder.BoundQuery{
		Source: aggregatePlannerSource(),
		Projection: []binder.BoundSelectItem{{Expression: sumB, Output: binder.RelationColumn{Name: "s", Type: sumB.Type, Nullable: true}}},
		Output: []binder.RelationColumn{{Name: "s", Type: sumB.Type, Nullable: true}},
		Having: &binder.BoundExpression{Kind: binder.BoundBinary, Type: types.LogicalType{ID: types.Boolean}, Nullable: true, Op: ">", Children: []*binder.BoundExpression{aggregateSum(aggregateColumn(1, true)), zero}},
	}
	plan, err := NewQueryPlanner(nil, nil).PlanStable(context.Background(), query)
	if err != nil {
		t.Fatal(err)
	}
	agg := plan.Children[0].Children[0]
	if agg.Kind != LogicalAggregate || len(agg.Aggregates) != 1 {
		t.Fatalf("aggregate=%+v", agg)
	}
}

// TestPlanStableCountStarNeedsNoSyntheticArgumentProjection 验证 COUNT(*) 直接消费 child，不伪造 aggregate input column。
func TestPlanStableCountStarNeedsNoSyntheticArgumentProjection(t *testing.T) {
	count := aggregateCountStar()
	query := &binder.BoundQuery{
		Source: aggregatePlannerSource(),
		Projection: []binder.BoundSelectItem{{Expression: count, Output: binder.RelationColumn{Name: "count_star", Type: count.Type}}},
		Output: []binder.RelationColumn{{Name: "count_star", Type: count.Type}},
	}
	plan, err := NewQueryPlanner(nil, nil).PlanStable(context.Background(), query)
	if err != nil {
		t.Fatal(err)
	}
	if plan.Kind != LogicalProject || plan.Children[0].Kind != LogicalAggregate {
		t.Fatalf("plan=%+v", plan)
	}
	agg := plan.Children[0]
	if len(agg.GroupBy) != 0 || len(agg.Aggregates) != 1 || len(agg.Aggregates[0].Arguments) != 0 || agg.Children[0].Kind != LogicalScan {
		t.Fatalf("aggregate=%+v", agg)
	}
}

// TestPlanStableAggregateOrderByHiddenCall 验证非输出 aggregate ORDER BY 通过 hidden final projection 进入 Sort，再从 result ABI 删除。
func TestPlanStableAggregateOrderByHiddenCall(t *testing.T) {
	group := aggregateColumn(0, false)
	sumB := aggregateSum(aggregateColumn(1, true))
	query := &binder.BoundQuery{
		Source: aggregatePlannerSource(), GroupBy: []*binder.BoundExpression{group},
		Projection: []binder.BoundSelectItem{{Expression: group, Output: binder.RelationColumn{Name: "a", Type: group.Type}}},
		Output: []binder.RelationColumn{{Name: "a", Type: group.Type}},
		OrderBy: []binder.BoundOrderTerm{{Expression: sumB, Descending: true}},
	}
	plan, err := NewQueryPlanner(nil, nil).PlanStable(context.Background(), query)
	if err != nil {
		t.Fatal(err)
	}
	if plan.Kind != LogicalProject || plan.Children[0].Kind != LogicalSort || len(plan.Children[0].Output) != 2 {
		t.Fatalf("plan=%+v", plan)
	}
	sortPlan := plan.Children[0]
	if len(sortPlan.Sort) != 1 || sortPlan.Sort[0].Expr.ColumnIndex != 1 || sortPlan.Children[0].Kind != LogicalProject {
		t.Fatalf("sort=%+v", sortPlan)
	}
}
