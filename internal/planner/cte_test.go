package planner

import (
	"context"
	"testing"

	"github.com/steamwo/fssql/internal/core/stats"
	"github.com/steamwo/fssql/internal/core/types"
	"github.com/steamwo/fssql/internal/function"
	"github.com/steamwo/fssql/internal/sql/binder"
)

func boundReaderQueryForCTE(name string) *binder.BoundQuery {
	column := binder.RelationColumn{Name: name, Type: types.LogicalType{ID: types.BigInt}}
	expr := &binder.BoundExpression{Kind: binder.BoundColumn, Type: column.Type, ColumnIndex: 0}
	return &binder.BoundQuery{
		Source: &binder.BoundRelation{
			Kind: binder.RelationTableFunction, ID: "tablefunc:read_csv:cte-fp", Columns: []binder.RelationColumn{column},
			FunctionID: function.ReadCSVID, Source: "mem://cte.csv", Fingerprint: stats.DatasetFingerprint("cte-fp"),
		},
		Projection: []binder.BoundSelectItem{{Expression: expr, Output: column}},
		Output:     []binder.RelationColumn{column},
	}
}

// TestPlanStableCTEUsesBoundQueryProvenance 验证 Planner 不重新 parse CTE SQL，而直接递归规划 Binder 保存的 BoundQuery。
func TestPlanStableCTEUsesBoundQueryProvenance(t *testing.T) {
	cte := boundReaderQueryForCTE("renamed")
	column := binder.RelationColumn{Name: "renamed", Type: types.LogicalType{ID: types.BigInt}}
	outerExpr := &binder.BoundExpression{Kind: binder.BoundColumn, Type: column.Type, ColumnIndex: 0}
	outer := &binder.BoundQuery{
		Source: &binder.BoundRelation{Kind: binder.RelationCTE, ID: "cte:x", Columns: []binder.RelationColumn{column}, CTEQuery: cte},
		Projection: []binder.BoundSelectItem{{Expression: outerExpr, Output: column}},
		Output:     []binder.RelationColumn{column},
	}
	plan, err := NewQueryPlanner(nil, nil).PlanStable(context.Background(), outer)
	if err != nil {
		t.Fatal(err)
	}
	if plan.Kind != LogicalProject || len(plan.Children) != 1 || plan.Children[0].Kind != LogicalProject || plan.Children[0].Children[0].Kind != LogicalScan {
		t.Fatalf("unexpected CTE plan: %+v", plan)
	}
	if plan.Output[0].Name != "renamed" || plan.Children[0].Output[0].Name != "renamed" {
		t.Fatalf("CTE output schema drift: outer=%+v inner=%+v", plan.Output, plan.Children[0].Output)
	}
}

// TestPlanStableCTERequiresBoundProvenance 验证缺失 provenance 不会退回 Catalog 或猜测 source。
func TestPlanStableCTERequiresBoundProvenance(t *testing.T) {
	column := binder.RelationColumn{Name: "id", Type: types.LogicalType{ID: types.BigInt}}
	outer := &binder.BoundQuery{
		Source: &binder.BoundRelation{Kind: binder.RelationCTE, ID: "cte:x", Columns: []binder.RelationColumn{column}},
		Projection: []binder.BoundSelectItem{{Expression: &binder.BoundExpression{Kind: binder.BoundColumn, Type: column.Type, ColumnIndex: 0}, Output: column}},
		Output: []binder.RelationColumn{column},
	}
	if _, err := NewQueryPlanner(nil, nil).PlanStable(context.Background(), outer); err == nil {
		t.Fatal("expected missing CTE provenance rejection")
	}
}
