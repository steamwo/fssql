package planner

import (
	"context"
	"testing"

	"github.com/steamwo/fssql/internal/function"
	"github.com/steamwo/fssql/internal/sql/ast"
	"github.com/steamwo/fssql/internal/sql/binder"
	"github.com/steamwo/fssql/internal/sql/parser"
)

func TestDistinctAggregatesShareInputAndIgnoreInsensitiveDistinct(t *testing.T) {
	for _, tc := range []struct {
		projection        string
		sorts, aggregates int
	}{{"count(DISTINCT v),sum(DISTINCT v),avg(DISTINCT v)", 1, 3}, {"min(v),min(DISTINCT v),max(v),max(DISTINCT v)", 0, 2}} {
		sql := "WITH t AS (SELECT 1 AS v UNION ALL SELECT 2) SELECT " + tc.projection + " FROM t"
		parsed, err := parser.Parse(sql)
		if err != nil {
			t.Fatal(err)
		}
		functions := function.NewPhase5Catalog()
		query, err := binder.NewQueryBinder(functions, nil, nil, sql).BindSelect(context.Background(), parsed.(*ast.SelectStmt))
		if err != nil {
			t.Fatal(err)
		}
		plan, err := NewQueryPlanner(functions, nil).PlanStable(context.Background(), query)
		if err != nil {
			t.Fatal(err)
		}
		sorts, aggregates := 0, 0
		var visit func(*LogicalPlan)
		visit = func(p *LogicalPlan) {
			if p.Kind == LogicalSort {
				sorts++
			}
			aggregates += len(p.Aggregates)
			for _, child := range p.Children {
				visit(child)
			}
		}
		visit(plan)
		if sorts != tc.sorts || aggregates != tc.aggregates {
			t.Fatalf("%s: sorts=%d aggregates=%d", tc.projection, sorts, aggregates)
		}
	}
}
