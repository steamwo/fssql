package planner

import (
	"context"
	"testing"

	"github.com/steamwo/fssql/internal/function"
	"github.com/steamwo/fssql/internal/sql/ast"
	"github.com/steamwo/fssql/internal/sql/binder"
	"github.com/steamwo/fssql/internal/sql/parser"
)

func TestDistinctAggregatesUseHashDedupWhenSharingOneArgument(t *testing.T) {
	for _, tc := range []struct {
		name              string
		projection        string
		groupBy           string
		sorts             int
		aggregates        int
		aggregateNodes    int
		distinctValueFunc int
	}{
		{"shared_argument", "count(DISTINCT v),sum(DISTINCT v),avg(DISTINCT v)", "", 0, 3, 2, 0},
		{"shared_argument_with_minmax", "count(DISTINCT v),min(v),max(DISTINCT v)", "", 0, 3, 2, 0},
		{"insensitive_only", "min(v),min(DISTINCT v),max(v),max(DISTINCT v)", "", 0, 2, 1, 0},
		{"mixed_argument", "count(DISTINCT v),sum(w)", "", 1, 2, 1, 1},
		{"different_arguments", "count(DISTINCT v),sum(DISTINCT w)", "", 2, 2, 1, 2},
		{"grouped_shared_argument", "g,count(DISTINCT v),sum(DISTINCT v)", "GROUP BY g", 0, 2, 2, 0},
		{"grouped_mixed", "g,count(DISTINCT v),count(*)", "GROUP BY g", 1, 2, 1, 1},
	} {
		t.Run(tc.name, func(t *testing.T) {
			sql := "WITH t AS (SELECT 1 AS g, 1 AS v, 2 AS w UNION ALL SELECT 2, 2, 3) SELECT " + tc.projection + " FROM t " + tc.groupBy
			parsed, err := parser.Parse(sql)
			if err != nil {
				t.Fatal(err)
			}
			functions := function.NewQueryCatalog()
			query, err := binder.NewQueryBinder(functions, nil, nil, sql).BindSelect(context.Background(), parsed.(*ast.SelectStmt))
			if err != nil {
				t.Fatal(err)
			}
			plan, err := NewQueryPlanner(functions, nil).PlanStable(context.Background(), query)
			if err != nil {
				t.Fatal(err)
			}
			sorts, aggregates, nodes, markers := 0, 0, 0, 0
			var visit func(*LogicalPlan)
			visit = func(p *LogicalPlan) {
				if p.Kind == LogicalSort {
					sorts++
				}
				if p.Kind == LogicalAggregate {
					nodes++
					aggregates += len(p.Aggregates)
				}
				for _, expression := range p.Projection {
					if expression != nil && expression.Kind == binder.BoundFunction && expression.FunctionID == function.DistinctValueID {
						markers++
					}
				}
				for _, child := range p.Children {
					visit(child)
				}
			}
			visit(plan)
			if sorts != tc.sorts || aggregates != tc.aggregates || nodes != tc.aggregateNodes || markers != tc.distinctValueFunc {
				t.Fatalf("%s: sorts=%d aggregates=%d nodes=%d markers=%d", tc.projection, sorts, aggregates, nodes, markers)
			}
		})
	}
}
