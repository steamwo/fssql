package planner

import (
	"context"
	"fmt"
	"strings"
	"testing"

	"github.com/steamwo/fssql/internal/function"
	"github.com/steamwo/fssql/internal/sql/ast"
	"github.com/steamwo/fssql/internal/sql/binder"
	"github.com/steamwo/fssql/internal/sql/parser"
)

// TestUnionAllPlanDepthAndOrder 用计划形状约束长链复杂度，避免依赖机器速度的耗时断言。
func TestUnionAllPlanDepthAndOrder(t *testing.T) {
	const terms = 1025
	var sql strings.Builder
	for i := 0; i < terms; i++ {
		if i > 0 {
			sql.WriteString(" UNION ALL ")
		}
		fmt.Fprintf(&sql, "SELECT %d AS value", i)
	}
	statement, err := parser.Parse(sql.String())
	if err != nil {
		t.Fatal(err)
	}
	functions := function.NewQueryCatalog()
	query, err := binder.NewQueryBinder(functions, nil, nil, sql.String()).BindSelect(context.Background(), statement.(*ast.SelectStmt))
	if err != nil {
		t.Fatal(err)
	}
	plan, err := NewQueryPlanner(functions, nil).PlanStable(context.Background(), query)
	if err != nil {
		t.Fatal(err)
	}
	if err := plan.Validate(); err != nil {
		t.Fatal(err)
	}
	values, unions, projects, maxDepth := 0, 0, 0, 0
	var visit func(*LogicalPlan, int)
	visit = func(node *LogicalPlan, depth int) {
		if depth > maxDepth {
			maxDepth = depth
		}
		if node.Kind == LogicalUnionAll {
			unions++
		}
		if node.Kind == LogicalProject {
			projects++
			if len(node.Projection) == 1 && node.Projection[0].Kind == binder.BoundLiteral {
				if got := node.Projection[0].Literal; got != fmt.Sprint(values) {
					t.Fatalf("branch %d has value %q", values, got)
				}
				values++
			}
		}
		for _, child := range node.Children {
			visit(child, depth+1)
		}
	}
	visit(plan, 1)
	if values != terms || unions != terms-1 || projects != terms+1 || maxDepth > 14 {
		t.Fatalf("values=%d unions=%d projects=%d depth=%d", values, unions, projects, maxDepth)
	}
}
