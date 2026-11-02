package session

import (
	"context"
	"strings"
	"testing"

	"github.com/steamwo/fssql/internal/catalog"
	"github.com/steamwo/fssql/internal/core/stats"
	"github.com/steamwo/fssql/internal/core/types"
	"github.com/steamwo/fssql/internal/function"
	"github.com/steamwo/fssql/internal/planner"
	"github.com/steamwo/fssql/internal/sql/ast"
	"github.com/steamwo/fssql/internal/sql/binder"
	"github.com/steamwo/fssql/internal/sql/parser"
)

// newViewPlanningFixture 创建带一个 external table 的 statement-scoped view planner。
func newViewPlanningFixture(t *testing.T, external catalog.ExternalTable, views ...catalog.View) (*ViewPlanningResolver, *catalog.MemoryCatalog) {
	t.Helper()
	ctx := context.Background()
	memoryCatalog, err := catalog.NewMemoryCatalog(16)
	if err != nil {
		t.Fatal(err)
	}
	write, _ := memoryCatalog.Begin(ctx)
	if err := write.CreateExternalTable(ctx, external); err != nil {
		t.Fatal(err)
	}
	for _, view := range views {
		if err := write.CreateView(ctx, view); err != nil {
			t.Fatal(err)
		}
	}
	if err := write.Commit(ctx); err != nil {
		t.Fatal(err)
	}
	read, _ := memoryCatalog.Begin(ctx)
	base, err := NewSourceResolver(read, nil, []TableBindAdapter{{
		FunctionID: function.ReadCSVID,
		Bind: func(_ context.Context, _ string) (binder.ResolvedRelation, error) {
			columns := make([]binder.RelationColumn, len(external.Columns))
			for i, column := range external.Columns {
				columns[i] = binder.RelationColumn{Name: column.Name, Type: column.Type, Nullable: column.Nullable}
			}
			return binder.ResolvedRelation{ID: "tablefunc:read_csv:fp", Fingerprint: stats.DatasetFingerprint("fp"), Columns: columns}, nil
		},
	}})
	if err != nil {
		t.Fatal(err)
	}
	planning, err := NewPlanningSourceResolver(base)
	if err != nil {
		t.Fatal(err)
	}
	resolver, err := NewViewPlanningResolver(planning)
	if err != nil {
		t.Fatal(err)
	}
	return resolver, memoryCatalog
}

// bindAndPlanViewQuery 通过真实 Parser/QueryBinder/QueryPlanner 规划外层 SELECT。
func bindAndPlanViewQuery(t *testing.T, resolver *ViewPlanningResolver, sql string) (*planner.LogicalPlan, error) {
	t.Helper()
	statement, err := parser.Parse(sql)
	if err != nil {
		return nil, err
	}
	selectStatement, ok := statement.(*ast.SelectStmt)
	if !ok {
		t.Fatalf("statement=%T", statement)
	}
	functions := function.NewBuiltinCatalog()
	bound, err := binder.NewQueryBinder(functions, resolver, resolver, sql).BindSelect(context.Background(), selectStatement)
	if err != nil {
		return nil, err
	}
	return planner.NewQueryPlanner(functions, resolver).Plan(context.Background(), bound)
}

// TestViewPlanningResolverExpandsViewToExternalScan 验证 view SQL 重新走 Parser/Binder/Planner，并最终恢复 external table scan。
func TestViewPlanningResolverExpandsViewToExternalScan(t *testing.T) {
	external := catalog.ExternalTable{
		Name: catalog.ObjectName{Schema: "main", Name: "ext"}, TableFunction: "read_csv", Source: "mem://rows.csv",
		Columns: []catalog.ColumnDefinition{{Name: "id", Type: types.LogicalType{ID: types.BigInt}, Nullable: false}},
	}
	view := catalog.View{
		Name: catalog.ObjectName{Schema: "main", Name: "v"}, SQL: "SELECT id FROM ext",
		Columns: []catalog.ColumnDefinition{{Name: "id", Type: types.LogicalType{ID: types.BigInt}, Nullable: false}},
	}
	resolver, _ := newViewPlanningFixture(t, external, view)
	logical, err := bindAndPlanViewQuery(t, resolver, "SELECT id FROM v")
	if err != nil {
		t.Fatal(err)
	}
	// outer Project -> view Project -> external Scan。
	if logical.Kind != planner.LogicalProject || len(logical.Children) != 1 {
		t.Fatalf("outer=%+v", logical)
	}
	viewPlan := logical.Children[0]
	if viewPlan.Kind != planner.LogicalProject || len(viewPlan.Children) != 1 {
		t.Fatalf("view plan=%+v", viewPlan)
	}
	scan := viewPlan.Children[0]
	if scan.Kind != planner.LogicalScan || scan.Scan == nil {
		t.Fatalf("scan=%+v", scan)
	}
	if scan.Scan.Function != "read_csv" || scan.Scan.Source != "mem://rows.csv" || scan.Scan.Fingerprint != stats.DatasetFingerprint("fp") {
		t.Fatalf("scan spec=%+v", scan.Scan)
	}
}

// TestViewPlanningResolverRejectsRecursiveView 验证 self-reference 在 planning stack 中被明确拒绝而不是无限递归。
func TestViewPlanningResolverRejectsRecursiveView(t *testing.T) {
	external := catalog.ExternalTable{
		Name: catalog.ObjectName{Schema: "main", Name: "unused"}, TableFunction: "read_csv", Source: "mem://unused.csv",
		Columns: []catalog.ColumnDefinition{{Name: "id", Type: types.LogicalType{ID: types.BigInt}}},
	}
	view := catalog.View{
		Name: catalog.ObjectName{Schema: "main", Name: "v"}, SQL: "SELECT id FROM v",
		Columns: []catalog.ColumnDefinition{{Name: "id", Type: types.LogicalType{ID: types.BigInt}}},
	}
	resolver, _ := newViewPlanningFixture(t, external, view)
	_, err := bindAndPlanViewQuery(t, resolver, "SELECT id FROM v")
	if err == nil || !strings.Contains(err.Error(), "recursive view reference") {
		t.Fatalf("err=%v", err)
	}
}

// TestViewPlanningResolverRejectsFrozenSchemaDrift 验证 Catalog view 的冻结输出与当前 SQL 重新绑定结果不一致时 fail-closed。
func TestViewPlanningResolverRejectsFrozenSchemaDrift(t *testing.T) {
	external := catalog.ExternalTable{
		Name: catalog.ObjectName{Schema: "main", Name: "ext"}, TableFunction: "read_csv", Source: "mem://rows.csv",
		Columns: []catalog.ColumnDefinition{{Name: "name", Type: types.LogicalType{ID: types.Varchar}, Nullable: true}},
	}
	view := catalog.View{
		Name: catalog.ObjectName{Schema: "main", Name: "v"}, SQL: "SELECT name FROM ext",
		// 模拟 Catalog 中冻结的是创建时不同 schema；执行时不能静默接受当前重新绑定结果。
		Columns: []catalog.ColumnDefinition{{Name: "id", Type: types.LogicalType{ID: types.BigInt}, Nullable: false}},
	}
	resolver, _ := newViewPlanningFixture(t, external, view)
	_, err := bindAndPlanViewQuery(t, resolver, "SELECT id FROM v")
	if err == nil || !strings.Contains(err.Error(), "current executable schema differs") {
		t.Fatalf("err=%v", err)
	}
}
