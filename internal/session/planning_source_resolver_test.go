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

// TestPlanningSourceResolverExternalTableEndToEnd 验证 Catalog external table 从 Parser/Bind 到 QueryPlanner 不丢 Function/Source/Fingerprint。
func TestPlanningSourceResolverExternalTableEndToEnd(t *testing.T) {
	ctx := context.Background()
	memoryCatalog, err := catalog.NewMemoryCatalog(8)
	if err != nil {
		t.Fatal(err)
	}
	write, _ := memoryCatalog.Begin(ctx)
	if err := write.CreateExternalTable(ctx, catalog.ExternalTable{
		Name:          catalog.ObjectName{Schema: "main", Name: "ext"},
		TableFunction: "read_csv",
		Source:        "s3://bucket/data.csv",
		Columns:       []catalog.ColumnDefinition{{Name: "id", Type: types.LogicalType{ID: types.BigInt}, Nullable: false}},
	}); err != nil {
		t.Fatal(err)
	}
	if err := write.Commit(ctx); err != nil {
		t.Fatal(err)
	}

	read, _ := memoryCatalog.Begin(ctx)
	base, err := NewSourceResolver(read, nil, []TableBindAdapter{{
		FunctionID: function.ReadCSVID,
		Bind: func(_ context.Context, source string) (binder.ResolvedRelation, error) {
			return binder.ResolvedRelation{
				ID:          "tablefunc:read_csv:snapshot-1",
				Fingerprint: stats.DatasetFingerprint("snapshot-1"),
				Columns:     []binder.RelationColumn{{Name: "id", Type: types.LogicalType{ID: types.BigInt}, Nullable: false}},
			}, nil
		},
	}})
	if err != nil {
		t.Fatal(err)
	}
	resolver, err := NewPlanningSourceResolver(base)
	if err != nil {
		t.Fatal(err)
	}

	statement, err := parser.Parse("SELECT id FROM ext")
	if err != nil {
		t.Fatal(err)
	}
	selectStatement, ok := statement.(*ast.SelectStmt)
	if !ok {
		t.Fatalf("statement=%T", statement)
	}
	query, err := binder.NewQueryBinder(function.NewBuiltinCatalog(), resolver, resolver, "SELECT id FROM ext").BindSelect(ctx, selectStatement)
	if err != nil {
		t.Fatal(err)
	}
	if query.Source == nil || !strings.HasPrefix(query.Source.ID, "external:main.ext:") {
		t.Fatalf("source=%+v", query.Source)
	}
	logical, err := planner.NewQueryPlanner(function.NewBuiltinCatalog(), resolver).Plan(ctx, query)
	if err != nil {
		t.Fatal(err)
	}
	if logical.Kind != planner.LogicalProject || len(logical.Children) != 1 {
		t.Fatalf("logical=%+v", logical)
	}
	scan := logical.Children[0]
	if scan.Kind != planner.LogicalScan || scan.Scan == nil {
		t.Fatalf("scan=%+v", scan)
	}
	if scan.Scan.Function != "read_csv" || scan.Scan.Source != "s3://bucket/data.csv" || scan.Scan.Fingerprint != stats.DatasetFingerprint("snapshot-1") {
		t.Fatalf("scan spec=%+v", scan.Scan)
	}
	if scan.Scan.RelationID != query.Source.ID {
		t.Fatalf("relation id=%q want=%q", scan.Scan.RelationID, query.Source.ID)
	}
}

// TestPlanningSourceResolverExternalObjectsHaveDistinctRelationIDs 防止两个 Catalog 对象指向同一 snapshot 时覆盖 statement provenance。
func TestPlanningSourceResolverExternalObjectsHaveDistinctRelationIDs(t *testing.T) {
	ctx := context.Background()
	memoryCatalog, _ := catalog.NewMemoryCatalog(8)
	write, _ := memoryCatalog.Begin(ctx)
	for _, name := range []string{"a", "b"} {
		if err := write.CreateExternalTable(ctx, catalog.ExternalTable{
			Name: catalog.ObjectName{Schema: "main", Name: name}, TableFunction: "read_csv", Source: "mem://same.csv",
			Columns: []catalog.ColumnDefinition{{Name: "id", Type: types.LogicalType{ID: types.BigInt}}},
		}); err != nil {
			t.Fatal(err)
		}
	}
	if err := write.Commit(ctx); err != nil {
		t.Fatal(err)
	}
	read, _ := memoryCatalog.Begin(ctx)
	base, _ := NewSourceResolver(read, nil, []TableBindAdapter{{FunctionID: function.ReadCSVID, Bind: func(context.Context, string) (binder.ResolvedRelation, error) {
		return binder.ResolvedRelation{ID: "tablefunc:read_csv:fp", Fingerprint: "fp", Columns: []binder.RelationColumn{{Name: "id", Type: types.LogicalType{ID: types.BigInt}}}}, nil
	}})
	resolver, _ := NewPlanningSourceResolver(base)
	a, err := resolver.ResolveRelation(ctx, testQualifiedName("a"))
	if err != nil {
		t.Fatal(err)
	}
	b, err := resolver.ResolveRelation(ctx, testQualifiedName("b"))
	if err != nil {
		t.Fatal(err)
	}
	if a.ID == b.ID || !strings.Contains(a.ID, "external:main.a:") || !strings.Contains(b.ID, "external:main.b:") {
		t.Fatalf("a=%q b=%q", a.ID, b.ID)
	}
}

// TestPlanningSourceResolverViewProvenanceFailsClosedUntilRecursivePlanner 验证 view SQL 已记录，但不会被误当 external scan 执行。
func TestPlanningSourceResolverViewProvenanceFailsClosedUntilRecursivePlanner(t *testing.T) {
	ctx := context.Background()
	memoryCatalog, _ := catalog.NewMemoryCatalog(8)
	write, _ := memoryCatalog.Begin(ctx)
	if err := write.CreateView(ctx, catalog.View{
		Name: catalog.ObjectName{Schema: "main", Name: "v"}, SQL: "SELECT id FROM read_csv('mem://v.csv')",
		Columns: []catalog.ColumnDefinition{{Name: "id", Type: types.LogicalType{ID: types.BigInt}, Nullable: true}},
	}); err != nil {
		t.Fatal(err)
	}
	if err := write.Commit(ctx); err != nil {
		t.Fatal(err)
	}
	read, _ := memoryCatalog.Begin(ctx)
	base, _ := NewSourceResolver(read, nil, nil)
	resolver, _ := NewPlanningSourceResolver(base)
	resolved, err := resolver.ResolveRelation(ctx, testQualifiedName("v"))
	if err != nil {
		t.Fatal(err)
	}
	relation := &binder.BoundRelation{Kind: binder.RelationCatalog, ID: resolved.ID, Columns: resolved.Columns, Fingerprint: resolved.Fingerprint}
	if _, err := resolver.PlanBoundRelation(ctx, relation); err == nil || !strings.Contains(err.Error(), "recursive view planning") {
		t.Fatalf("err=%v", err)
	}
}
