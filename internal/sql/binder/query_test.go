package binder

import (
	"context"
	"fmt"
	"testing"

	"github.com/steamwo/fssql/internal/core/stats"
	"github.com/steamwo/fssql/internal/core/types"
	"github.com/steamwo/fssql/internal/function"
	"github.com/steamwo/fssql/internal/sql/ast"
	"github.com/steamwo/fssql/internal/sql/parser"
	"github.com/steamwo/fssql/internal/tablefunc"
)

type fakeRelationResolver struct {
	relations map[string]ResolvedRelation
}

func (r fakeRelationResolver) ResolveRelation(_ context.Context, name ast.QualifiedName) (ResolvedRelation, error) {
	key := name.Parts[len(name.Parts)-1].Name
	v, ok := r.relations[key]
	if !ok {
		return ResolvedRelation{}, fmt.Errorf("not found: %s", key)
	}
	return v, nil
}

type fakeTableFunctionResolver struct{}

func (fakeTableFunctionResolver) BindTableFunction(_ context.Context, def function.Definition, args []TableFunctionArgument) (ResolvedRelation, error) {
	if len(args) != 1 {
		return ResolvedRelation{}, fmt.Errorf("expected one source")
	}
	return ResolvedRelation{
		ID:          "tf:" + def.Name + ":" + args[0].Value,
		Fingerprint: stats.DatasetFingerprint("fp:" + args[0].Value),
		Columns: []RelationColumn{
			{Name: "id", Type: types.LogicalType{ID: types.BigInt}},
			{Name: "name", Type: types.LogicalType{ID: types.Varchar}, Nullable: true},
		},
	}, nil
}

func mustQueryAST(t *testing.T, sql string) *ast.SelectStmt {
	t.Helper()
	stmt, err := parser.Parse(sql)
	if err != nil {
		t.Fatal(err)
	}
	q, ok := stmt.(*ast.SelectStmt)
	if !ok {
		t.Fatalf("stmt=%T", stmt)
	}
	return q
}

func testQueryBinder(sql string) *QueryBinder {
	return NewQueryBinder(nil, fakeRelationResolver{relations: map[string]ResolvedRelation{
		"t": {ID: "catalog:main.t", Columns: []RelationColumn{
			{Name: "a", Type: types.LogicalType{ID: types.BigInt}},
			{Name: "b", Type: types.LogicalType{ID: types.Varchar}, Nullable: true},
		}},
		"u": {ID: "catalog:main.u", Columns: []RelationColumn{
			{Name: "a", Type: types.LogicalType{ID: types.BigInt}},
		}},
	}}, fakeTableFunctionResolver{}, sql)
}

func TestQueryBinderCatalogRelationProjectionAndFilter(t *testing.T) {
	sql := "SELECT a, b FROM t WHERE a > 1 ORDER BY a LIMIT 10 OFFSET 2"
	q, err := testQueryBinder(sql).BindSelect(context.Background(), mustQueryAST(t, sql))
	if err != nil {
		t.Fatal(err)
	}
	if q.Source == nil || q.Source.ID != "catalog:main.t" || len(q.Output) != 2 || q.Where == nil || len(q.OrderBy) != 1 || q.Limit == nil || q.Offset == nil {
		t.Fatalf("query=%+v", q)
	}
	if q.Output[0].Name != "a" || q.Output[0].Type.ID != types.BigInt {
		t.Fatalf("output=%+v", q.Output)
	}
}

func TestQueryBinderCTEShadowsResolverAndExpandsStar(t *testing.T) {
	sql := "WITH x(v) AS (SELECT a FROM t) SELECT * FROM x"
	q, err := testQueryBinder(sql).BindSelect(context.Background(), mustQueryAST(t, sql))
	if err != nil {
		t.Fatal(err)
	}
	if q.Source == nil || q.Source.Kind != RelationCTE || len(q.Output) != 1 || q.Output[0].Name != "v" {
		t.Fatalf("query=%+v", q)
	}
}

func TestQueryBinderUsesRealTableFunctionResolverBoundary(t *testing.T) {
	sql := "SELECT id FROM read_csv('s3://bucket/a.csv') f WHERE id > 0"
	q, err := testQueryBinder(sql).BindSelect(context.Background(), mustQueryAST(t, sql))
	if err != nil {
		t.Fatal(err)
	}
	source, options, encoded, err := tablefunc.Decode(q.Source.Source)
	if err != nil || !encoded || source != "s3://bucket/a.csv" || options["allow_quoted_nulls"] != "true" {
		t.Fatalf("source=%s options=%v encoded=%v err=%v", source, options, encoded, err)
	}
	if q.Source.Kind != RelationTableFunction || q.Source.FunctionID != function.ReadCSVID || q.Source.Fingerprint == "" {
		t.Fatalf("source=%+v", q.Source)
	}
}

func TestQueryBinderJoinAndSemanticRejections(t *testing.T) {
	sql := "SELECT t.a FROM t JOIN u ON t.a = u.a"
	q, err := testQueryBinder(sql).BindSelect(context.Background(), mustQueryAST(t, sql))
	if err != nil {
		t.Fatal(err)
	}
	if q.Source == nil || q.Source.Kind != RelationJoin || q.Source.Condition == nil {
		t.Fatalf("source=%+v", q.Source)
	}
	for _, bad := range []string{
		"SELECT a FROM t WHERE count(a) > 1",
		"SELECT a FROM t GROUP BY count(a)",
		"SELECT a FROM t LIMIT a",
		"SELECT *",
	} {
		if _, err := testQueryBinder(bad).BindSelect(context.Background(), mustQueryAST(t, bad)); err == nil {
			t.Fatalf("expected rejection for %s", bad)
		}
	}
}

func TestQueryBinderRejectsNonLiteralStableTableFunctionArgument(t *testing.T) {
	sql := "SELECT * FROM read_parquet(a)"
	if _, err := testQueryBinder(sql).BindSelect(context.Background(), mustQueryAST(t, sql)); err == nil {
		t.Fatal("expected non-literal table-function argument rejection")
	}
}
