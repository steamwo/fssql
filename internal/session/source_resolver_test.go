package session

import (
	"context"
	"testing"

	"github.com/steamwo/fssql/internal/catalog"
	"github.com/steamwo/fssql/internal/core/stats"
	"github.com/steamwo/fssql/internal/core/types"
	"github.com/steamwo/fssql/internal/function"
	"github.com/steamwo/fssql/internal/sql/ast"
	"github.com/steamwo/fssql/internal/sql/binder"
)

func testQualifiedName(parts ...string) ast.QualifiedName {
	ids := make([]ast.Identifier, len(parts))
	for i, part := range parts {
		ids[i] = ast.Identifier{Name: part}
	}
	return ast.QualifiedName{Parts: ids}
}

func TestSourceResolverBindsRegisteredTableFunction(t *testing.T) {
	memoryCatalog, _ := catalog.NewMemoryCatalog(8)
	tx, _ := memoryCatalog.Begin(context.Background())
	resolver, err := NewSourceResolver(tx, nil, []TableBindAdapter{{
		FunctionID: function.ReadCSVID,
		Bind: func(_ context.Context, source string) (binder.ResolvedRelation, error) {
			return binder.ResolvedRelation{
				ID:          "csv:" + source,
				Fingerprint: stats.DatasetFingerprint("fp:" + source),
				Columns: []binder.RelationColumn{{Name: "id", Type: types.LogicalType{ID: types.BigInt}}},
			}, nil
		},
	}})
	if err != nil {
		t.Fatal(err)
	}
	def, _ := function.NewBuiltinCatalog().Lookup("read_csv")
	resolved, err := resolver.BindTableFunction(context.Background(), def, []binder.TableFunctionArgument{{Type: types.LogicalType{ID: types.Varchar}, Value: "s3://bucket/a.csv"}})
	if err != nil {
		t.Fatal(err)
	}
	if resolved.ID == "" || resolved.Fingerprint == "" || len(resolved.Columns) != 1 {
		t.Fatalf("resolved=%+v", resolved)
	}
}

func TestSourceResolverResolvesViewFromCatalogSnapshot(t *testing.T) {
	memoryCatalog, _ := catalog.NewMemoryCatalog(8)
	write, _ := memoryCatalog.Begin(context.Background())
	if err := write.CreateView(context.Background(), catalog.View{
		Name: catalog.ObjectName{Schema: "main", Name: "v"},
		SQL:  "SELECT a FROM t",
		Columns: []catalog.ColumnDefinition{{Name: "a", Type: types.LogicalType{ID: types.BigInt}}},
	}); err != nil {
		t.Fatal(err)
	}
	if err := write.Commit(context.Background()); err != nil {
		t.Fatal(err)
	}
	read, _ := memoryCatalog.Begin(context.Background())
	resolver, err := NewSourceResolver(read, nil, nil)
	if err != nil {
		t.Fatal(err)
	}
	resolved, err := resolver.ResolveRelation(context.Background(), testQualifiedName("v"))
	if err != nil {
		t.Fatal(err)
	}
	if resolved.ID != "view:main.v" || len(resolved.Columns) != 1 || resolved.Columns[0].Name != "a" {
		t.Fatalf("resolved=%+v", resolved)
	}
}

func TestSourceResolverRebindsExternalTableAndRejectsSchemaDrift(t *testing.T) {
	memoryCatalog, _ := catalog.NewMemoryCatalog(8)
	write, _ := memoryCatalog.Begin(context.Background())
	if err := write.CreateExternalTable(context.Background(), catalog.ExternalTable{
		Name:          catalog.ObjectName{Schema: "main", Name: "ext"},
		TableFunction: "read_parquet",
		Source:        "s3://bucket/data/*.parquet",
		Columns: []catalog.ColumnDefinition{{Name: "id", Type: types.LogicalType{ID: types.BigInt}}},
	}); err != nil {
		t.Fatal(err)
	}
	if err := write.Commit(context.Background()); err != nil {
		t.Fatal(err)
	}

	newResolver := func(columnType types.LogicalType) *SourceResolver {
		read, _ := memoryCatalog.Begin(context.Background())
		resolver, err := NewSourceResolver(read, nil, []TableBindAdapter{{
			FunctionID: function.ReadParquetID,
			Bind: func(_ context.Context, source string) (binder.ResolvedRelation, error) {
				return binder.ResolvedRelation{
					ID:          "parquet:" + source,
					Fingerprint: stats.DatasetFingerprint("snapshot-1"),
					Columns:     []binder.RelationColumn{{Name: "id", Type: columnType}},
				}, nil
			},
		}})
		if err != nil {
			t.Fatal(err)
		}
		return resolver
	}

	resolved, err := newResolver(types.LogicalType{ID: types.BigInt}).ResolveRelation(context.Background(), testQualifiedName("ext"))
	if err != nil {
		t.Fatal(err)
	}
	if resolved.Fingerprint != stats.DatasetFingerprint("snapshot-1") || len(resolved.Columns) != 1 {
		t.Fatalf("resolved=%+v", resolved)
	}
	if _, err := newResolver(types.LogicalType{ID: types.Varchar}).ResolveRelation(context.Background(), testQualifiedName("ext")); err == nil {
		t.Fatal("expected external-table schema drift rejection")
	}
}

func TestSourceResolverRejectsMissingAdapterFingerprintAndBadRegistry(t *testing.T) {
	memoryCatalog, _ := catalog.NewMemoryCatalog(8)
	tx, _ := memoryCatalog.Begin(context.Background())
	if _, err := NewSourceResolver(tx, nil, []TableBindAdapter{{FunctionID: function.SumID, Bind: func(context.Context, string) (binder.ResolvedRelation, error) { return binder.ResolvedRelation{}, nil }}}); err == nil {
		t.Fatal("aggregate function must not register as table binder")
	}

	resolver, err := NewSourceResolver(tx, nil, []TableBindAdapter{{
		FunctionID: function.ReadJSONID,
		Bind: func(_ context.Context, source string) (binder.ResolvedRelation, error) {
			return binder.ResolvedRelation{ID: "json:" + source, Columns: []binder.RelationColumn{{Name: "id", Type: types.LogicalType{ID: types.BigInt}}}}, nil
		},
	}})
	if err != nil {
		t.Fatal(err)
	}
	def, _ := function.NewBuiltinCatalog().Lookup("read_json")
	if _, err := resolver.BindTableFunction(context.Background(), def, []binder.TableFunctionArgument{{Type: types.LogicalType{ID: types.Varchar}, Value: "x.json"}}); err == nil {
		t.Fatal("expected empty fingerprint rejection")
	}
}
