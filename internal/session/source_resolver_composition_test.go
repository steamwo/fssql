package session

import (
	"context"
	"testing"

	"github.com/steamwo/fssql/internal/catalog"
	"github.com/steamwo/fssql/internal/core/types"
	"github.com/steamwo/fssql/internal/function"
	"github.com/steamwo/fssql/internal/sql/ast"
	"github.com/steamwo/fssql/internal/sql/binder"
	"github.com/steamwo/fssql/internal/storage/fileset"
)

// TestNewStableSourceResolverComposesRealBind 验证 production helper 把 concrete Bind provider 接入 Statement 级唯一 SourceResolver。
func TestNewStableSourceResolverComposesRealBind(t *testing.T) {
	ctx := context.Background()
	cat, err := catalog.NewMemoryCatalog(8)
	if err != nil {
		t.Fatal(err)
	}
	tx, err := cat.Begin(ctx)
	if err != nil {
		t.Fatal(err)
	}
	defer tx.Rollback(context.Background())

	called := 0
	reader := func(_ context.Context, source string) ([]types.StructField, string, error) {
		called++
		if source != "s3://bucket/data.parquet" {
			t.Fatalf("source=%q", source)
		}
		return []types.StructField{{Name: "id", Type: types.LogicalType{ID: types.BigInt}}}, "fp-1", nil
	}
	provider := &StableTableFunctionResolver{
		parquet: reader,
		csv:     reader,
		json:    reader,
		ndjson:  reader,
		glob: func(context.Context, string) (*fileset.ScanSet, error) {
			return nil, nil
		},
	}
	resolver, err := NewStableSourceResolver(tx, function.NewBuiltinCatalog(), provider)
	if err != nil {
		t.Fatal(err)
	}
	def, ok := function.NewBuiltinCatalog().LookupID(function.ReadParquetID)
	if !ok {
		t.Fatal("missing read_parquet definition")
	}
	resolved, err := resolver.BindTableFunction(ctx, def, []binder.TableFunctionArgument{{Type: types.LogicalType{ID: types.Varchar}, Value: "s3://bucket/data.parquet"}})
	if err != nil {
		t.Fatal(err)
	}
	if called != 1 || string(resolved.Fingerprint) != "fp-1" || len(resolved.Columns) != 1 {
		t.Fatalf("called=%d resolved=%+v", called, resolved)
	}
}

// TestStableSourceResolverExternalTableRebind 验证 Catalog external table 每个 Statement 仍经同一 concrete Bind callback 重绑定，而不是只信任存储 schema。
func TestStableSourceResolverExternalTableRebind(t *testing.T) {
	ctx := context.Background()
	cat, err := catalog.NewMemoryCatalog(8)
	if err != nil {
		t.Fatal(err)
	}
	createTx, err := cat.Begin(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if err := createTx.CreateExternalTable(ctx, catalog.ExternalTable{
		Name:          catalog.ObjectName{Schema: "main", Name: "events"},
		Columns:       []catalog.ColumnDefinition{{Name: "id", Type: types.LogicalType{ID: types.BigInt}, Nullable: true}},
		TableFunction: "read_parquet",
		Source:        "s3://bucket/data.parquet",
	}); err != nil {
		t.Fatal(err)
	}
	if err := createTx.Commit(ctx); err != nil {
		t.Fatal(err)
	}

	tx, err := cat.Begin(ctx)
	if err != nil {
		t.Fatal(err)
	}
	defer tx.Rollback(context.Background())
	calls := 0
	reader := func(context.Context, string) ([]types.StructField, string, error) {
		calls++
		return []types.StructField{{Name: "id", Type: types.LogicalType{ID: types.BigInt}}}, "fp-current", nil
	}
	provider := &StableTableFunctionResolver{
		parquet: reader,
		csv:     reader,
		json:    reader,
		ndjson:  reader,
		glob: func(context.Context, string) (*fileset.ScanSet, error) {
			return nil, nil
		},
	}
	resolver, err := NewStableSourceResolver(tx, function.NewBuiltinCatalog(), provider)
	if err != nil {
		t.Fatal(err)
	}
	name := ast.QualifiedName{Parts: []ast.Identifier{{Name: "events"}}}
	resolved, err := resolver.ResolveRelation(ctx, name)
	if err != nil {
		t.Fatal(err)
	}
	if calls != 1 || string(resolved.Fingerprint) != "fp-current" {
		t.Fatalf("calls=%d resolved=%+v", calls, resolved)
	}
}

// TestStableTableBindAdaptersRequireCompleteProvider 防止缺失任意 Stable reader 时 composition 静默注册不完整 resolver。
func TestStableTableBindAdaptersRequireCompleteProvider(t *testing.T) {
	provider := &StableTableFunctionResolver{
		parquet: func(context.Context, string) ([]types.StructField, string, error) {
			return nil, "fp", nil
		},
	}
	if _, err := provider.TableBindAdapters(); err == nil {
		t.Fatal("expected incomplete provider error")
	}
}
