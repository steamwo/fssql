package session

import (
	"context"
	"errors"
	"testing"

	"github.com/steamwo/fssql/internal/core/types"
	"github.com/steamwo/fssql/internal/function"
	"github.com/steamwo/fssql/internal/sql/binder"
)

// TestStableTableFunctionResolverReaderDispatch 验证 SQL Binder 得到的 schema/fingerprint 来自实际 reader Bind callback。
func TestStableTableFunctionResolverReaderDispatch(t *testing.T) {
	called := ""
	resolver := &StableTableFunctionResolver{
		parquet: func(_ context.Context, source string) ([]types.StructField, string, error) {
			called = source
			return []types.StructField{{Name: "id", Type: types.LogicalType{ID: types.BigInt}}}, "fp-parquet", nil
		},
	}
	def := function.Definition{ID: function.ReadParquetID, Name: "read_parquet", Kind: function.Table, MinArgs: 1, MaxArgs: 1}
	got, err := resolver.BindTableFunction(context.Background(), def, []binder.TableFunctionArgument{{Type: types.LogicalType{ID: types.Varchar}, Value: "s3://bucket/x.parquet"}})
	if err != nil {
		t.Fatal(err)
	}
	if called != "s3://bucket/x.parquet" {
		t.Fatalf("source=%q", called)
	}
	if got.ID != "tablefunc:read_parquet:fp-parquet" || string(got.Fingerprint) != "fp-parquet" {
		t.Fatalf("identity=%+v", got)
	}
	if len(got.Columns) != 1 || got.Columns[0].Name != "id" || got.Columns[0].Type.ID != types.BigInt || !got.Columns[0].Nullable {
		t.Fatalf("columns=%+v", got.Columns)
	}
}

// TestStableTableFunctionResolverPropagatesBindFailure 确保真实 source Bind 错误不会被替换为猜测 schema。
func TestStableTableFunctionResolverPropagatesBindFailure(t *testing.T) {
	want := errors.New("bind failed")
	resolver := &StableTableFunctionResolver{
		csv: func(context.Context, string) ([]types.StructField, string, error) { return nil, "", want },
	}
	def := function.Definition{ID: function.ReadCSVID, Name: "read_csv", Kind: function.Table, MinArgs: 1, MaxArgs: 1}
	_, err := resolver.BindTableFunction(context.Background(), def, []binder.TableFunctionArgument{{Type: types.LogicalType{ID: types.Varchar}, Value: "file:///tmp/x.csv"}})
	if !errors.Is(err, want) {
		t.Fatalf("err=%v", err)
	}
}

// TestStableTableFunctionResolverRejectsInvalidInput 覆盖 table-kind、source 类型、未配置 reader 与 glob 配置失败。
func TestStableTableFunctionResolverRejectsInvalidInput(t *testing.T) {
	resolver := &StableTableFunctionResolver{}
	cases := []struct {
		name string
		def  function.Definition
		args []binder.TableFunctionArgument
	}{
		{name: "not table", def: function.Definition{ID: function.SumID, Name: "sum", Kind: function.Aggregate}},
		{name: "wrong arg type", def: function.Definition{ID: function.ReadJSONID, Name: "read_json", Kind: function.Table}, args: []binder.TableFunctionArgument{{Type: types.LogicalType{ID: types.BigInt}, Value: "x"}}},
		{name: "empty source", def: function.Definition{ID: function.ReadJSONID, Name: "read_json", Kind: function.Table}, args: []binder.TableFunctionArgument{{Type: types.LogicalType{ID: types.Varchar}}}},
		{name: "reader missing", def: function.Definition{ID: function.ReadJSONID, Name: "read_json", Kind: function.Table}, args: []binder.TableFunctionArgument{{Type: types.LogicalType{ID: types.Varchar}, Value: "x"}}},
		{name: "glob missing", def: function.Definition{ID: function.GlobID, Name: "glob", Kind: function.Table}, args: []binder.TableFunctionArgument{{Type: types.LogicalType{ID: types.Varchar}, Value: "*.csv"}}},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			if _, err := resolver.BindTableFunction(context.Background(), tc.def, tc.args); err == nil {
				t.Fatal("expected error")
			}
		})
	}
}

// TestStableTableFunctionResolverRejectsIncompleteBinding 确保空 schema/fingerprint 不会被当作可信 relation。
func TestStableTableFunctionResolverRejectsIncompleteBinding(t *testing.T) {
	resolver := &StableTableFunctionResolver{
		ndjson: func(context.Context, string) ([]types.StructField, string, error) {
			return []types.StructField{{Name: "id", Type: types.LogicalType{ID: types.BigInt}}}, "", nil
		},
	}
	def := function.Definition{ID: function.ReadNDJSONID, Name: "read_ndjson", Kind: function.Table}
	_, err := resolver.BindTableFunction(context.Background(), def, []binder.TableFunctionArgument{{Type: types.LogicalType{ID: types.Varchar}, Value: "x.ndjson"}})
	if err == nil {
		t.Fatal("expected incomplete binding error")
	}
}
