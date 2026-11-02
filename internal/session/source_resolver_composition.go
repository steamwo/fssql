package session

import (
	"context"
	"fmt"

	"github.com/steamwo/fssql/internal/catalog"
	"github.com/steamwo/fssql/internal/core/types"
	"github.com/steamwo/fssql/internal/function"
	"github.com/steamwo/fssql/internal/sql/binder"
)

// TableBindAdapters 将 production StableTableFunctionResolver 冻结为 SourceResolver 使用的真实 Bind adapter 列表。
// SourceResolver 因此仍是 Statement 级唯一 Catalog/TableFunction resolver；本类型只提供 concrete format composition。
func (r *StableTableFunctionResolver) TableBindAdapters() ([]TableBindAdapter, error) {
	if r == nil {
		return nil, fmt.Errorf("session: nil Stable table function resolver")
	}
	if r.parquet == nil || r.csv == nil || r.json == nil || r.ndjson == nil || r.glob == nil {
		return nil, fmt.Errorf("session: Stable table function resolver is incomplete")
	}
	catalog := function.NewBuiltinCatalog()
	ids := [...]function.ID{
		function.ReadParquetID,
		function.ReadCSVID,
		function.ReadJSONID,
		function.ReadNDJSONID,
		function.GlobID,
	}
	out := make([]TableBindAdapter, 0, len(ids))
	for _, id := range ids {
		def, ok := catalog.LookupID(id)
		if !ok || def.Kind != function.Table {
			return nil, fmt.Errorf("session: builtin table function id %d is unavailable", id)
		}
		// 每次循环复制 definition，避免 callback 闭包共享可变循环变量。
		definition := def
		out = append(out, TableBindAdapter{
			FunctionID: id,
			Bind: func(ctx context.Context, source string) (binder.ResolvedRelation, error) {
				return r.BindTableFunction(ctx, definition, []binder.TableFunctionArgument{{
					Type:  types.LogicalType{ID: types.Varchar},
					Value: source,
				}})
			},
		})
	}
	return out, nil
}

// NewStableSourceResolver 是生产 composition helper：把 Catalog transaction 与全部 Stable reader 的真实 Bind 路径收敛到一个 SourceResolver。
func NewStableSourceResolver(tx catalog.Transaction, functions *function.Catalog, tableFunctions *StableTableFunctionResolver) (*SourceResolver, error) {
	if tableFunctions == nil {
		return nil, fmt.Errorf("session: Stable source resolver requires table-function composition")
	}
	adapters, err := tableFunctions.TableBindAdapters()
	if err != nil {
		return nil, err
	}
	return NewSourceResolver(tx, functions, adapters)
}
