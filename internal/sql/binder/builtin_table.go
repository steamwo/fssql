package binder

import (
	"context"
	"fmt"
	"github.com/steamwo/fssql/internal/core/types"
	"github.com/steamwo/fssql/internal/function"
	"github.com/steamwo/fssql/internal/sql/ast"
)

func (b *QueryBinder) bindBuiltinTable(ctx context.Context, t *ast.TableFunction, def function.Definition) (*BoundRelation, error) {
	if len(t.Call.Args) < def.MinArgs || len(t.Call.Args) > def.MaxArgs {
		return nil, b.errorAt(t.Span, "invalid table function argument count")
	}
	exprs := b.newExpressionBinder(ctx, Scope{})
	frame := len(b.lateralScope.Columns) > 0
	if frame {
		exprs.OuterScopes = append([]Scope{cloneScope(b.lateralScope)}, b.outerScopes...)
	}
	args := make([]*BoundExpression, len(t.Call.Args))
	typ := types.LogicalType{ID: types.BigInt}
	for i, raw := range t.Call.Args {
		arg, err := exprs.BindExpression(raw)
		if err != nil {
			return nil, err
		}
		if def.ID == function.UnnestID {
			if arg.Type.ID != types.List || arg.Type.Child == nil {
				return nil, b.errorAt(raw.NodeSpan(), "unnest requires LIST")
			}
			typ = *arg.Type.Child
		} else {
			if !arg.SQLNull && (arg.Type.ID < types.TinyInt || arg.Type.ID > types.BigInt) {
				return nil, b.errorAt(raw.NodeSpan(), "range arguments must be integers")
			}
			arg, err = exprs.coerce(arg, types.LogicalType{ID: types.BigInt})
			if err != nil {
				return nil, err
			}
		}
		args[i] = arg
	}
	if len(args) == 3 {
		if step, ok := constantRoundPrecision(args[2]); ok && step == 0 {
			return nil, b.errorAt(t.Span, "range step cannot be zero")
		}
	}
	alias := def.Name
	if t.Alias != nil {
		alias = t.Alias.Name
	}
	cols := []RelationColumn{{Name: def.Name, Type: typ, Nullable: def.ID == function.UnnestID}}
	r := &BoundRelation{Kind: RelationApply, ID: fmt.Sprintf("builtin:%s:%p", def.Name, t), Alias: alias, Columns: cols, Scope: makeScope(alias, cols), Source: def.Name, FunctionID: def.ID, Projection: args}
	if !frame {
		return r, nil
	}
	q := &BoundQuery{Source: r, Output: cloneRelationColumns(cols), Projection: []BoundSelectItem{{Expression: &BoundExpression{Kind: BoundColumn, ColumnIndex: 0, Type: typ, Nullable: cols[0].Nullable}, Output: cols[0]}}}
	q, dependent, err := normalizeSiblingQuery(q, true, t.Span)
	if err != nil {
		return nil, err
	}
	return &BoundRelation{Kind: RelationCTE, ID: r.ID, Alias: alias, Columns: cols, Scope: makeScope(alias, cols), CTEQuery: q, Lateral: dependent}, nil
}
