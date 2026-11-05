package binder

import (
	"context"
	"fmt"

	"github.com/steamwo/fssql/internal/core/types"
	"github.com/steamwo/fssql/internal/sql/ast"
)

func (b *QueryBinder) bindUnionSelect(ctx context.Context, stmt *ast.SelectStmt) (*BoundQuery, error) {
	child := *b
	child.allowNullProjection = true
	left, err := child.BindSelect(ctx, stmt.UnionLeft)
	if err != nil {
		return nil, err
	}
	right, err := child.BindSelect(ctx, stmt.UnionRight)
	if err != nil {
		return nil, err
	}
	if len(left.Output) != len(right.Output) {
		return nil, b.errorAt(stmt.Span, "UNION ALL input column counts differ")
	}
	columns := cloneRelationColumns(left.Output)
	for i := range columns {
		l, r := left.Output[i].Type, right.Output[i].Type
		var target types.LogicalType
		switch {
		case l.ID == types.Invalid && r.ID == types.Invalid:
			target = types.LogicalType{ID: types.Integer}
		case l.ID == types.Invalid:
			target = r
		case r.ID == types.Invalid:
			target = l
		case l.Same(r):
			target = l
		case l.IsNumeric() && r.IsNumeric():
			target, err = commonNumericType(l, r)
		case (l.ID == types.Varchar && !l.Binary) || (r.ID == types.Varchar && !r.Binary):
			target = types.LogicalType{ID: types.Varchar}
		default:
			err = fmt.Errorf("UNION ALL incompatible column types %d and %d", l.ID, r.ID)
		}
		if err != nil {
			return nil, err
		}
		columns[i].Type = cloneLogicalType(target)
		columns[i].Nullable = left.Output[i].Nullable || right.Output[i].Nullable
	}
	coerceQuery := func(q *BoundQuery) (*BoundQuery, error) {
		out := *q
		out.Output = cloneRelationColumns(columns)
		out.Projection = append([]BoundSelectItem(nil), q.Projection...)
		for i := range out.Projection {
			expr := out.Projection[i].Expression
			target := columns[i].Type
			if expr.SQLNull {
				expr, err = New(b.Functions, Scope{}).coerce(expr, target)
			} else if !expr.Type.Same(target) {
				expr = &BoundExpression{Kind: BoundCast, Type: target, Nullable: expr.Nullable, Span: expr.Span, Children: []*BoundExpression{expr}}
			}
			if err != nil {
				return nil, err
			}
			out.Projection[i] = BoundSelectItem{Expression: expr, Output: columns[i]}
		}
		return &out, nil
	}
	left, err = coerceQuery(left)
	if err != nil {
		return nil, err
	}
	right, err = coerceQuery(right)
	if err != nil {
		return nil, err
	}
	source := &BoundRelation{Kind: RelationUnion, Columns: columns, Scope: makeScope("", columns), Children: []*BoundRelation{{Kind: RelationCTE, ID: "union:left", Columns: columns, CTEQuery: left}, {Kind: RelationCTE, ID: "union:right", Columns: columns, CTEQuery: right}}}
	out := &BoundQuery{Source: source, Output: columns}
	for i, col := range columns {
		out.Projection = append(out.Projection, BoundSelectItem{Expression: &BoundExpression{Kind: BoundColumn, Type: col.Type, Nullable: col.Nullable, ColumnIndex: i}, Output: col})
	}
	exprs := b.newExpressionBinder(ctx, source.Scope)
	for _, term := range stmt.OrderBy {
		term, err := b.bindOrderTerm(exprs, columns, term)
		if err != nil {
			return nil, err
		}
		out.OrderBy = append(out.OrderBy, term)
	}
	if stmt.Limit != nil {
		out.Limit, err = b.bindRowCountExpression(exprs, stmt.Limit, "LIMIT")
		if err != nil {
			return nil, err
		}
	}
	if stmt.Offset != nil {
		out.Offset, err = b.bindRowCountExpression(exprs, stmt.Offset, "OFFSET")
		if err != nil {
			return nil, err
		}
	}
	return out, nil
}
