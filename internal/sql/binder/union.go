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
	if stmt.ByName {
		left,right,err = alignUnionByName(left,right)
		if err != nil { return nil,b.errorAt(stmt.Span,err.Error()) }
	}
	if len(left.Output) != len(right.Output) {
		return nil, b.errorAt(stmt.Span, "set operation input column counts differ")
	}
	var orderNames [][]RelationColumn
	if len(stmt.OrderBy) > 0 {
		orderNames = [][]RelationColumn{cloneRelationColumns(left.Output), cloneRelationColumns(right.Output)}
	}
	columns := cloneRelationColumns(left.Output)
	for i := range columns {
		l, r := left.Output[i].Type, right.Output[i].Type
 if l.NullType{l=types.LogicalType{}};if r.NullType{r=types.LogicalType{}}
		var target types.LogicalType
		switch {
		case l.ID == types.Invalid && r.ID == types.Invalid:
			target = types.LogicalType{ID: types.Integer,NullType:true}
		case l.ID == types.Invalid:
			target = r
		case r.ID == types.Invalid:
			target = l
		case l.Same(r):
			target = l
		case l.IsNumeric() && r.IsNumeric():
			target, err = commonNumericType(l, r)
		case l.ID == types.Boolean && r.IsNumeric():
			target = r
		case r.ID == types.Boolean && l.IsNumeric():
			target = l
		case (l.ID == types.DateType || l.ID == types.TimestampType) && (r.ID == types.DateType || r.ID == types.TimestampType):
			target = types.LogicalType{ID: types.TimestampType}
			if l.IsTimestampTZ() || r.IsTimestampTZ() {
				target = types.TimestampTZType()
			} else if l.IsTimestampNS() || r.IsTimestampNS() {
				target = types.TimestampNSType()
			}
		case (l.ID == types.Varchar && !l.Binary) || (r.ID == types.Varchar && !r.Binary):
			target = types.LogicalType{ID: types.Varchar}
		default:
			err = fmt.Errorf("set operation incompatible column types %d and %d", l.ID, r.ID)
		}
		if err != nil {
			return nil, err
		}
		columns[i].Type = cloneLogicalType(target)
		columns[i].Nullable = left.Output[i].Nullable || right.Output[i].Nullable
	}
	coerceQuery := func(q *BoundQuery) (*BoundQuery, error) {
		needsCast := false
		for i, col := range q.Output {
			needsCast = needsCast || !col.Type.Same(columns[i].Type)
		}
		if needsCast && (q.Distinct || len(q.OrderBy) > 0 || q.Limit != nil || q.Offset != nil || len(q.GroupBy) > 0 || q.Having != nil) {
			// Coerce the completed branch, not its pre-DISTINCT/ORDER/LIMIT rows.
			input := &BoundRelation{Kind: RelationCTE, ID: "set:coerce", Columns: cloneRelationColumns(q.Output), CTEQuery: q}
			wrapper := &BoundQuery{Source: input, Output: cloneRelationColumns(q.Output)}
			for i, col := range q.Output {
				wrapper.Projection = append(wrapper.Projection, BoundSelectItem{Expression: &BoundExpression{Kind: BoundColumn, Type: col.Type, Nullable: col.Nullable, ColumnIndex: i}, Output: col})
			}
			q = wrapper
		}
		out := *q
		out.Output = cloneRelationColumns(columns)
		out.Projection = append([]BoundSelectItem(nil), q.Projection...)
		for i := range out.Projection {
			expr := out.Projection[i].Expression
			target := columns[i].Type
			if expr.SQLNull {
				expr, err = New(b.Functions, Scope{}).coerce(expr, target)
			} else if !expr.Type.Same(target) {
				if expr.Type.ID == types.Boolean && target.IsNumeric() {
					expr = &BoundExpression{Kind: BoundCast, Type: types.LogicalType{ID: types.Integer}, Nullable: expr.Nullable, Span: expr.Span, Children: []*BoundExpression{expr}}
				}
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
	if stmt.SetOperation != ast.UnionAll && stmt.SetOperation != ast.UnionDistinct {
		source.Kind = RelationSetOperation
		source.SetOperation = stmt.SetOperation
	}
	out := &BoundQuery{Source: source, Output: columns, Distinct: stmt.SetOperation == ast.UnionDistinct || stmt.SetOperation == ast.IntersectDistinct || stmt.SetOperation == ast.ExceptDistinct}
	for i, col := range columns {
		out.Projection = append(out.Projection, BoundSelectItem{Expression: &BoundExpression{Kind: BoundColumn, Type: col.Type, Nullable: col.Nullable, ColumnIndex: i}, Output: col})
	}
	orderScope := source.Scope
	if len(stmt.OrderBy) > 0 {
		orderScope = setOrderScope(stmt, orderNames, columns)
	}
	exprs := b.newExpressionBinder(ctx, source.Scope)
	for _, term := range stmt.OrderBy {
		term, err = resolveSetOrderAlias(orderScope, term)
		if err != nil {
			return nil, b.errorAt(term.Span, err.Error())
		}
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
	for _, term := range out.OrderBy {
		if out.Distinct && !term.UsesOutput {
			// Evaluate computed ORDER BY keys after set de-duplication, rather
			// than making them part of the DISTINCT row identity.
			inner := *out
			inner.OrderBy = nil
			inner.Limit = nil
			inner.Offset = nil
			out.Source = &BoundRelation{Kind: RelationCTE, ID: "set:ordered", Columns: cloneRelationColumns(columns), CTEQuery: &inner}
			out.Projection = make([]BoundSelectItem, len(columns))
			for i, col := range columns {
				out.Projection[i] = BoundSelectItem{Expression: &BoundExpression{Kind: BoundColumn, Type: col.Type, Nullable: col.Nullable, ColumnIndex: i}, Output: col}
			}
			out.Distinct = false
			break
		}
	}
	return out, nil
}
