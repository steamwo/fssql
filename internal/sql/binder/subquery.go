package binder

import (
	"fmt"

	"github.com/steamwo/fssql/internal/core/types"
	"github.com/steamwo/fssql/internal/sql/ast"
)

type missingColumnError struct{ name string }

func (e *missingColumnError) Error() string { return fmt.Sprintf("column %s not found", e.name) }

func resolveSubqueryNull(q *BoundQuery, index int, target types.LogicalType, b *Binder) error {
	expr, err := b.coerce(q.Projection[index].Expression, target)
	if err != nil {
		return err
	}
	q.Projection[index].Expression = expr
	q.Projection[index].Output.Type = target
	q.Output[index].Type = target
	return nil
}

func (b *Binder) bindInSubquery(e *ast.InSubqueryExpr) (*BoundExpression, error) {
	if b.Subqueries == nil {
		return nil, b.errorAt(e.Span, "IN subquery requires query binder")
	}
	value, err := b.BindExpression(e.Value)
	if err != nil {
		return nil, err
	}
	q, err := b.Subqueries(e.Query)
	if err != nil {
		return nil, err
	}
	if q == nil || len(q.Output) != 1 {
		return nil, b.errorAt(e.Span, "IN subquery must return exactly one column")
	}
	item := q.Projection[0].Expression
	left, right, err := b.coerceComparable(value, item)
	if err != nil {
		return nil, b.errorAt(e.Span, err.Error())
	}
	q.Projection[0].Expression = right
	q.Projection[0].Output.Type = right.Type
	q.Output[0].Type = right.Type
	for i, term := range q.OrderBy {
		if term.UsesOutput && term.OutputIndex == 0 {
			copyExpr := *term.Expression
			copyExpr.Type = right.Type
			q.OrderBy[i].Expression = &copyExpr
		}
	}
	expr := &BoundExpression{Kind: BoundSubquery, Type: types.LogicalType{ID: types.Boolean}, Nullable: true, Span: e.Span, Op: "IN", Children: []*BoundExpression{left}, Subquery: q}
	if e.Not {
		return &BoundExpression{Kind: BoundUnary, Type: expr.Type, Nullable: true, Span: e.Span, Op: "NOT", Children: []*BoundExpression{expr}}, nil
	}
	return expr, nil
}

// QueryCorrelated detects outer references in this query block, without
// treating a separate nested subquery's own correlation as this block's.
func QueryCorrelated(q *BoundQuery) bool {
	return queryCorrelatedAtDepth(q,0)
}
func ExpressionCorrelated(expr *BoundExpression) bool {
	if expr == nil {
		return false
	}
	if expr.Kind == BoundOuterColumn {
		return true
	}
	for _, child := range expr.Children {
		if ExpressionCorrelated(child) {
			return true
		}
	}
	return false
}
