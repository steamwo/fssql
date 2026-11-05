package binder

import (
	"fmt"

	"github.com/steamwo/fssql/internal/function"
)

// aggregateArgumentDepth identifies the closest scope directly referenced by
// aggregate arguments. Subqueries are separate scopes and do not contribute
// direct references; arguments without direct columns aggregate locally.
func aggregateArgumentDepth(arguments []*BoundExpression) int {
	depth := 0
	local := false
	var visit func(*BoundExpression)
	visit = func(e *BoundExpression) {
		if e == nil {
			return
		}
		if e.Kind == BoundSubquery {
			return
		}
		if e.Kind == BoundColumn || (e.Kind == BoundFunction && e.FunctionKind == function.Aggregate && e.Depth == 0) {
			local = true
			return
		}
		if e.Kind == BoundOuterColumn && (depth == 0 || e.Depth < depth) {
			depth = e.Depth
		}
		for _, child := range e.Children {
			visit(child)
		}
	}
	for _, e := range arguments {
		visit(e)
	}
	if local {
		return 0
	}
	return depth
}

// Walk aggregates owned by the expression's query scope, even when their
// syntax occurs in a nested subquery. Local aggregates of nested blocks do not
// belong to this scope. The walker also enforces same-scope aggregate nesting.
func walkOwnedAggregates(e *BoundExpression, inside bool, visit func(*BoundExpression) error) error {
	var expression func(*BoundExpression, int, bool) error
	var query func(*BoundQuery, int, bool) error
	var relation func(*BoundRelation, int, bool) error
	expression = func(e *BoundExpression, depth int, inside bool) error {
		if e == nil {
			return nil
		}
		owned := e.Kind == BoundFunction && e.FunctionKind == function.Aggregate && e.Depth == depth
		if owned {
			if inside {
				return fmt.Errorf("nested aggregate is not allowed")
			}
			if visit != nil {
				if err := visit(e); err != nil {
					return err
				}
			}
		}
		for _, child := range e.Children {
			if err := expression(child, depth, inside || owned); err != nil {
				return err
			}
		}
		return query(e.Subquery, depth+1, inside || owned)
	}
	relation = func(r *BoundRelation, depth int, inside bool) error {
		if r == nil {
			return nil
		}
		if err := expression(r.Condition, depth, inside); err != nil {
			return err
		}
		for _, e := range r.Projection {
			if err := expression(e, depth, inside); err != nil {
				return err
			}
		}
		for _, child := range r.Children {
			if err := relation(child, depth, inside); err != nil {
				return err
			}
		}
		if r.Lateral {
			depth++
		}
		return query(r.CTEQuery, depth, inside)
	}
	query = func(q *BoundQuery, depth int, inside bool) error {
		if q == nil {
			return nil
		}
		for _, item := range q.Projection {
			if err := expression(item.Expression, depth, inside); err != nil {
				return err
			}
		}
		for _, e := range []*BoundExpression{q.Where, q.Having, q.Limit, q.Offset} {
			if err := expression(e, depth, inside); err != nil {
				return err
			}
		}
		for _, e := range q.GroupBy {
			if err := expression(e, depth, inside); err != nil {
				return err
			}
		}
		for _, term := range q.OrderBy {
			if err := expression(term.Expression, depth, inside); err != nil {
				return err
			}
		}
		return relation(q.Source, depth, inside)
	}
	return expression(e, 0, inside)
}

// ExpressionHasLocalAggregate includes outer-owned calls written in a child
// subquery while excluding child-owned aggregate calls.
func ExpressionHasLocalAggregate(e *BoundExpression) bool {
	found := false
	_ = walkOwnedAggregates(e, false, func(*BoundExpression) error { found = true; return nil })
	return found
}
