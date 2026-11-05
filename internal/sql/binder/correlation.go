package binder

func expressionCorrelatedAtDepth(e *BoundExpression, depth int) bool {
	if e == nil {
		return false
	}
	if e.Kind == BoundOuterColumn && e.Depth > depth {
		return true
	}
	if queryCorrelatedAtDepth(e.Subquery, depth+1) {
		return true
	}
	for _, c := range e.Children {
		if expressionCorrelatedAtDepth(c, depth) {
			return true
		}
	}
	return false
}
func relationCorrelatedAtDepth(r *BoundRelation, depth int) bool {
	if r == nil {
		return false
	}
	queryDepth := depth
	if r.Lateral {
		queryDepth++
	}
	if expressionCorrelatedAtDepth(r.Condition, depth) || queryCorrelatedAtDepth(r.CTEQuery, queryDepth) {
		return true
	}
	for _, e := range r.Projection {
		if expressionCorrelatedAtDepth(e, depth) {
			return true
		}
	}
	for _, c := range r.Children {
		if relationCorrelatedAtDepth(c, depth) {
			return true
		}
	}
	return false
}
func queryCorrelatedAtDepth(q *BoundQuery, depth int) bool {
	if q == nil {
		return false
	}
	if relationCorrelatedAtDepth(q.Source, depth) {
		return true
	}
	for _, item := range q.Projection {
		if expressionCorrelatedAtDepth(item.Expression, depth) {
			return true
		}
	}
	for _, e := range []*BoundExpression{q.Where, q.Having, q.Limit, q.Offset} {
		if expressionCorrelatedAtDepth(e, depth) {
			return true
		}
	}
	for _, e := range q.GroupBy {
		if expressionCorrelatedAtDepth(e, depth) {
			return true
		}
	}
	for _, term := range q.OrderBy {
		if expressionCorrelatedAtDepth(term.Expression, depth) {
			return true
		}
	}
	return false
}

// RewriteOuterReferences copies the query tree and rewrites references that
// escape its root query block. depth is the number of nested query boundaries.
// References owned by a nested query's own parent are left untouched.
func RewriteOuterReferences(q *BoundQuery, rewrite func(*BoundExpression, int) (*BoundExpression, error)) (*BoundQuery, error) {
	return RewriteQueryExpressions(q, func(e *BoundExpression, depth int) (*BoundExpression, error) {
		if e.Kind == BoundOuterColumn && e.Depth > depth {
			return rewrite(e, depth)
		}
		return e, nil
	})
}

// RewriteQueryExpressions visits expressions before their children and copies
// the query tree. Returning a replacement skips that expression's descendants.
// Expression subqueries and lateral sibling scopes add depth; ordinary
// derived relations retain their inherited binding scope.
func RewriteQueryExpressions(q *BoundQuery, rewrite func(*BoundExpression, int) (*BoundExpression, error)) (*BoundQuery, error) {
	var query func(*BoundQuery, int) (*BoundQuery, error)
	var relation func(*BoundRelation, int) (*BoundRelation, error)
	var expression func(*BoundExpression, int) (*BoundExpression, error)
	expression = func(e *BoundExpression, depth int) (*BoundExpression, error) {
		if e == nil {
			return nil, nil
		}
		replacement, err := rewrite(e, depth)
		if err != nil {
			return nil, err
		}
		if replacement != e {
			return replacement, nil
		}
		out := *e
		out.Subquery, err = query(e.Subquery, depth+1)
		if err != nil {
			return nil, err
		}
		out.Children = make([]*BoundExpression, len(e.Children))
		for i, child := range e.Children {
			out.Children[i], err = expression(child, depth)
			if err != nil {
				return nil, err
			}
		}
		return &out, nil
	}
	relation = func(r *BoundRelation, depth int) (*BoundRelation, error) {
		if r == nil {
			return nil, nil
		}
		out := *r
		var err error
		out.Condition, err = expression(r.Condition, depth)
		if err != nil {
			return nil, err
		}
		queryDepth := depth
		if r.Lateral {
			queryDepth++
		}
		out.CTEQuery, err = query(r.CTEQuery, queryDepth)
		if err != nil {
			return nil, err
		}
		out.Projection = make([]*BoundExpression, len(r.Projection))
		for i, e := range r.Projection {
			out.Projection[i], err = expression(e, depth)
			if err != nil {
				return nil, err
			}
		}
		out.Children = make([]*BoundRelation, len(r.Children))
		for i, child := range r.Children {
			out.Children[i], err = relation(child, depth)
			if err != nil {
				return nil, err
			}
		}
		return &out, nil
	}
	query = func(q *BoundQuery, depth int) (*BoundQuery, error) {
		if q == nil {
			return nil, nil
		}
		out := *q
		var err error
		out.Source, err = relation(q.Source, depth)
		if err != nil {
			return nil, err
		}
		out.Projection = append([]BoundSelectItem(nil), q.Projection...)
		for i, item := range q.Projection {
			out.Projection[i].Expression, err = expression(item.Expression, depth)
			if err != nil {
				return nil, err
			}
		}
		out.GroupBy = make([]*BoundExpression, len(q.GroupBy))
		for i, e := range q.GroupBy {
			out.GroupBy[i], err = expression(e, depth)
			if err != nil {
				return nil, err
			}
		}
		out.OrderBy = append([]BoundOrderTerm(nil), q.OrderBy...)
		for i, term := range q.OrderBy {
			out.OrderBy[i].Expression, err = expression(term.Expression, depth)
			if err != nil {
				return nil, err
			}
		}
		pairs := []struct {
			src *BoundExpression
			dst **BoundExpression
		}{{q.Where, &out.Where}, {q.Having, &out.Having}, {q.Limit, &out.Limit}, {q.Offset, &out.Offset}}
		for _, pair := range pairs {
			*pair.dst, err = expression(pair.src, depth)
			if err != nil {
				return nil, err
			}
		}
		return &out, nil
	}
	return query(q, 0)
}
