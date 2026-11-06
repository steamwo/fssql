package session

import (
	"context"
	"fmt"
	"strconv"

	"github.com/steamwo/fssql/internal/optimizer"
	"github.com/steamwo/fssql/internal/planner"
	"github.com/steamwo/fssql/internal/sql/binder"
)

type scalarQueryRewriter struct {
	ctx       context.Context
	planner   *planner.QueryPlanner
	optimizer *optimizer.Optimizer
	prepared  []preparedScalarSubquery
	queries   map[*binder.BoundQuery]*binder.BoundQuery
}

func (r *scalarQueryRewriter) rewriteQuery(query *binder.BoundQuery) (*binder.BoundQuery, error) {
	return r.rewriteQueryMode(query, false)
}

func (r *scalarQueryRewriter) rewriteExistsQuery(query *binder.BoundQuery) (*binder.BoundQuery, error) {
	if !binder.QueryCorrelated(query) {
		return r.rewriteQuery(planner.PrepareExistsQuery(query))
	}
	// Preserve outer-owned aggregates for the enclosing collector, while
	// avoiding eager preparation of unused SELECT/ORDER BY scalar children.
	return r.rewriteQueryMode(query, !query.Distinct || query.Offset == nil)
}

func (r *scalarQueryRewriter) rewriteQueryMode(query *binder.BoundQuery, skipProjection bool) (*binder.BoundQuery, error) {
	if query == nil || !queryTreeContainsScalarSubquery(query) {
		return query, nil
	}
	if previous, ok := r.queries[query]; ok {
		return previous, nil
	}
	out := *query
	r.queries[query] = &out
	var err error
	out.Source, err = r.rewriteRelation(query.Source)
	if err != nil {
		return nil, err
	}
	out.Projection = append([]binder.BoundSelectItem(nil), query.Projection...)
	for i := range out.Projection {
		if skipProjection {
			break
		}
		out.Projection[i].Expression, err = r.rewriteExpr(query.Projection[i].Expression)
		if err != nil {
			return nil, err
		}
	}
	out.Where, err = r.rewriteExpr(query.Where)
	if err != nil {
		return nil, err
	}
	out.Having, err = r.rewriteExpr(query.Having)
	if err != nil {
		return nil, err
	}
	out.OrderBy = append([]binder.BoundOrderTerm(nil), query.OrderBy...)
	for i := range out.OrderBy {
		if skipProjection {
			break
		}
		out.OrderBy[i].Expression, err = r.rewriteExpr(query.OrderBy[i].Expression)
		if err != nil {
			return nil, err
		}
	}
	return &out, nil
}

func (r *scalarQueryRewriter) rewriteRelation(relation *binder.BoundRelation) (*binder.BoundRelation, error) {
	if relation == nil {
		return nil, nil
	}
	out := *relation
	var err error
	out.Condition, err = r.rewriteExpr(relation.Condition)
	if err != nil {
		return nil, err
	}
	out.Projection = append([]*binder.BoundExpression(nil), relation.Projection...)
	for i, e := range relation.Projection {
		out.Projection[i], err = r.rewriteExpr(e)
		if err != nil {
			return nil, err
		}
	}
	out.CTEQuery, err = r.rewriteQuery(relation.CTEQuery)
	if err != nil {
		return nil, err
	}
	out.Children = make([]*binder.BoundRelation, len(relation.Children))
	for i := range out.Children {
		out.Children[i], err = r.rewriteRelation(relation.Children[i])
		if err != nil {
			return nil, err
		}
	}
	return &out, nil
}

func (r *scalarQueryRewriter) rewriteExpr(expr *binder.BoundExpression) (*binder.BoundExpression, error) {
	if expr == nil || !expressionContainsScalarSubquery(expr) {
		return expr, nil
	}
	if expr.Kind == binder.BoundSubquery && (expr.Op == "IN" || binder.QueryCorrelated(expr.Subquery)) {
		out := *expr
		out.Children = append([]*binder.BoundExpression(nil), expr.Children...)
		for i, child := range out.Children {
			var err error
			out.Children[i], err = r.rewriteExpr(child)
			if err != nil {
				return nil, err
			}
		}
		var inner *binder.BoundQuery
		var err error
		if expr.Op == "EXISTS" {
			inner, err = r.rewriteExistsQuery(expr.Subquery)
		} else {
			inner, err = r.rewriteQuery(expr.Subquery)
		}
		if err != nil {
			return nil, err
		}
		out.Subquery = inner
		return &out, nil
	}
	if expr.Kind == binder.BoundSubquery {
		if expr.Subquery == nil || (expr.Op != "EXISTS" && len(expr.Subquery.Output) != 1) {
			return nil, fmt.Errorf("session: scalar subquery must have one bound output")
		}
		var logical *planner.LogicalPlan
		var err error
		var inner *binder.BoundQuery
		if expr.Op == "EXISTS" {
			inner, err = r.rewriteExistsQuery(expr.Subquery)
		} else {
			inner, err = r.rewriteQuery(expr.Subquery)
		}
		if err != nil {
			return nil, err
		}
		if expr.Op == "EXISTS" {
			logical, err = r.planner.PlanExists(r.ctx, inner)
		} else {
			logical, err = r.planner.PlanStable(r.ctx, inner)
		}
		if err != nil {
			return nil, fmt.Errorf("session: plan scalar subquery: %w", err)
		}
		logical, err = planner.RewriteQueryLogical(logical)
		if err != nil {
			return nil, err
		}
		physical, err := r.optimizer.Optimize(r.ctx, logical)
		if err != nil {
			return nil, fmt.Errorf("session: optimize scalar subquery: %w", err)
		}
		id := len(r.prepared)
		r.prepared = append(r.prepared, preparedScalarSubquery{id: id, physical: physical, typeID: cloneSourceType(expr.Type)})
		return &binder.BoundExpression{Kind: binder.BoundRuntimeConstant, Span: expr.Span, Type: cloneSourceType(expr.Type), Nullable: true, Literal: strconv.Itoa(id)}, nil
	}
	out := *expr
	out.Children = make([]*binder.BoundExpression, len(expr.Children))
	for i, child := range expr.Children {
		var err error
		out.Children[i], err = r.rewriteExpr(child)
		if err != nil {
			return nil, err
		}
	}
	return &out, nil
}

func queryTreeContainsScalarSubquery(query *binder.BoundQuery) bool {
	return query != nil && (queryContainsScalarSubquery(query) || relationContainsScalarSubquery(query.Source))
}

func relationContainsScalarSubquery(relation *binder.BoundRelation) bool {
	if relation == nil {
		return false
	}
	if expressionContainsScalarSubquery(relation.Condition) {
		return true
	}
	for _, e := range relation.Projection {
		if expressionContainsScalarSubquery(e) {
			return true
		}
	}
	if queryTreeContainsScalarSubquery(relation.CTEQuery) {
		return true
	}
	for _, child := range relation.Children {
		if relationContainsScalarSubquery(child) {
			return true
		}
	}
	return false
}
