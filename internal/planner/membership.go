package planner

import (
	"context"
	"fmt"

	"github.com/steamwo/fssql/internal/core/types"
	"github.com/steamwo/fssql/internal/function"
	"github.com/steamwo/fssql/internal/sql/binder"
)

// The mark is lowered to a deduplicated LEFT JOIN plus per-set statistics.
// This reuses the existing spillable operators and retains NOT IN's NULL case.
func (p *QueryPlanner) lowerMembership(ctx context.Context, input *LogicalPlan, expr *binder.BoundExpression) (*LogicalPlan, *binder.BoundExpression, error) {
	if expr.Subquery == nil || len(expr.Subquery.Output) != 1 || len(expr.Children) != 1 {
		return nil, nil, fmt.Errorf("IN subquery requires one value column")
	}
	inner := copySubquery(expr.Subquery)
	var keys correlationKeys
	if binder.QueryCorrelated(inner) {
		var err error
		keys, err = extractCorrelation(inner)
		if err != nil {
			return nil, nil, err
		}
		if len(inner.GroupBy) == 0 && queryHasAggregate(inner) && inner.Having == nil {
			left, slots := appendSubqueryValues(input, expr.Children)
			scalar := &binder.BoundExpression{Kind: binder.BoundSubquery, Type: inner.Output[0].Type, Nullable: true, Subquery: expr.Subquery}
			left, value, err := p.lowerCorrelatedScalar(ctx, left, scalar)
			if err != nil {
				return nil, nil, err
			}
			return left, &binder.BoundExpression{Kind: binder.BoundBinary, Type: types.LogicalType{ID: types.Boolean}, Nullable: true, Op: "=", Children: []*binder.BoundExpression{subqueryColumn(left.Output, slots[0]), value}}, nil
		}
		if len(inner.GroupBy) == 0 && inner.Having != nil {
			return nil, nil, fmt.Errorf("correlated global HAVING requires empty-group decorrelation support")
		}
		if len(inner.GroupBy) > 0 {
			inner.GroupBy = append(inner.GroupBy, keys.inner...)
		}
		inner.Where = keys.residual
		inner.OrderBy = nil
	}
	if binder.ExpressionCorrelated(inner.Projection[0].Expression) {
		return nil, nil, fmt.Errorf("outer reference in IN projection is not yet supported")
	}
	values := append(append([]*binder.BoundExpression(nil), keys.outer...), expr.Children[0])
	input, leftKeys := appendSubqueryValues(input, values)
	projected := append(append([]*binder.BoundExpression(nil), keys.inner...), inner.Projection[0].Expression)
	setSubqueryProjection(inner, projected)
	// Both consumers refer to the same bound projection, allowing the existing
	// repeated-CTE materializer to evaluate volatile inner values only once.
	columns := append([]binder.RelationColumn(nil), inner.Output...)
	source := &binder.BoundRelation{Kind: binder.RelationCTE, ID: fmt.Sprintf("membership:%p", inner), Columns: columns, CTEQuery: inner, Materialize: true}
	unique := &binder.BoundQuery{Source: source, Distinct: true, Output: columns}
	for i, column := range columns {
		unique.Projection = append(unique.Projection, binder.BoundSelectItem{Expression: &binder.BoundExpression{Kind: binder.BoundColumn, Type: column.Type, Nullable: column.Nullable, ColumnIndex: i}, Output: column})
	}
	right, err := p.PlanStable(ctx, unique)
	if err != nil {
		return nil, nil, err
	}
	rightKeys := make([]int, len(leftKeys))
	for i := range rightKeys {
		rightKeys[i] = i
	}
	markIndex := len(input.Output) + len(columns) - 1
	input, err = subqueryJoin(input, right, leftKeys, rightKeys, JoinLeft)
	if err != nil {
		return nil, nil, err
	}
	stats := &binder.BoundQuery{Source: source}
	for i := range keys.inner {
		column := columns[i]
		key := &binder.BoundExpression{Kind: binder.BoundColumn, Type: column.Type, Nullable: column.Nullable, ColumnIndex: i}
		stats.GroupBy = append(stats.GroupBy, key)
	}
	valueIndex := len(columns) - 1
	value := &binder.BoundExpression{Kind: binder.BoundColumn, Type: columns[valueIndex].Type, Nullable: columns[valueIndex].Nullable, ColumnIndex: valueIndex}
	statsProjection := append([]*binder.BoundExpression(nil), stats.GroupBy...)
	statsProjection = append(statsProjection, aggregateSubqueryExpr(function.CountID, nil), aggregateSubqueryExpr(function.CountID, value))
	setSubqueryProjection(stats, statsProjection)
	statsPlan, err := p.PlanStable(ctx, stats)
	if err != nil {
		return nil, nil, err
	}
	statsOffset := len(input.Output)
	if len(keys.inner) == 0 {
		input, err = subqueryJoin(input, statsPlan, nil, nil, JoinCross)
	} else {
		input, err = subqueryJoin(input, statsPlan, leftKeys[:len(keys.inner)], rightKeys[:len(keys.inner)], JoinLeft)
	}
	if err != nil {
		return nil, nil, err
	}
	mark := &binder.BoundExpression{Kind: binder.BoundFunction, Type: types.LogicalType{ID: types.Boolean}, Nullable: true, FunctionID: function.MembershipMarkID, FunctionKind: function.Scalar, Children: []*binder.BoundExpression{subqueryColumn(input.Output, leftKeys[len(leftKeys)-1]), subqueryColumn(input.Output, markIndex), subqueryColumn(input.Output, statsOffset+len(keys.inner)), subqueryColumn(input.Output, statsOffset+len(keys.inner)+1)}}
	return input, mark, nil
}
