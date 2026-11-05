package planner

import (
	"context"
	"fmt"

	"github.com/steamwo/fssql/internal/core/types"
	"github.com/steamwo/fssql/internal/function"
	"github.com/steamwo/fssql/internal/sql/binder"
)

func (p *QueryPlanner) lowerCorrelatedScalar(ctx context.Context, input *LogicalPlan, expr *binder.BoundExpression) (*LogicalPlan, *binder.BoundExpression, error) {
	inner := copySubquery(expr.Subquery)
	keys, err := extractCorrelation(inner)
	if err != nil {
		return nil, nil, err
	}
	inner.Where = keys.residual
	inner.OrderBy = nil
	if expr.Op == "EXISTS" {
		if queryHasAggregate(inner) && len(inner.GroupBy) == 0 && inner.Having == nil {
			return input, &binder.BoundExpression{Kind: binder.BoundLiteral, Type: types.LogicalType{ID: types.Boolean}, Literal: "true"}, nil
		}
		if len(inner.GroupBy) == 0 && inner.Having != nil {
			return nil, nil, fmt.Errorf("correlated global HAVING requires empty-group decorrelation support")
		}
		input, leftKeys := appendSubqueryValues(input, keys.outer)
		if len(inner.GroupBy) > 0 || inner.Having != nil {
			inner.GroupBy = append(inner.GroupBy, keys.inner...)
		}
		setSubqueryProjection(inner, keys.inner)
		inner.Distinct = true
		right, err := p.PlanStable(ctx, inner)
		if err != nil {
			return nil, nil, err
		}
		rightKeys := make([]int, len(keys.inner))
		for i := range rightKeys {
			rightKeys[i] = i
		}
		markIndex := len(input.Output)
		input, err = subqueryJoin(input, right, leftKeys, rightKeys, JoinLeft)
		if err != nil {
			return nil, nil, err
		}
		return input, &binder.BoundExpression{Kind: binder.BoundIsNull, Type: types.LogicalType{ID: types.Boolean}, Op: "IS NOT NULL", Children: []*binder.BoundExpression{subqueryColumn(input.Output, markIndex)}}, nil
	}
	if len(inner.Output) != 1 {
		return nil, nil, fmt.Errorf("correlated scalar subquery requires one value")
	}
	value := inner.Projection[0].Expression
	if binder.ExpressionCorrelated(value) {
		return nil, nil, fmt.Errorf("outer reference in scalar projection requires delimiter-domain decorrelation")
	}
	if len(inner.GroupBy) > 0 {
		return p.lowerGroupedCorrelatedScalar(ctx, input, inner, keys, value)
	}
	if inner.Having != nil {
		return nil, nil, fmt.Errorf("correlated global HAVING requires empty-group decorrelation support")
	}
	isAggregate := queryHasAggregate(inner)
	baseAggregate := value
	for (baseAggregate.Kind == binder.BoundCast || (baseAggregate.Kind == binder.BoundFunction && baseAggregate.FunctionKind == function.Scalar && baseAggregate.FunctionID >= function.FirstCastFunctionID && baseAggregate.FunctionID < function.FirstDynamicScalarID)) && len(baseAggregate.Children) == 1 {
		baseAggregate = baseAggregate.Children[0]
	}
	if isAggregate && (baseAggregate.Kind != binder.BoundFunction || baseAggregate.FunctionKind != function.Aggregate) {
		return nil, nil, fmt.Errorf("correlated scalar aggregate currently requires a single aggregate call")
	}
	if !isAggregate && inner.Distinct {
		dedup := copySubquery(inner)
		dedupProjection := append(append([]*binder.BoundExpression(nil), keys.inner...), value)
		setSubqueryProjection(dedup, dedupProjection)
		source := &binder.BoundRelation{Kind: binder.RelationCTE, ID: fmt.Sprintf("correlated-distinct:%p", dedup), Columns: dedup.Output, CTEQuery: dedup}
		inner = &binder.BoundQuery{Source: source}
		keys.inner = make([]*binder.BoundExpression, len(keys.inner))
		for i := range keys.inner {
			column := dedup.Output[i]
			keys.inner[i] = &binder.BoundExpression{Kind: binder.BoundColumn, Type: column.Type, Nullable: column.Nullable, ColumnIndex: i}
		}
		column := dedup.Output[len(keys.inner)]
		value = &binder.BoundExpression{Kind: binder.BoundColumn, Type: column.Type, Nullable: column.Nullable, ColumnIndex: len(keys.inner)}
	}
	input, leftKeys := appendSubqueryValues(input, keys.outer)
	inner.GroupBy = append([]*binder.BoundExpression(nil), keys.inner...)
	inner.Distinct = false
	projection := append([]*binder.BoundExpression(nil), keys.inner...)
	if isAggregate {
		projection = append(projection, value)
	} else {
		minimum := &binder.BoundExpression{Kind: binder.BoundFunction, Type: value.Type, Nullable: true, FunctionID: function.MinID, FunctionKind: function.Aggregate, Children: []*binder.BoundExpression{value}}
		projection = append(projection, minimum, aggregateSubqueryExpr(function.CountID, nil))
	}
	setSubqueryProjection(inner, projection)
	right, err := p.PlanStable(ctx, inner)
	if err != nil {
		return nil, nil, err
	}
	rightKeys := make([]int, len(keys.inner))
	for i := range rightKeys {
		rightKeys[i] = i
	}
	offset := len(input.Output)
	input, err = subqueryJoin(input, right, leftKeys, rightKeys, JoinLeft)
	if err != nil {
		return nil, nil, err
	}
	result := subqueryColumn(input.Output, offset+len(keys.inner))
	if !isAggregate {
		return input, &binder.BoundExpression{Kind: binder.BoundFunction, Type: result.Type, Nullable: true, FunctionID: function.ScalarCardinalityID, FunctionKind: function.Scalar, Children: []*binder.BoundExpression{result, subqueryColumn(input.Output, offset+len(keys.inner)+1)}}, nil
	}
	if baseAggregate.FunctionID == function.CountID {
		zero := &binder.BoundExpression{Kind: binder.BoundLiteral, Type: baseAggregate.Type, Literal: "0"}
		if !zero.Type.Same(result.Type) {
			zero = &binder.BoundExpression{Kind: binder.BoundCast, Type: result.Type, Children: []*binder.BoundExpression{zero}}
		}
		result = &binder.BoundExpression{Kind: binder.BoundFunction, Type: result.Type, FunctionID: function.CoalesceID, FunctionKind: function.Scalar, Children: []*binder.BoundExpression{result, zero}}
	}
	return input, result, nil
}

func (p *QueryPlanner) lowerGroupedCorrelatedScalar(ctx context.Context, input *LogicalPlan, inner *binder.BoundQuery, keys correlationKeys, value *binder.BoundExpression) (*LogicalPlan, *binder.BoundExpression, error) {
	input, leftKeys := appendSubqueryValues(input, keys.outer)
	inner.GroupBy = append(inner.GroupBy, keys.inner...)
	setSubqueryProjection(inner, append(append([]*binder.BoundExpression(nil), keys.inner...), value))
	source := &binder.BoundRelation{Kind: binder.RelationCTE, ID: fmt.Sprintf("correlated-groups:%p", inner), Columns: inner.Output, CTEQuery: inner}
	grouped := &binder.BoundQuery{Source: source}
	for i := range keys.inner {
		col := inner.Output[i]
		grouped.GroupBy = append(grouped.GroupBy, &binder.BoundExpression{Kind: binder.BoundColumn, Type: col.Type, Nullable: col.Nullable, ColumnIndex: i})
	}
	col := inner.Output[len(keys.inner)]
	minimum := &binder.BoundExpression{Kind: binder.BoundFunction, Type: col.Type, Nullable: true, FunctionID: function.MinID, FunctionKind: function.Aggregate, Children: []*binder.BoundExpression{{Kind: binder.BoundColumn, Type: col.Type, Nullable: col.Nullable, ColumnIndex: len(keys.inner)}}}
	setSubqueryProjection(grouped, append(append([]*binder.BoundExpression(nil), grouped.GroupBy...), minimum, aggregateSubqueryExpr(function.CountID, nil)))
	right, err := p.PlanStable(ctx, grouped)
	if err != nil {
		return nil, nil, err
	}
	rightKeys := make([]int, len(keys.inner))
	for i := range rightKeys {
		rightKeys[i] = i
	}
	offset := len(input.Output)
	input, err = subqueryJoin(input, right, leftKeys, rightKeys, JoinLeft)
	if err != nil {
		return nil, nil, err
	}
	result := subqueryColumn(input.Output, offset+len(keys.inner))
	count := subqueryColumn(input.Output, offset+len(keys.inner)+1)
	return input, &binder.BoundExpression{Kind: binder.BoundFunction, Type: result.Type, Nullable: true, FunctionID: function.ScalarCardinalityID, FunctionKind: function.Scalar, Children: []*binder.BoundExpression{result, count}}, nil
}
