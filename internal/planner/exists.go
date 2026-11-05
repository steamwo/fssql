package planner

import (
	"context"

	"github.com/steamwo/fssql/internal/core/types"
	"github.com/steamwo/fssql/internal/function"
	"github.com/steamwo/fssql/internal/sql/binder"
)

// PrepareExistsQuery removes projections that cannot affect row presence.
// Callers can apply this before recursively preparing scalar dependencies.
func PrepareExistsQuery(query *binder.BoundQuery) *binder.BoundQuery {
	base := *query
	base.OrderBy = nil
	// DISTINCT values matter only when OFFSET can remove the sole distinct row.
	if !base.Distinct || base.Offset == nil {
		base.Distinct = false
		var expr *binder.BoundExpression
		if queryHasAggregate(query) {
			expr = &binder.BoundExpression{Kind: binder.BoundFunction, Type: types.LogicalType{ID: types.BigInt}, FunctionID: function.CountID, FunctionKind: function.Aggregate}
		} else {
			expr = &binder.BoundExpression{Kind: binder.BoundLiteral, Type: types.LogicalType{ID: types.Boolean}, Literal: "true"}
		}
		column := binder.RelationColumn{Name: "__exists_input", Type: expr.Type}
		base.Projection = []binder.BoundSelectItem{{Expression: expr, Output: column}}
		base.Output = []binder.RelationColumn{column}
	}
	return &base
}

func (p *QueryPlanner) PlanExists(ctx context.Context, query *binder.BoundQuery) (*LogicalPlan, error) {
	input, err := p.PlanStable(ctx, PrepareExistsQuery(query))
	if err != nil {
		return nil, err
	}
	input = pruneExistsProjection(input, map[int]bool{})
	input = &LogicalPlan{Kind: LogicalLimit, Children: []*LogicalPlan{input}, Output: clonePlannerColumns(input.Output), Limit: 1}
	count := Column{Name: "__exists_count", Type: types.LogicalType{ID: types.BigInt}}
	aggregate := &LogicalPlan{Kind: LogicalAggregate, Children: []*LogicalPlan{input}, Output: []Column{count}, Aggregates: []AggregateSpec{{FunctionID: uint16(function.CountID), Output: count}}}
	expr := &binder.BoundExpression{Kind: binder.BoundBinary, Type: types.LogicalType{ID: types.Boolean}, Op: ">", Children: []*binder.BoundExpression{{Kind: binder.BoundColumn, Type: count.Type, ColumnIndex: 0}, {Kind: binder.BoundLiteral, Type: count.Type, Literal: "0"}}}
	return &LogicalPlan{Kind: LogicalProject, Children: []*LogicalPlan{aggregate}, Output: []Column{{Name: "exists", Type: expr.Type}}, Projection: []*binder.BoundExpression{expr}}, nil
}
