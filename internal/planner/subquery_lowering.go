package planner

import (
	"context"
	"fmt"

	"github.com/steamwo/fssql/internal/core/types"
	"github.com/steamwo/fssql/internal/function"
	"github.com/steamwo/fssql/internal/sql/binder"
)

func plannedSubquery(expr *binder.BoundExpression) bool {
	return expr != nil && expr.Kind == binder.BoundSubquery && (expr.Op == "IN" || binder.QueryCorrelated(expr.Subquery))
}
func hasPlannedSubquery(expr *binder.BoundExpression) bool {
	if expr == nil {
		return false
	}
	if plannedSubquery(expr) {
		return true
	}
	for _, child := range expr.Children {
		if hasPlannedSubquery(child) {
			return true
		}
	}
	return false
}

func (p *QueryPlanner) lowerSubqueryExpression(ctx context.Context, input *LogicalPlan, expr *binder.BoundExpression) (*LogicalPlan, *binder.BoundExpression, error) {
	if binder.ExpressionCorrelated(expr) {
		return nil, nil, fmt.Errorf("outer reference remains outside a supported correlation predicate")
	}
	if !hasPlannedSubquery(expr) {
		return input, expr, nil
	}
	out := *expr
	out.Children = append([]*binder.BoundExpression(nil), expr.Children...)
	for i, child := range out.Children {
		var err error
		input, out.Children[i], err = p.lowerSubqueryExpression(ctx, input, child)
		if err != nil {
			return nil, nil, err
		}
	}
	if out.Kind != binder.BoundSubquery {
		return input, &out, nil
	}
	if out.Op == "IN" && out.Comparison != "" { return p.lowerDomainSubquery(ctx,input,&out) }
	if binder.QueryCorrelated(out.Subquery) && needsDomainDecorrelation(out.Subquery) {
		return p.lowerDomainSubquery(ctx, input, &out)
	}
	if out.Op == "IN" {
		return p.lowerMembership(ctx, input, &out)
	}
	return p.lowerCorrelatedScalar(ctx, input, &out)
}

func subqueryColumn(columns []Column, index int) *binder.BoundExpression {
	c := columns[index]
	return &binder.BoundExpression{Kind: binder.BoundColumn, Type: c.Type, Nullable: c.Nullable, ColumnIndex: index}
}
func appendSubqueryValues(input *LogicalPlan, values []*binder.BoundExpression) (*LogicalPlan, []int) {
	projection := passThrough(input.Output)
	output := clonePlannerColumns(input.Output)
	indexes := make([]int, len(values))
	for i, value := range values {
		indexes[i] = len(output)
		projection = append(projection, value)
		output = append(output, Column{Name: fmt.Sprintf("__subquery_%d", len(output)), Type: value.Type, Nullable: value.Nullable})
	}
	return &LogicalPlan{Kind: LogicalProject, Children: []*LogicalPlan{input}, Output: output, Projection: projection}, indexes
}
func subqueryJoin(input, right *LogicalPlan, leftKeys, rightKeys []int, kind JoinType) (*LogicalPlan, error) {
	if len(leftKeys) != len(rightKeys) {
		return nil, fmt.Errorf("subquery join key count mismatch")
	}
	var predicate *binder.BoundExpression
	for i, left := range leftKeys {
		r := subqueryColumn(right.Output, rightKeys[i])
		r.ColumnIndex += len(input.Output)
		eq := &binder.BoundExpression{Kind: binder.BoundBinary, Type: types.LogicalType{ID: types.Boolean}, Nullable: true, Op: "=", Children: []*binder.BoundExpression{subqueryColumn(input.Output, left), r}}
		if predicate == nil {
			predicate = eq
		} else {
			predicate = andExpression(predicate, eq)
		}
	}
	output := clonePlannerColumns(input.Output)
	for _, column := range right.Output {
		if kind == JoinSemi || kind == JoinAnti {
			break
		}
		if kind == JoinLeft {
			column.Nullable = true
		}
		output = append(output, column)
	}
	return &LogicalPlan{Kind: LogicalJoin, Children: []*LogicalPlan{input, right}, Output: output, Join: kind, Predicate: predicate}, nil
}
func copySubquery(q *binder.BoundQuery) *binder.BoundQuery {
	out := *q
	out.Projection = append([]binder.BoundSelectItem(nil), q.Projection...)
	out.Output = append([]binder.RelationColumn(nil), q.Output...)
	out.GroupBy = append([]*binder.BoundExpression(nil), q.GroupBy...)
	out.OrderBy = append([]binder.BoundOrderTerm(nil), q.OrderBy...)
	return &out
}
func setSubqueryProjection(q *binder.BoundQuery, exprs []*binder.BoundExpression) {
	q.Projection = nil
	q.Output = nil
	for i, expr := range exprs {
		column := binder.RelationColumn{Name: fmt.Sprintf("__sq_%d", i), Type: expr.Type, Nullable: expr.Nullable}
		q.Projection = append(q.Projection, binder.BoundSelectItem{Expression: expr, Output: column})
		q.Output = append(q.Output, column)
	}
}
func aggregateSubqueryExpr(id function.ID, value *binder.BoundExpression) *binder.BoundExpression {
	var children []*binder.BoundExpression
	if value != nil {
		children = []*binder.BoundExpression{value}
	}
	return &binder.BoundExpression{Kind: binder.BoundFunction, Type: types.LogicalType{ID: types.BigInt}, Nullable: id != function.CountID, FunctionID: id, FunctionKind: function.Aggregate, Children: children}
}

type correlationKeys struct {
	inner, outer []*binder.BoundExpression
	residual     *binder.BoundExpression
}

func extractCorrelation(q *binder.BoundQuery) (correlationKeys, error) {
	var keys correlationKeys
	var visit func(*binder.BoundExpression) error
	visit = func(expr *binder.BoundExpression) error {
		if expr == nil {
			return nil
		}
		if expr.Kind == binder.BoundBinary && expr.Op == "AND" && len(expr.Children) == 2 {
			if err := visit(expr.Children[0]); err != nil {
				return err
			}
			return visit(expr.Children[1])
		}
		if !binder.ExpressionCorrelated(expr) {
			if keys.residual == nil {
				keys.residual = expr
			} else {
				keys.residual = andExpression(keys.residual, expr)
			}
			return nil
		}
		if expr.Kind != binder.BoundBinary || expr.Op != "=" || len(expr.Children) != 2 {
			return fmt.Errorf("correlation currently requires AND-connected equality predicates")
		}
		left, right := expr.Children[0], expr.Children[1]
		if binder.ExpressionCorrelated(right) {
			left, right = right, left
		}
		if !binder.ExpressionCorrelated(left) || hasLocalSubqueryColumn(left) || binder.ExpressionCorrelated(right) || !hasLocalSubqueryColumn(right) {
			return fmt.Errorf("correlation equality must compare outer and inner expressions")
		}
		outer, err := localizeOuter(left)
		if err != nil {
			return err
		}
		keys.outer = append(keys.outer, outer)
		keys.inner = append(keys.inner, right)
		return nil
	}
	if err := visit(q.Where); err != nil {
		return keys, err
	}
	if len(keys.inner) == 0 {
		return keys, fmt.Errorf("correlated subquery requires an equality key in WHERE")
	}
	if q.Limit != nil || q.Offset != nil {
		return keys, fmt.Errorf("correlated LIMIT/OFFSET is not yet supported")
	}
	for _, group := range q.GroupBy {
		if binder.ExpressionCorrelated(group) {
			return keys, fmt.Errorf("outer reference in inner GROUP BY is not yet supported")
		}
	}
	if binder.ExpressionCorrelated(q.Having) {
		return keys, fmt.Errorf("outer reference in inner HAVING is not yet supported")
	}
	return keys, nil
}
func hasLocalSubqueryColumn(expr *binder.BoundExpression) bool {
	if expr == nil {
		return false
	}
	if expr.Kind == binder.BoundColumn {
		return true
	}
	for _, child := range expr.Children {
		if hasLocalSubqueryColumn(child) {
			return true
		}
	}
	return false
}
func localizeOuter(expr *binder.BoundExpression) (*binder.BoundExpression, error) {
	out := *expr
	if expr.Kind == binder.BoundOuterColumn {
		if expr.Depth != 1 {
			return nil, fmt.Errorf("multi-level correlation is not yet supported")
		}
		out.Kind = binder.BoundColumn
		out.Depth = 0
	}
	out.Children = make([]*binder.BoundExpression, len(expr.Children))
	for i, child := range expr.Children {
		var err error
		out.Children[i], err = localizeOuter(child)
		if err != nil {
			return nil, err
		}
	}
	return &out, nil
}
