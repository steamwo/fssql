package planner

import (
	"context"
	"fmt"

	"github.com/steamwo/fssql/internal/sql/binder"
)

// PlanStable 是 Stable SELECT 的统一 Planner 入口。
// 当前已接通的 output-modifier 顺序固定为 Project -> DISTINCT -> LIMIT/OFFSET；ORDER/GROUP/subquery 仍由基础 Planner fail-closed。
func (p *QueryPlanner) PlanStable(ctx context.Context, query *binder.BoundQuery) (*LogicalPlan, error) {
	if query == nil {
		return nil, fmt.Errorf("query planner: nil BoundQuery")
	}
	if err := ctx.Err(); err != nil {
		return nil, err
	}

	distinct := query.Distinct
	limitExpr, offsetExpr := query.Limit, query.Offset
	base := *query
	base.Distinct = false
	base.Limit = nil
	base.Offset = nil
	root, err := p.Plan(ctx, &base)
	if err != nil {
		return nil, err
	}

	if distinct {
		root = &LogicalPlan{
			Kind:     LogicalDistinct,
			Children: []*LogicalPlan{root},
			Output:   clonePlannerColumns(root.Output),
		}
		if err := root.Validate(); err != nil {
			return nil, err
		}
	}

	if limitExpr == nil && offsetExpr == nil {
		return root, nil
	}
	limit := UnlimitedRows
	if limitExpr != nil {
		limit, err = foldRowCountExpression(limitExpr, "LIMIT")
		if err != nil {
			return nil, err
		}
	}
	offset := uint64(0)
	if offsetExpr != nil {
		offset, err = foldRowCountExpression(offsetExpr, "OFFSET")
		if err != nil {
			return nil, err
		}
	}
	// OFFSET 0 without LIMIT is a semantic no-op；不额外包 PhysicalLimit。
	if limitExpr == nil && offset == 0 {
		return root, nil
	}
	root = &LogicalPlan{
		Kind:     LogicalLimit,
		Children: []*LogicalPlan{root},
		Output:   clonePlannerColumns(root.Output),
		Limit:    limit,
		Offset:   offset,
	}
	if err := root.Validate(); err != nil {
		return nil, err
	}
	return root, nil
}
