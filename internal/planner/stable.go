package planner

import (
	"context"
	"fmt"

	"github.com/steamwo/fssql/internal/sql/binder"
)

// PlanStable 是 Stable SELECT 的统一 Planner 入口。
// 当前 output-modifier 顺序固定为 Project -> DISTINCT -> ORDER BY -> LIMIT/OFFSET；GROUP/subquery 仍由基础 Planner fail-closed。
func (p *QueryPlanner) PlanStable(ctx context.Context, query *binder.BoundQuery) (*LogicalPlan, error) {
	if query == nil {
		return nil, fmt.Errorf("query planner: nil BoundQuery")
	}
	if err := ctx.Err(); err != nil {
		return nil, err
	}

	distinct := query.Distinct
	orderBy := append([]binder.BoundOrderTerm(nil), query.OrderBy...)
	limitExpr, offsetExpr := query.Limit, query.Offset
	base := *query
	base.Distinct = false
	base.OrderBy = nil
	base.Limit = nil
	base.Offset = nil
	base.Projection = append([]binder.BoundSelectItem(nil), query.Projection...)
	base.Output = cloneStableBoundColumns(query.Output)

	sortKeys, hiddenSortKeys, err := prepareStableOrder(&base, orderBy, distinct)
	if err != nil {
		return nil, err
	}
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

	if len(sortKeys) != 0 {
		root = &LogicalPlan{
			Kind:     LogicalSort,
			Children: []*LogicalPlan{root},
			Output:   clonePlannerColumns(root.Output),
			Sort:     sortKeys,
		}
		if err := root.Validate(); err != nil {
			return nil, err
		}
		if hiddenSortKeys {
			root, err = dropStableHiddenOrderColumns(root, query.Output)
			if err != nil {
				return nil, err
			}
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

// prepareStableOrder 把 Binder ORDER BY term 转成对 projected child 的纯列 SortKey。
// 非输出表达式通过 hidden projection materialize；DISTINCT 不能让 hidden 列参与去重，因此显式拒绝。
func prepareStableOrder(base *binder.BoundQuery, terms []binder.BoundOrderTerm, distinct bool) ([]SortKey, bool, error) {
	if len(terms) == 0 {
		return nil, false, nil
	}
	if base == nil {
		return nil, false, fmt.Errorf("query planner: ORDER BY requires query")
	}
	keys := make([]SortKey, 0, len(terms))
	hidden := false
	for i, term := range terms {
		if term.Expression == nil {
			return nil, false, fmt.Errorf("query planner: ORDER BY term %d is nil", i)
		}
		index := term.OutputIndex
		if term.UsesOutput {
			if index < 0 || index >= len(base.Output) {
				return nil, false, fmt.Errorf("query planner: ORDER BY output index %d out of range", index)
			}
		} else {
			if distinct {
				return nil, false, fmt.Errorf("query planner: SELECT DISTINCT ORDER BY expression must reference projected output")
			}
			index = len(base.Output)
			hiddenColumn := binder.RelationColumn{
				Name:     fmt.Sprintf("__fssql_order_%d", i),
				Type:     cloneQueryPlannerType(term.Expression.Type),
				Nullable: term.Expression.Nullable,
			}
			base.Projection = append(base.Projection, binder.BoundSelectItem{Expression: term.Expression, Output: hiddenColumn})
			base.Output = append(base.Output, hiddenColumn)
			hidden = true
		}
		column := base.Output[index]
		keyExpression := &binder.BoundExpression{
			Kind:        binder.BoundColumn,
			Type:        cloneQueryPlannerType(column.Type),
			Nullable:    column.Nullable,
			ColumnIndex: index,
		}
		var nullsFirst *bool
		if term.NullsFirst != nil {
			value := *term.NullsFirst
			nullsFirst = &value
		}
		keys = append(keys, SortKey{Expr: keyExpression, Descending: term.Descending, NullsFirst: nullsFirst})
	}
	return keys, hidden, nil
}

// dropStableHiddenOrderColumns 在 Sort 后只保留用户 SELECT 输出，hidden sort key 不进入 result ABI。
func dropStableHiddenOrderColumns(child *LogicalPlan, output []binder.RelationColumn) (*LogicalPlan, error) {
	if child == nil || len(output) == 0 || len(child.Output) < len(output) {
		return nil, fmt.Errorf("query planner: invalid hidden ORDER BY projection shape")
	}
	projection := make([]*binder.BoundExpression, len(output))
	for i, column := range output {
		projection[i] = &binder.BoundExpression{
			Kind:        binder.BoundColumn,
			Type:        cloneQueryPlannerType(column.Type),
			Nullable:    column.Nullable,
			ColumnIndex: i,
		}
	}
	root := &LogicalPlan{
		Kind:       LogicalProject,
		Children:   []*LogicalPlan{child},
		Output:     relationColumnsToPlanner(output),
		Projection: projection,
	}
	if err := root.Validate(); err != nil {
		return nil, err
	}
	return root, nil
}

// cloneStableBoundColumns 深拷贝 Binder output metadata，避免 hidden projection 修改调用方 BoundQuery。
func cloneStableBoundColumns(columns []binder.RelationColumn) []binder.RelationColumn {
	out := make([]binder.RelationColumn, len(columns))
	for i, column := range columns {
		out[i] = column
		out[i].Type = cloneQueryPlannerType(column.Type)
	}
	return out
}
