package planner

import (
	"context"
	"fmt"

	"github.com/steamwo/fssql/internal/function"
	"github.com/steamwo/fssql/internal/sql/binder"
)

// planStableAggregateQuery 把 Binder 已验证的 GROUP BY/aggregate/HAVING 查询改写成
// pre-project -> aggregate -> having -> final-project。所有 aggregate 输入先 materialize 成普通列，
// execution 层因此无需重新解释 SQL expression。
func (p *QueryPlanner) planStableAggregateQuery(ctx context.Context, query *binder.BoundQuery) (*LogicalPlan, error) {
	if query == nil {
		return nil, fmt.Errorf("query planner: nil aggregate query")
	}
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	root := &LogicalPlan{Kind: LogicalOneRow}
	if query.Source != nil {
		var err error
		root, err = p.planRelation(ctx, query.Source)
		if err != nil {
			return nil, err
		}
	}
	if query.Where != nil {
		var predicate *binder.BoundExpression
		var err error
		root, predicate, err = p.lowerSubqueryExpression(ctx, root, query.Where)
		if err != nil {
			return nil, err
		}
		root = &LogicalPlan{Kind: LogicalFilter, Children: []*LogicalPlan{root}, Output: clonePlannerColumns(root.Output), Predicate: predicate}
	}

	collector := newAggregateCollector(query.GroupBy)
	for _, item := range query.Projection {
		if err := collector.collect(item.Expression); err != nil {
			return nil, err
		}
	}
	if err := collector.collect(query.Having); err != nil {
		return nil, err
	}

	// Aggregate 输入布局固定为 group expressions 在前，aggregate arguments 在后。
	preProjection := make([]*binder.BoundExpression, 0, len(collector.groups)+len(collector.arguments))
	preOutput := make([]Column, 0, len(collector.groups)+len(collector.arguments))
	groupColumns := make([]*binder.BoundExpression, len(collector.groups))
	for i, group := range collector.groups {
		preProjection = append(preProjection, cloneAggregateExpression(group))
		preOutput = append(preOutput, Column{Name: fmt.Sprintf("__fssql_group_%d", i), Type: cloneQueryPlannerType(group.Type), Nullable: group.Nullable})
		groupColumns[i] = &binder.BoundExpression{Kind: binder.BoundColumn, Type: cloneQueryPlannerType(group.Type), Nullable: group.Nullable, ColumnIndex: i}
	}
	for i, argument := range collector.arguments {
		preProjection = append(preProjection, cloneAggregateExpression(argument))
		preOutput = append(preOutput, Column{Name: fmt.Sprintf("__fssql_agg_arg_%d", i), Type: cloneQueryPlannerType(argument.Type), Nullable: argument.Nullable})
	}
	for i, expr := range preProjection {
		var err error
		root, preProjection[i], err = p.lowerSubqueryExpression(ctx, root, expr)
		if err != nil {
			return nil, err
		}
	}
	if len(preProjection) != 0 {
		root = &LogicalPlan{Kind: LogicalProject, Children: []*LogicalPlan{root}, Output: preOutput, Projection: preProjection}
	}

	sharedDistinct, rewriteDistinct := collector.sharedDistinctArgument()
	aggregates := make([]AggregateSpec, len(collector.aggregates))
	distinctArguments := map[int]int{}
	aggregateOutput := make([]Column, 0, len(collector.groups)+len(collector.aggregates))
	aggregateOutput = append(aggregateOutput, preOutput[:len(collector.groups)]...)
	for i, aggregateExpr := range collector.aggregates {
		arguments := make([]*binder.BoundExpression, len(aggregateExpr.Children))
		for j, child := range aggregateExpr.Children {
			argumentIndex, ok := collector.argumentIndex(child)
			if !ok {
				return nil, fmt.Errorf("query planner: aggregate argument provenance missing")
			}
			columnIndex := len(collector.groups) + argumentIndex
			if !rewriteDistinct && aggregateExpr.Distinct && aggregateExpr.FunctionID != function.MinID && aggregateExpr.FunctionID != function.MaxID {
				if marked, ok := distinctArguments[columnIndex]; ok {
					columnIndex = marked
				} else {
					var err error
					original := columnIndex
					root, columnIndex, err = markDistinctArgument(root, groupColumns, columnIndex)
					if err != nil {
						return nil, err
					}
					distinctArguments[original] = columnIndex
				}
			}
			arguments[j] = &binder.BoundExpression{Kind: binder.BoundColumn, Type: cloneQueryPlannerType(child.Type), Nullable: child.Nullable, ColumnIndex: columnIndex}
		}
		output := Column{Name: fmt.Sprintf("__fssql_aggregate_%d", i), Type: cloneQueryPlannerType(aggregateExpr.Type), Nullable: aggregateExpr.Nullable}
		aggregates[i] = AggregateSpec{FunctionID: uint16(aggregateExpr.FunctionID), Arguments: arguments, Output: output}
		aggregateOutput = append(aggregateOutput, output)
	}
	if rewriteDistinct {
		var err error
		root, err = twoLevelDistinctAggregate(root, groupColumns, preOutput, aggregates, aggregateOutput, sharedDistinct)
		if err != nil {
			return nil, err
		}
	} else {
		root = &LogicalPlan{
			Kind:       LogicalAggregate,
			Children:   []*LogicalPlan{root},
			Output:     aggregateOutput,
			GroupBy:    groupColumns,
			Aggregates: aggregates,
		}
	}
	if err := root.Validate(); err != nil {
		return nil, err
	}

	if query.Having != nil {
		predicate, err := collector.rewritePostAggregate(query.Having)
		if err != nil {
			return nil, err
		}
		root, predicate, err = p.lowerSubqueryExpression(ctx, root, predicate)
		if err != nil {
			return nil, err
		}
		root = &LogicalPlan{Kind: LogicalFilter, Children: []*LogicalPlan{root}, Output: clonePlannerColumns(root.Output), Predicate: predicate}
	}

	projection := make([]*binder.BoundExpression, len(query.Projection))
	for i, item := range query.Projection {
		if item.Expression == nil {
			return nil, fmt.Errorf("query planner: projection %d is nil", i)
		}
		var err error
		projection[i], err = collector.rewritePostAggregate(item.Expression)
		if err == nil {
			root, projection[i], err = p.lowerSubqueryExpression(ctx, root, projection[i])
		}
		if err != nil {
			return nil, err
		}
	}
	root = &LogicalPlan{Kind: LogicalProject, Children: []*LogicalPlan{root}, Output: relationColumnsToPlanner(query.Output), Projection: projection}
	if err := root.Validate(); err != nil {
		return nil, err
	}
	return root, nil
}

type aggregateCollector struct {
	outerAggregates map[*binder.BoundExpression]*binder.BoundExpression
	groups          []*binder.BoundExpression
	aggregates      []*binder.BoundExpression
	arguments       []*binder.BoundExpression
}

func newAggregateCollector(groups []*binder.BoundExpression) *aggregateCollector {
	out := &aggregateCollector{groups: make([]*binder.BoundExpression, len(groups))}
	for i, group := range groups {
		out.groups[i] = cloneAggregateExpression(group)
	}
	return out
}

// collect 只收集 aggregate node；aggregate 内部参数不会继续寻找 aggregate，嵌套 aggregate 已由 Binder 禁止。
func (c *aggregateCollector) collect(expr *binder.BoundExpression) error {
	if expr == nil {
		return nil
	}
	if expr.Kind == binder.BoundSubquery && !plannedSubquery(expr) {
		return fmt.Errorf("query planner: scalar subquery inside aggregate query is not lowered yet")
	}
	if expr.Kind == binder.BoundSubquery {
		if err := c.collectCorrelatedAggregates(expr.Subquery); err != nil {
			return err
		}
	}
	if expr.Kind == binder.BoundFunction && expr.FunctionKind == function.Aggregate {
		if c.aggregateIndex(expr) < 0 {
			c.aggregates = append(c.aggregates, cloneAggregateExpression(expr))
		}
		for _, child := range expr.Children {
			if c.argumentIndexExisting(child) < 0 {
				c.arguments = append(c.arguments, cloneAggregateExpression(child))
			}
		}
		return nil
	}
	for _, child := range expr.Children {
		if err := c.collect(child); err != nil {
			return err
		}
	}
	return nil
}

func (c *aggregateCollector) aggregateIndex(expr *binder.BoundExpression) int {
	for i, candidate := range c.aggregates {
		if aggregateExpressionEqual(candidate, expr) {
			return i
		}
	}
	return -1
}

func (c *aggregateCollector) argumentIndexExisting(expr *binder.BoundExpression) int {
	for i, candidate := range c.arguments {
		if aggregateExpressionEqual(candidate, expr) {
			return i
		}
	}
	return -1
}

func (c *aggregateCollector) argumentIndex(expr *binder.BoundExpression) (int, bool) {
	i := c.argumentIndexExisting(expr)
	return i, i >= 0
}

// rewritePostAggregate 把完整 group expression 与 aggregate call 替换成 Aggregate 输出列，
// 其余 scalar 结构递归保留。Binder 已保证普通列不会逃出 GROUP BY 约束。
func (c *aggregateCollector) rewritePostAggregate(expr *binder.BoundExpression) (*binder.BoundExpression, error) {
	if expr == nil {
		return nil, nil
	}
	for i, group := range c.groups {
		if aggregateExpressionEqual(group, expr) {
			return &binder.BoundExpression{Kind: binder.BoundColumn, Type: cloneQueryPlannerType(expr.Type), Nullable: expr.Nullable, ColumnIndex: i}, nil
		}
	}
	if expr.Kind == binder.BoundFunction && expr.FunctionKind == function.Aggregate {
		index := c.aggregateIndex(expr)
		if index < 0 {
			return nil, fmt.Errorf("query planner: aggregate output provenance missing")
		}
		return &binder.BoundExpression{Kind: binder.BoundColumn, Type: cloneQueryPlannerType(expr.Type), Nullable: expr.Nullable, ColumnIndex: len(c.groups) + index}, nil
	}
	if expr.Kind == binder.BoundColumn {
		return nil, fmt.Errorf("query planner: ungrouped column %d remained after aggregate binding", expr.ColumnIndex)
	}
	if expr.Kind == binder.BoundSubquery && binder.QueryCorrelated(expr.Subquery) {
		out := *expr
		var err error
		out.Subquery, err = c.rewriteCorrelatedAggregateQuery(expr.Subquery)
		if err != nil {
			return nil, err
		}
		out.Children = make([]*binder.BoundExpression, len(expr.Children))
		for i, child := range expr.Children {
			out.Children[i], err = c.rewritePostAggregate(child)
			if err != nil {
				return nil, err
			}
		}
		return &out, nil
	}
	if expr.Kind == binder.BoundSubquery && !plannedSubquery(expr) {
		return nil, fmt.Errorf("query planner: scalar subquery inside aggregate query is not lowered yet")
	}
	out := cloneAggregateExpression(expr)
	out.Children = make([]*binder.BoundExpression, len(expr.Children))
	for i, child := range expr.Children {
		rewritten, err := c.rewritePostAggregate(child)
		if err != nil {
			return nil, err
		}
		out.Children[i] = rewritten
	}
	return out, nil
}

func cloneAggregateExpression(expr *binder.BoundExpression) *binder.BoundExpression {
	if expr == nil {
		return nil
	}
	out := *expr
	out.Type = cloneQueryPlannerType(expr.Type)
	out.Children = make([]*binder.BoundExpression, len(expr.Children))
	for i, child := range expr.Children {
		out.Children[i] = cloneAggregateExpression(child)
	}
	// BoundSubquery provenance 只作为不可变 Binder IR 指针保留；aggregate path 当前会在 collect/rewrite 时显式拒绝。
	return &out
}

func aggregateExpressionEqual(left, right *binder.BoundExpression) bool {
	if left == nil || right == nil {
		return left == right
	}
	if left.Kind != right.Kind || left.Distinct != right.Distinct || left.Depth != right.Depth || left.Subquery != right.Subquery || left.ColumnIndex != right.ColumnIndex || left.Op != right.Op || left.FunctionID != right.FunctionID || left.FunctionKind != right.FunctionKind || left.Literal != right.Literal || left.Nullable != right.Nullable || !queryPlannerTypeEqual(left.Type, right.Type) || len(left.Children) != len(right.Children) {
		return false
	}
	for i := range left.Children {
		if !aggregateExpressionEqual(left.Children[i], right.Children[i]) {
			return false
		}
	}
	return true
}
