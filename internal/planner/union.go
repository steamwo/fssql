package planner

import (
	"context"
	"fmt"

	"github.com/steamwo/fssql/internal/sql/binder"
)

// planUnionRelation 按原分支顺序收集同 schema 的 UNION ALL 输入并构造平衡树。
// Binder 的逐层类型推导已完成；只跨越纯透传包装，保留 CAST 和查询修饰符的执行边界。
func (p *QueryPlanner) planUnionRelation(ctx context.Context, relation *binder.BoundRelation) (*LogicalPlan, error) {
	columns := relation.Columns
	output := relationColumnsToPlanner(columns)
	pending := []*binder.BoundRelation{relation}
	var inputs []*LogicalPlan
	for len(pending) > 0 {
		if err := ctx.Err(); err != nil {
			return nil, err
		}
		input := pending[len(pending)-1]
		pending = pending[:len(pending)-1]
		if input == nil {
			return nil, fmt.Errorf("query planner: nil UNION ALL input")
		}
		if input.Kind == binder.RelationUnion && unionColumnsEqual(input.Columns, columns) {
			if len(input.Children) != 2 {
				return nil, fmt.Errorf("query planner: UNION ALL requires two inputs")
			}
			pending = append(pending, input.Children[1], input.Children[0])
			continue
		}
		if source := unionPassthroughSource(input, columns); source != nil {
			pending = append(pending, source)
			continue
		}
		planned, err := p.planRelation(ctx, input)
		if err != nil {
			return nil, err
		}
		if !plannerColumnsEqual(planned.Output, output) {
			return nil, fmt.Errorf("query planner: UNION ALL input schema mismatch")
		}
		inputs = append(inputs, planned)
	}
	return balancedUnionPlan(inputs, output), nil
}

// unionPassthroughSource 仅穿透 Binder 为 UNION 操作数生成的无副作用中间投影。
func unionPassthroughSource(relation *binder.BoundRelation, columns []binder.RelationColumn) *binder.BoundRelation {
	if relation.Kind != binder.RelationCTE || (relation.ID != "union:left" && relation.ID != "union:right") || relation.Materialize || relation.Lateral {
		return nil
	}
	q := relation.CTEQuery
	if q == nil || q.Source == nil || q.Source.Kind != binder.RelationUnion || q.Where != nil || len(q.GroupBy) != 0 || q.Having != nil || q.Distinct || len(q.OrderBy) != 0 || q.Limit != nil || q.Offset != nil {
		return nil
	}
	if !unionColumnsEqual(relation.Columns, columns) || !unionColumnsEqual(q.Output, columns) || !unionColumnsEqual(q.Source.Columns, columns) || len(q.Projection) != len(columns) {
		return nil
	}
	for i, item := range q.Projection {
		expr := item.Expression
		if expr == nil || expr.Kind != binder.BoundColumn || expr.Depth != 0 || expr.ColumnIndex != i || expr.SQLNull || expr.Nullable != columns[i].Nullable || !expr.Type.Same(columns[i].Type) {
			return nil
		}
	}
	return q.Source
}

// unionColumnsEqual 比较重关联所需的完整 schema，包含列名、可空性及逻辑类型。
func unionColumnsEqual(left, right []binder.RelationColumn) bool {
	if len(left) != len(right) {
		return false
	}
	for i := range left {
		if left[i].Name != right[i].Name || left[i].Nullable != right[i].Nullable || !left[i].Type.Same(right[i].Type) {
			return false
		}
	}
	return true
}

// balancedUnionPlan 保留输入次序，将线性链深度降到对数级，避免逐行经过所有祖先。
func balancedUnionPlan(inputs []*LogicalPlan, output []Column) *LogicalPlan {
	if len(inputs) == 1 {
		return inputs[0]
	}
	middle := len(inputs) / 2
	return &LogicalPlan{
		Kind: LogicalUnionAll,
		Children: []*LogicalPlan{
			balancedUnionPlan(inputs[:middle], output),
			balancedUnionPlan(inputs[middle:], output),
		},
		Output: clonePlannerColumns(output),
	}
}
