package planner

import (
	"fmt"

	"github.com/steamwo/fssql/internal/core/types"
	"github.com/steamwo/fssql/internal/function"
	"github.com/steamwo/fssql/internal/sql/ast"
	"github.com/steamwo/fssql/internal/sql/binder"
)

// Full-row NULL-safe semi/anti joins implement membership. ALL adds per-value
// ordinals so occurrences match one-to-one using existing managed sort/spill.
func planSetOperation(op ast.SetOperation, left, right *LogicalPlan) (*LogicalPlan, error) {
	if !plannerColumnsEqual(left.Output, right.Output) || len(left.Output) == 0 {
		return nil, fmt.Errorf("set operation input schema mismatch")
	}
	width := len(left.Output)
	all := op == ast.IntersectAll || op == ast.ExceptAll
	join := JoinSemi
	if op == ast.ExceptAll || op == ast.ExceptDistinct {
		join = JoinAnti
	} else if op != ast.IntersectAll && op != ast.IntersectDistinct {
		return nil, fmt.Errorf("unknown set operation %d", op)
	}
	if all {
		left = numberSetRows(left)
		right = numberSetRows(right)
	}
	var predicate *binder.BoundExpression
	for i := range left.Output {
		a := subqueryColumn(left.Output, i)
		b := subqueryColumn(right.Output, i)
		b.ColumnIndex += len(left.Output)
		key := domainNullSafeEqual(a, b)
		if predicate == nil {
			predicate = key
		} else {
			predicate = andExpression(predicate, key)
		}
	}
	result := &LogicalPlan{Kind: LogicalJoin, Join: join, Children: []*LogicalPlan{left, right}, Output: clonePlannerColumns(left.Output), Predicate: predicate}
	if all {
		result = &LogicalPlan{Kind: LogicalProject, Children: []*LogicalPlan{result}, Output: clonePlannerColumns(result.Output[:width]), Projection: passThrough(result.Output)[:width]}
	}
	return result, nil
}

func numberSetRows(input *LogicalPlan) *LogicalPlan {
	partition := passThrough(input.Output)
	spec := &binder.BoundExpression{Kind: binder.BoundLiteral, Type: types.LogicalType{ID: types.Varchar}, Literal: fmt.Sprintf("p=%d;o=0;d=;n=;f=none", len(partition))}
	row := &binder.BoundExpression{Kind: binder.BoundFunction, FunctionKind: function.Scalar, FunctionID: function.WindowRowNumberID, Type: types.LogicalType{ID: types.BigInt}, Children: append([]*binder.BoundExpression{spec}, partition...)}
	projection := append(partition, row)
	output := append(clonePlannerColumns(input.Output), Column{Name: "__fssql_set_ordinal", Type: row.Type})
	return &LogicalPlan{Kind: LogicalProject, Children: []*LogicalPlan{input}, Output: output, Projection: projection}
}
