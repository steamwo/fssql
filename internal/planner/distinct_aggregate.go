package planner

import (
	"fmt"

	"github.com/steamwo/fssql/internal/function"
	"github.com/steamwo/fssql/internal/sql/binder"
)

// Sort and mark the first (group, value) occurrence without removing input
// rows, so ordinary aggregates and independent DISTINCT counts can coexist.
func markDistinctArgument(input *LogicalPlan, groups []*binder.BoundExpression, index int) (*LogicalPlan, int, error) {
	if index < 0 || index >= len(input.Output) {
		return nil, 0, fmt.Errorf("DISTINCT aggregate argument is out of range")
	}
	keys := make([]SortKey, 0, len(groups)+1)
	args := make([]*binder.BoundExpression, 0, len(groups)+1)
	for _, group := range groups {
		keys = append(keys, SortKey{Expr: group})
		args = append(args, group)
	}
	column := input.Output[index]
	value := &binder.BoundExpression{Kind: binder.BoundColumn, Type: column.Type, Nullable: column.Nullable, ColumnIndex: index}
	keys = append(keys, SortKey{Expr: value})
	args = append(args, value)
	sorted := &LogicalPlan{Kind: LogicalSort, Children: []*LogicalPlan{input}, Output: clonePlannerColumns(input.Output), Sort: keys}
	projection := passThrough(input.Output)
	projection = append(projection, &binder.BoundExpression{Kind: binder.BoundFunction, Type: column.Type, Nullable: true, FunctionID: function.DistinctValueID, FunctionKind: function.Scalar, Children: args})
	output := clonePlannerColumns(input.Output)
	output = append(output, Column{Name: fmt.Sprintf("__distinct_%d", len(output)), Type: column.Type, Nullable: true})
	return &LogicalPlan{Kind: LogicalProject, Children: []*LogicalPlan{sorted}, Output: output, Projection: projection}, len(output) - 1, nil
}
