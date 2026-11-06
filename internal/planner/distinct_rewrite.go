package planner

import (
	"fmt"

	"github.com/steamwo/fssql/internal/function"
	"github.com/steamwo/fssql/internal/sql/binder"
)

// sharedDistinctArgument reports whether every aggregate is DISTINCT (or
// DISTINCT-insensitive MIN/MAX) over one materialized argument. Such shapes can
// aggregate distinct values with a hash GROUP BY instead of sorting the input.
func (c *aggregateCollector) sharedDistinctArgument() (int, bool) {
	if len(c.aggregates) == 0 {
		return 0, false
	}
	shared := -1
	distinct := false
	for _, aggregate := range c.aggregates {
		if aggregate == nil || aggregate.Kind != binder.BoundFunction || aggregate.FunctionKind != function.Aggregate {
			return 0, false
		}
		if len(aggregate.Children) != 1 {
			return 0, false
		}
		insensitive := aggregate.FunctionID == function.MinID || aggregate.FunctionID == function.MaxID
		if !aggregate.Distinct && !insensitive {
			return 0, false
		}
		if aggregate.Distinct && !insensitive {
			distinct = true
		}
		index, ok := c.argumentIndex(aggregate.Children[0])
		if !ok {
			return 0, false
		}
		if shared < 0 {
			shared = index
		} else if shared != index {
			return 0, false
		}
	}
	if !distinct {
		return 0, false
	}
	return shared, true
}

// twoLevelDistinctAggregate replaces DISTINCT aggregate marking with an inner
// hash GROUP BY over (groups, argument) followed by the original aggregates
// over the deduplicated argument. NULL stays one inner group, and the outer
// aggregate functions still ignore it exactly as their DISTINCT forms do.
func twoLevelDistinctAggregate(input *LogicalPlan, groupColumns []*binder.BoundExpression, preOutput []Column, aggregates []AggregateSpec, aggregateOutput []Column, argumentIndex int) (*LogicalPlan, error) {
	groupCount := len(groupColumns)
	argumentColumnIndex := groupCount + argumentIndex
	if argumentIndex < 0 || argumentColumnIndex >= len(preOutput) {
		return nil, fmt.Errorf("query planner: DISTINCT aggregate argument index %d is out of range", argumentColumnIndex)
	}
	argument := preOutput[argumentColumnIndex]
	innerGroups := make([]*binder.BoundExpression, 0, groupCount+1)
	innerGroups = append(innerGroups, groupColumns...)
	innerGroups = append(innerGroups, &binder.BoundExpression{
		Kind:        binder.BoundColumn,
		Type:        cloneQueryPlannerType(argument.Type),
		Nullable:    argument.Nullable,
		ColumnIndex: argumentColumnIndex,
	})
	innerOutput := make([]Column, 0, groupCount+1)
	innerOutput = append(innerOutput, clonePlannerColumns(preOutput[:groupCount])...)
	innerOutput = append(innerOutput, argument)
	inner := &LogicalPlan{
		Kind:     LogicalAggregate,
		Children: []*LogicalPlan{input},
		Output:   innerOutput,
		GroupBy:  innerGroups,
	}

	outerGroups := make([]*binder.BoundExpression, groupCount)
	for i, group := range groupColumns {
		outerGroups[i] = &binder.BoundExpression{
			Kind:        binder.BoundColumn,
			Type:        cloneQueryPlannerType(group.Type),
			Nullable:    group.Nullable,
			ColumnIndex: i,
		}
	}
	for i := range aggregates {
		aggregates[i].Arguments = []*binder.BoundExpression{{
			Kind:        binder.BoundColumn,
			Type:        cloneQueryPlannerType(argument.Type),
			Nullable:    argument.Nullable,
			ColumnIndex: groupCount,
		}}
	}
	return &LogicalPlan{
		Kind:       LogicalAggregate,
		Children:   []*LogicalPlan{inner},
		Output:     aggregateOutput,
		GroupBy:    outerGroups,
		Aggregates: aggregates,
	}, nil
}
