package planner

import (
	"context"
	"fmt"

	"github.com/steamwo/fssql/internal/sql/binder"
)

// Remove just the right input's sibling scope. Its references will be rebound
// to a parameter domain built from the already planned left input.
func lateralInput(r *binder.BoundRelation) *binder.BoundRelation {
	out := *r
	if out.Lateral {
		out.Lateral = false
		return &out
	}
	if (r.Kind == binder.RelationSample || r.Kind == binder.RelationProject) && len(r.Children) == 1 {
		out.Children = []*binder.BoundRelation{lateralInput(r.Children[0])}
	}
	return &out
}

func (p *QueryPlanner) planLateralJoin(ctx context.Context, relation *binder.BoundRelation, domain *LogicalPlan, slots map[int]int) (*LogicalPlan, error) {
	left, err := p.planDomainRelation(ctx, relation.Children[0], domain, slots)
	if err != nil {
		return nil, err
	}
	width := len(relation.Children[0].Columns)
	parameters := make([]int, 0, len(domain.Output))
	parameterSlots := make(map[int]int)
	// Parent domain columns retain their prefix in the right input's parameter
	// schema, so the enclosing query can keep propagating its own parameters.
	for i := range domain.Output {
		index := width + i
		parameterSlots[index] = len(parameters)
		parameters = append(parameters, index)
	}
	query, err := binder.RewriteOuterReferences(&binder.BoundQuery{Source: lateralInput(relation.Children[1])}, func(ref *binder.BoundExpression, depth int) (*binder.BoundExpression, error) {
		index := -1
		switch ref.Depth - depth {
		case 1:
			if ref.ColumnIndex < 0 || ref.ColumnIndex >= width {
				return nil, fmt.Errorf("LATERAL left reference out of range")
			}
			index = ref.ColumnIndex
		case 2:
			slot, ok := slots[ref.ColumnIndex]
			if !ok {
				return nil, fmt.Errorf("missing parent LATERAL parameter")
			}
			index = width + slot
		default:
			return nil, fmt.Errorf("unresolved LATERAL query depth")
		}
		if _, ok := parameterSlots[index]; !ok {
			parameterSlots[index] = len(parameters)
			parameters = append(parameters, index)
		}
		out := *ref
		out.Depth = depth + 1
		out.ColumnIndex = index
		return &out, nil
	})
	if err != nil {
		return nil, err
	}
	left = materializeDomainInput(left, fmt.Sprintf("lateral-left:%p", left))
	projection := make([]*binder.BoundExpression, len(parameters))
	output := make([]Column, len(parameters))
	for i, index := range parameters {
		projection[i] = subqueryColumn(left.Output, index)
		output[i] = left.Output[index]
	}
	input := &LogicalPlan{Kind: LogicalProject, Children: []*LogicalPlan{clonePhase5Logical(left)}, Output: output, Projection: projection}
	input = &LogicalPlan{Kind: LogicalDistinct, Children: []*LogicalPlan{input}, Output: clonePlannerColumns(input.Output)}
	right, err := p.planDomainRelation(ctx, query.Source, input, parameterSlots)
	if err != nil {
		return nil, err
	}
	rightKeys := make([]int, len(parameters))
	for i := range rightKeys {
		rightKeys[i] = len(relation.Children[1].Columns) + i
	}
	return p.finishDomainJoin(ctx, relation, left, right, domain, slots, parameters, rightKeys)
}
