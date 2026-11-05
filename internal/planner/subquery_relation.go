package planner

import (
	"context"
	"fmt"

	"github.com/steamwo/fssql/internal/core/types"
	"github.com/steamwo/fssql/internal/function"
	"github.com/steamwo/fssql/internal/sql/binder"
)

func attachParameterDomain(input, domain *LogicalPlan) *LogicalPlan {
	if len(input.Output) == 0 {
		return clonePhase5Logical(domain)
	}
	if len(domain.Output) == 0 {
		return input
	}
	return &LogicalPlan{Kind: LogicalJoin, Join: JoinCross, Children: []*LogicalPlan{input, clonePhase5Logical(domain)}, Output: append(clonePlannerColumns(input.Output), domain.Output...)}
}

// Every relation returns its original columns followed by the same parameter
// schema. Query blocks can therefore keep their bound local column indexes.
func (p *QueryPlanner) planDomainRelation(ctx context.Context, relation *binder.BoundRelation, domain *LogicalPlan, slots map[int]int) (*LogicalPlan, error) {
	if relation == nil {
		return clonePhase5Logical(domain), nil
	}
	if !binder.QueryCorrelated(&binder.BoundQuery{Source: relation}) {
		input, err := p.planRelation(ctx, relation)
		if err != nil {
			return nil, err
		}
		return attachParameterDomain(input, domain), nil
	}
	output := append(relationColumnsToPlanner(relation.Columns), domain.Output...)
	switch relation.Kind {
	case binder.RelationCTE:
		if relation.CTEQuery == nil {
			return nil, fmt.Errorf("correlated CTE lacks query provenance")
		}
		input, _, err := p.planDomainQuery(ctx, relation.CTEQuery, domain, slots, false)
		if err != nil {
			return nil, err
		}
		// Relation aliases and UNION coercions own the visible output metadata.
		input = &LogicalPlan{Kind: LogicalProject, Children: []*LogicalPlan{input}, Output: output, Projection: phase5PassThrough(input.Output)}
		if relation.Materialize {
			input = materializeDomainInput(input, fmt.Sprintf("domain-cte:%p:%p", relation.CTEQuery, domain))
		}
		return input, nil
	case binder.RelationUnion:
		if len(relation.Children) != 2 {
			return nil, fmt.Errorf("correlated UNION requires two inputs")
		}
		children := make([]*LogicalPlan, 2)
		for i, child := range relation.Children {
			input, err := p.planDomainRelation(ctx, child, domain, slots)
			if err != nil {
				return nil, err
			}
			children[i] = input
		}
		return &LogicalPlan{Kind: LogicalUnionAll, Children: children, Output: output}, nil
	case binder.RelationProject:
		if len(relation.Children) != 1 {
			return nil, fmt.Errorf("correlated projection requires one input")
		}
		input, err := p.planDomainRelation(ctx, relation.Children[0], domain, slots)
		if err != nil {
			return nil, err
		}
		width := len(relation.Children[0].Columns)
		projection := make([]*binder.BoundExpression, 0, len(output))
		for _, e := range relation.Projection {
			e, err = rewriteDomainExpression(e, func(i int) int { return i }, width, slots)
			if err != nil {
				return nil, err
			}
			input, e, err = p.lowerSubqueryExpression(ctx, input, e)
			if err != nil {
				return nil, err
			}
			projection = append(projection, e)
		}
		for i := range domain.Output {
			projection = append(projection, subqueryColumn(input.Output, width+i))
		}
		return &LogicalPlan{Kind: LogicalProject, Children: []*LogicalPlan{input}, Output: output, Projection: projection}, nil
	case binder.RelationJoin:
		return p.planDomainJoin(ctx, relation, domain, slots)
	case binder.RelationSample:
		return nil, fmt.Errorf("sampling in correlated subqueries is not supported")
	default:
		return nil, fmt.Errorf("parameter-domain correlation through relation kind %d is unsupported", relation.Kind)
	}
}

// Remap the input scope of an expression, including references from nested
// expression subqueries back to that scope. Other nested local scopes stay put.
func rewriteDomainExpression(e *binder.BoundExpression, local func(int) int, domainStart int, slots map[int]int) (*binder.BoundExpression, error) {
	q, err := binder.RewriteQueryExpressions(&binder.BoundQuery{Projection: []binder.BoundSelectItem{{Expression: e}}}, func(ref *binder.BoundExpression, depth int) (*binder.BoundExpression, error) {
		out := *ref
		if (depth == 0 && ref.Kind == binder.BoundColumn) || (ref.Kind == binder.BoundOuterColumn && ref.Depth == depth) {
			out.ColumnIndex = local(ref.ColumnIndex)
			return &out, nil
		}
		if ref.Kind == binder.BoundOuterColumn && ref.Depth > depth {
			if ref.Depth != depth+1 {
				return nil, fmt.Errorf("unresolved correlation through JOIN scope")
			}
			slot, ok := slots[ref.ColumnIndex]
			if !ok {
				return nil, fmt.Errorf("missing JOIN correlation parameter")
			}
			out.ColumnIndex = domainStart + slot
			if depth == 0 {
				out.Kind = binder.BoundColumn
				out.Depth = 0
			} else {
				out.Depth = depth
			}
			return &out, nil
		}
		return ref, nil
	})
	if err != nil {
		return nil, err
	}
	return q.Projection[0].Expression, nil
}

func domainSelect(input *LogicalPlan, output []Column, indexes []int) *LogicalPlan {
	projection := make([]*binder.BoundExpression, len(indexes))
	for i, index := range indexes {
		if index < 0 {
			projection[i] = &binder.BoundExpression{Kind: binder.BoundNull, Type: output[i].Type, Nullable: true}
		} else {
			projection[i] = subqueryColumn(input.Output, index)
		}
	}
	return &LogicalPlan{Kind: LogicalProject, Children: []*LogicalPlan{input}, Output: clonePlannerColumns(output), Projection: projection}
}

func numberJoinRows(input *LogicalPlan) *LogicalPlan {
	spec := &binder.BoundExpression{Kind: binder.BoundLiteral, Type: types.LogicalType{ID: types.Varchar}, Literal: "p=0;o=0;d=;n=;f=none"}
	row := &binder.BoundExpression{Kind: binder.BoundFunction, Type: types.LogicalType{ID: types.BigInt}, FunctionID: function.WindowRowNumberID, FunctionKind: function.Scalar, Children: []*binder.BoundExpression{spec}}
	input, _ = appendSubqueryValues(input, []*binder.BoundExpression{row})
	return materializeDomainInput(input, fmt.Sprintf("domain-join-side:%p", input))
}

// Match predicates over candidate pairs. Preserved-side completion uses row
// identity, rather than values, so duplicate and all-NULL input rows survive.
func (p *QueryPlanner) planDomainJoin(ctx context.Context, relation *binder.BoundRelation, domain *LogicalPlan, slots map[int]int) (*LogicalPlan, error) {
	if len(relation.Children) != 2 {
		return nil, fmt.Errorf("parameter-domain JOIN requires two inputs")
	}
	if relation.Dependent {
		return p.planLateralJoin(ctx, relation, domain, slots)
	}
	left, err := p.planDomainRelation(ctx, relation.Children[0], domain, slots)
	if err != nil {
		return nil, err
	}
	right, err := p.planDomainRelation(ctx, relation.Children[1], domain, slots)
	if err != nil {
		return nil, err
	}
	return p.finishDomainJoin(ctx, relation, left, right, domain, slots, nil, nil)
}

func (p *QueryPlanner) finishDomainJoin(ctx context.Context, relation *binder.BoundRelation, left, right, domain *LogicalPlan, slots map[int]int, leftParameters, rightParameters []int) (*LogicalPlan, error) {
	kind, err := plannerJoinType(relation.Join)
	if err != nil {
		return nil, err
	}
	lw, rw := len(relation.Children[0].Columns), len(relation.Children[1].Columns)
	preserve := kind != JoinInner && kind != JoinCross
	if preserve {
		left = numberJoinRows(left)
		right = numberJoinRows(right)
	}
	rightStart := len(left.Output)
	condition, err := rewriteDomainExpression(relation.Condition, func(i int) int {
		if i < lw {
			return i
		}
		return rightStart + i - lw
	}, lw, slots)
	if err != nil {
		return nil, err
	}
	var identity *binder.BoundExpression
	if len(leftParameters) == 0 {
		for i := range domain.Output {
			leftParameters = append(leftParameters, lw+i)
			rightParameters = append(rightParameters, rw+i)
		}
	}
	for i, index := range leftParameters {
		r := subqueryColumn(right.Output, rightParameters[i])
		r.ColumnIndex += rightStart
		eq := domainNullSafeEqual(subqueryColumn(left.Output, index), r)
		if identity == nil {
			identity = eq
		} else {
			identity = phase5And(identity, eq)
		}
	}
	matched := &LogicalPlan{Kind: LogicalJoin, Join: JoinCross, Children: []*LogicalPlan{left, right}, Output: append(clonePlannerColumns(left.Output), right.Output...)}
	// Match domain identities before evaluating ON. This avoids pairing every
	// domain with every other domain, and keeps equality candidates spillable.
	if identity == nil {
		identity = domainJoinCandidate(condition, len(left.Output), len(right.Output))
	}
	if identity != nil {
		matched.Join = JoinInner
		matched.Predicate = identity
	}
	matchedWidth := len(matched.Output)
	if condition != nil {
		matched, condition, err = p.lowerSubqueryExpression(ctx, matched, condition)
		if err != nil {
			return nil, err
		}
		matched = &LogicalPlan{Kind: LogicalFilter, Children: []*LogicalPlan{matched}, Output: clonePlannerColumns(matched.Output), Predicate: condition}
	}
	if len(matched.Output) != matchedWidth {
		indexes := make([]int, matchedWidth)
		for i := range indexes {
			indexes[i] = i
		}
		matched = domainSelect(matched, matched.Output[:matchedWidth], indexes)
	}
	if preserve {
		matched = materializeDomainInput(matched, fmt.Sprintf("domain-join-matched:%p", matched))
	}
	output := append(relationColumnsToPlanner(relation.Columns), domain.Output...)
	// Find source rows by their stable per-side row numbers. SEMI/ANTI emit
	// each input row once even when the ON condition has multiple matches.
	selectSide := func(side *LogicalPlan, id int, semi bool) (*LogicalPlan, error) {
		join := JoinAnti
		if semi {
			join = JoinSemi
		}
		lkey := len(side.Output) - 1
		return subqueryJoin(clonePhase5Logical(side), clonePhase5Logical(matched), []int{lkey}, []int{id}, join)
	}
	if kind == JoinSemi || kind == JoinAnti {
		selected, err := selectSide(left, len(left.Output)-1, kind == JoinSemi)
		if err != nil {
			return nil, err
		}
		indexes := make([]int, lw+len(domain.Output))
		for i := range indexes {
			indexes[i] = i
		}
		return domainSelect(selected, output, indexes), nil
	}
	indexes := make([]int, 0, len(output))
	for i := 0; i < lw; i++ {
		indexes = append(indexes, i)
	}
	for i := 0; i < rw; i++ {
		indexes = append(indexes, rightStart+i)
	}
	for i := range domain.Output {
		indexes = append(indexes, lw+i)
	}
	result := domainSelect(matched, output, indexes)
	if kind == JoinLeft || kind == JoinFull {
		missing, err := selectSide(left, len(left.Output)-1, false)
		if err != nil {
			return nil, err
		}
		indexes := make([]int, 0, len(output))
		for i := 0; i < lw; i++ {
			indexes = append(indexes, i)
		}
		for i := 0; i < rw; i++ {
			indexes = append(indexes, -1)
		}
		for i := range domain.Output {
			indexes = append(indexes, lw+i)
		}
		empty := domainSelect(missing, output, indexes)
		result = &LogicalPlan{Kind: LogicalUnionAll, Children: []*LogicalPlan{result, empty}, Output: clonePlannerColumns(output)}
	}
	if kind == JoinRight || kind == JoinFull {
		missing, err := selectSide(right, rightStart+len(right.Output)-1, false)
		if err != nil {
			return nil, err
		}
		indexes := make([]int, 0, len(output))
		for i := 0; i < lw; i++ {
			indexes = append(indexes, -1)
		}
		for i := 0; i < rw; i++ {
			indexes = append(indexes, i)
		}
		for i := range domain.Output {
			indexes = append(indexes, rw+i)
		}
		empty := domainSelect(missing, output, indexes)
		result = &LogicalPlan{Kind: LogicalUnionAll, Children: []*LogicalPlan{result, empty}, Output: clonePlannerColumns(output)}
	}
	return result, nil
}

// One hashable conjunct is sufficient to narrow candidate pairs; the complete
// SQL predicate remains a filter, including mixed NULL-equality modes.
func domainJoinCandidate(e *binder.BoundExpression, left, right int) *binder.BoundExpression {
	if e == nil {
		return nil
	}
	if _, err := ExtractHashJoinKeys(e, left, right); err == nil {
		return e
	}
	if e.Kind == binder.BoundBinary && e.Op == "AND" && len(e.Children) == 2 {
		if key := domainJoinCandidate(e.Children[0], left, right); key != nil {
			return key
		}
		return domainJoinCandidate(e.Children[1], left, right)
	}
	return nil
}
