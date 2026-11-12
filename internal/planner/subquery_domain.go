package planner

import (
	"context"
	"fmt"
	"math"
	"strconv"

	"github.com/steamwo/fssql/internal/core/types"
	"github.com/steamwo/fssql/internal/function"
	"github.com/steamwo/fssql/internal/sql/binder"
)

// Keep the equality-only rewrite as a fast path. Other shapes evaluate a
// duplicate-free parameter domain through ordinary relational operators.
func needsDomainDecorrelation(q *binder.BoundQuery) bool {
	if binder.QueryCorrelated(&binder.BoundQuery{Source: q.Source}) {
		return true
	}
	if _, err := extractCorrelation(q); err != nil {
		return true
	}
	if len(q.GroupBy) == 0 && q.Having != nil {
		return true
	}
	for _, item := range q.Projection {
		if binder.ExpressionCorrelated(item.Expression) {
			return true
		}
		base := item.Expression
		for base != nil && base.Kind == binder.BoundCast && len(base.Children) == 1 {
			base = base.Children[0]
		}
		if containsBoundAggregate(base) && (base.Kind != binder.BoundFunction || base.FunctionKind != function.Aggregate) {
			return true
		}
	}
	// Nested correlations must receive this block's parameter columns too.
	nested := false
	_, _ = binder.RewriteOuterReferences(q, func(e *binder.BoundExpression, depth int) (*binder.BoundExpression, error) {
		if depth > 0 {
			nested = true
		}
		return e, nil
	})
	return nested
}

// A planner-local resolver supplies already planned inputs without adding an
// execution dependency to the binder or a new runtime scan type.
type domainPlanResolver struct {
	parent RelationPlanResolver
	id     string
	plan   *LogicalPlan
}

func (r domainPlanResolver) PlanBoundRelation(ctx context.Context, relation *binder.BoundRelation) (*LogicalPlan, error) {
	if relation.ID == r.id {
		return cloneLogical(r.plan), nil
	}
	if r.parent == nil {
		return nil, fmt.Errorf("query planner: missing relation %s", relation.ID)
	}
	return r.parent.PlanBoundRelation(ctx, relation)
}

func (p *QueryPlanner) planOnDomainInput(ctx context.Context, q *binder.BoundQuery, input *LogicalPlan) (*LogicalPlan, error) {
	q = copySubquery(q)
	id := fmt.Sprintf("domain-input:%p", q)
	columns := make([]binder.RelationColumn, len(input.Output))
	for i, c := range input.Output {
		columns[i] = binder.RelationColumn{Name: c.Name, Type: c.Type, Nullable: c.Nullable}
	}
	q.Source = &binder.BoundRelation{Kind: binder.RelationCatalog, ID: id, Columns: columns}
	local := *p
	local.Relations = domainPlanResolver{parent: p.Relations, id: id, plan: input}
	return local.PlanStable(ctx, q)
}

func materializeDomainInput(input *LogicalPlan, key string) *LogicalPlan {
	return &LogicalPlan{Kind: LogicalSort, Children: []*LogicalPlan{input}, Output: clonePlannerColumns(input.Output), Sort: []SortKey{{Expr: internalMarker(function.CTEMarkerID, key)}}}
}

func domainBinary(op string, a, b *binder.BoundExpression) *binder.BoundExpression {
	return &binder.BoundExpression{Kind: binder.BoundBinary, Op: op, Type: types.LogicalType{ID: types.Boolean}, Nullable: a.Nullable || b.Nullable, Children: []*binder.BoundExpression{a, b}}
}
func domainInt(n uint64) *binder.BoundExpression {
	return &binder.BoundExpression{Kind: binder.BoundLiteral, Type: types.LogicalType{ID: types.BigInt}, Literal: strconv.FormatUint(n, 10)}
}
func domainIsNull(e *binder.BoundExpression, op string) *binder.BoundExpression {
	return &binder.BoundExpression{Kind: binder.BoundIsNull, Op: op, Type: types.LogicalType{ID: types.Boolean}, Children: []*binder.BoundExpression{e}}
}
func domainCase(cond, value *binder.BoundExpression) *binder.BoundExpression {
	return &binder.BoundExpression{Kind: binder.BoundFunction, FunctionID: function.CaseWhenID, FunctionKind: function.Scalar, Type: value.Type, Nullable: true, Children: []*binder.BoundExpression{cond, value, {Kind: binder.BoundNull, Type: value.Type, Nullable: true}}}
}
func domainNullSafeEqual(a, b *binder.BoundExpression) *binder.BoundExpression {
	eq := domainBinary("=", a, b)
	coalesce := &binder.BoundExpression{Kind: binder.BoundFunction, FunctionID: function.CoalesceID, FunctionKind: function.Scalar, Type: eq.Type, Children: []*binder.BoundExpression{eq, {Kind: binder.BoundLiteral, Type: eq.Type, Literal: "false"}}}
	return domainBinary("OR", coalesce, andExpression(domainIsNull(a, "IS NULL"), domainIsNull(b, "IS NULL")))
}

func (p *QueryPlanner) lowerDomainSubquery(ctx context.Context, input *LogicalPlan, expr *binder.BoundExpression) (*LogicalPlan, *binder.BoundExpression, error) {
	params := []*binder.BoundExpression{}
	slots := map[int]int{}
	_, err := binder.RewriteOuterReferences(expr.Subquery, func(ref *binder.BoundExpression, depth int) (*binder.BoundExpression, error) {
		if ref.Depth != depth+1 {
			return nil, fmt.Errorf("unresolved multi-level correlation depth %d", ref.Depth)
		}
		if _, ok := slots[ref.ColumnIndex]; !ok {
			if ref.ColumnIndex < 0 || ref.ColumnIndex >= len(input.Output) {
				return nil, fmt.Errorf("correlation column out of range")
			}
			slots[ref.ColumnIndex] = len(params)
			params = append(params, subqueryColumn(input.Output, ref.ColumnIndex))
		}
		return ref, nil
	})
	if err != nil {
		return nil, nil, err
	}
	if expr.Op == "IN" {
		if len(params)==0{params=append(params,domainInt(0))}
		params = append(params, expr.Children[0])
	}
	input, leftKeys := appendSubqueryValues(input, params)
	input = materializeDomainInput(input, fmt.Sprintf("domain-outer:%p", input))
	projection := make([]*binder.BoundExpression, len(leftKeys))
	output := make([]Column, len(leftKeys))
	for i, k := range leftKeys {
		projection[i] = subqueryColumn(input.Output, k)
		output[i] = input.Output[k]
	}
	domain := &LogicalPlan{Kind: LogicalProject, Children: []*LogicalPlan{cloneLogical(input)}, Output: output, Projection: projection}
	domain = &LogicalPlan{Kind: LogicalDistinct, Children: []*LogicalPlan{domain}, Output: clonePlannerColumns(domain.Output)}
	fullDomain := domain
	if expr.Op == "IN" {
		// The tested operand affects the mark, not evaluation of the inner
		// query. Different operands with equal parameters share volatile values.
		n := len(params) - 1
		keys := make([]*binder.BoundExpression, n)
		for i := range keys {
			keys[i] = subqueryColumn(domain.Output, i)
		}
		domain = &LogicalPlan{Kind: LogicalProject, Children: []*LogicalPlan{domain}, Output: clonePlannerColumns(domain.Output[:n]), Projection: keys}
		domain = &LogicalPlan{Kind: LogicalDistinct, Children: []*LogicalPlan{domain}, Output: clonePlannerColumns(domain.Output)}
	}
	result, keys, err := p.planDomainQuery(ctx, expr.Subquery, domain, slots, expr.Op == "EXISTS")
	if err != nil {
		return nil, nil, err
	}
	if expr.Op == "IN" {
		result = materializeDomainInput(result, fmt.Sprintf("domain-membership:%p", result))
		start := len(result.Output)
		var identity *binder.BoundExpression
		for i, k := range keys {
			r := subqueryColumn(fullDomain.Output, i)
			r.ColumnIndex += start
			eq := domainNullSafeEqual(subqueryColumn(result.Output, k), r)
			if identity == nil {
				identity = eq
			} else {
				identity = andExpression(identity, eq)
			}
		}
		result = &LogicalPlan{Kind: LogicalJoin, Join: JoinInner, Children: []*LogicalPlan{result, fullDomain}, Output: append(clonePlannerColumns(result.Output), fullDomain.Output...), Predicate: identity}
		keys = make([]int, len(params))
		for i := range keys {
			keys[i] = start + i
		}
	}
	// Reduce rows per parameter set, then attach the one-row summary using
	// NULL-safe identity. SQL predicates above retain normal NULL semantics.
	summary := &binder.BoundQuery{}
	for _, k := range keys {
		summary.GroupBy = append(summary.GroupBy, subqueryColumn(result.Output, k))
	}
	values := append([]*binder.BoundExpression(nil), summary.GroupBy...)
	value := subqueryColumn(result.Output, 0)
	if expr.Op == "EXISTS" {
		values = append(values, aggregateSubqueryExpr(function.CountID, nil))
	} else {
		values = append(values, &binder.BoundExpression{Kind: binder.BoundFunction, Type: value.Type, Nullable: true, FunctionID: function.MinID, FunctionKind: function.Aggregate, Children: []*binder.BoundExpression{value}}, aggregateSubqueryExpr(function.CountID, nil))
		if expr.Op == "IN" {
			values = append(values, aggregateSubqueryExpr(function.CountID, value))
			op:=expr.Comparison;if op=="" {op="="}
			match := domainCase(domainBinary(op, subqueryColumn(result.Output, keys[len(keys)-1]), value), domainInt(1))
			values = append(values, &binder.BoundExpression{Kind: binder.BoundFunction, Type: match.Type, Nullable: true, FunctionID: function.MaxID, FunctionKind: function.Aggregate, Children: []*binder.BoundExpression{match}})
		}
	}
	setSubqueryProjection(summary, values)
	right, err := p.planOnDomainInput(ctx, summary, result)
	if err != nil {
		return nil, nil, err
	}
	start := len(input.Output)
	var predicate *binder.BoundExpression
	for i, k := range leftKeys {
		r := subqueryColumn(right.Output, i)
		r.ColumnIndex += start
		eq := domainNullSafeEqual(subqueryColumn(input.Output, k), r)
		if predicate == nil {
			predicate = eq
		} else {
			predicate = andExpression(predicate, eq)
		}
	}
	for i := range right.Output {
		right.Output[i].Nullable = true
	}
	input = &LogicalPlan{Kind: LogicalJoin, Join: JoinLeft, Children: []*LogicalPlan{input, right}, Output: append(clonePlannerColumns(input.Output), right.Output...), Predicate: predicate}
	base := start + len(keys)
	if expr.Op == "EXISTS" {
		return input, domainIsNull(subqueryColumn(input.Output, base), "IS NOT NULL"), nil
	}
	if expr.Op == "IN" {
		return input, &binder.BoundExpression{Kind: binder.BoundFunction, Type: expr.Type, Nullable: true, FunctionID: function.MembershipMarkID, FunctionKind: function.Scalar, Children: []*binder.BoundExpression{subqueryColumn(input.Output, leftKeys[len(leftKeys)-1]), subqueryColumn(input.Output, base+3), subqueryColumn(input.Output, base+1), subqueryColumn(input.Output, base+2)}}, nil
	}
	return input, &binder.BoundExpression{Kind: binder.BoundFunction, Type: expr.Type, Nullable: true, FunctionID: function.ScalarCardinalityID, FunctionKind: function.Scalar, Children: []*binder.BoundExpression{subqueryColumn(input.Output, base), subqueryColumn(input.Output, base+1)}}, nil
}

// Complete absent groups with one NULL inner row. Arbitrary SQL predicates
// execute as filters over the cross product; only parameter identity is joined.
func (p *QueryPlanner) completeDomainGroups(ctx context.Context, joined, domain *LogicalPlan, predicate *binder.BoundExpression, width int) (*LogicalPlan, error) {
	output := clonePlannerColumns(joined.Output)
	matched := joined
	if predicate != nil {
		var err error
		matched, predicate, err = p.lowerSubqueryExpression(ctx, matched, predicate)
		if err != nil {
			return nil, err
		}
		matched = &LogicalPlan{Kind: LogicalFilter, Children: []*LogicalPlan{matched}, Output: clonePlannerColumns(matched.Output), Predicate: predicate}
	}
	projection := make([]*binder.BoundExpression, len(output))
	for i := range projection {
		projection[i] = subqueryColumn(matched.Output, i)
	}
	matched = &LogicalPlan{Kind: LogicalProject, Children: []*LogicalPlan{matched}, Output: output, Projection: projection}
	matched = materializeDomainInput(matched, fmt.Sprintf("domain-matched:%p", matched))
	var identity *binder.BoundExpression
	for i := range domain.Output {
		r := subqueryColumn(matched.Output, width+i)
		r.ColumnIndex += len(domain.Output)
		eq := domainNullSafeEqual(subqueryColumn(domain.Output, i), r)
		if identity == nil {
			identity = eq
		} else {
			identity = andExpression(identity, eq)
		}
	}
	absent := &LogicalPlan{Kind: LogicalJoin, Join: JoinAnti, Children: []*LogicalPlan{cloneLogical(domain), cloneLogical(matched)}, Output: clonePlannerColumns(domain.Output), Predicate: identity}
	nulls := make([]*binder.BoundExpression, len(output))
	for i := 0; i < width; i++ {
		nulls[i] = &binder.BoundExpression{Kind: binder.BoundNull, Type: output[i].Type, Nullable: true}
	}
	for i := range domain.Output {
		nulls[width+i] = subqueryColumn(absent.Output, i)
	}
	empty := &LogicalPlan{Kind: LogicalProject, Children: []*LogicalPlan{absent}, Output: clonePlannerColumns(output), Projection: nulls}
	return &LogicalPlan{Kind: LogicalUnionAll, Children: []*LogicalPlan{matched, empty}, Output: clonePlannerColumns(output)}, nil
}

// DuckDB likewise lowers constant LIMIT/OFFSET to a partitioned row number.
// Expressions depending on outer rows remain unsupported, as in DuckDB.
func limitDomainRows(input *LogicalPlan, keys []int, order []SortKey, limitExpr, offsetExpr *binder.BoundExpression) (*LogicalPlan, error) {
	limit, offset := uint64(math.MaxInt64), uint64(0)
	var err error
	if limitExpr != nil {
		limit, err = foldRowCountExpression(limitExpr, "correlated LIMIT")
		if err != nil {
			return nil, err
		}
	}
	if offsetExpr != nil {
		offset, err = foldRowCountExpression(offsetExpr, "correlated OFFSET")
		if err != nil {
			return nil, err
		}
	}
	desc, nulls := "", ""
	args := []*binder.BoundExpression{nil}
	for _, k := range keys {
		args = append(args, subqueryColumn(input.Output, k))
	}
	for _, key := range order {
		args = append(args, key.Expr)
		if key.Descending {
			desc += "1"
		} else {
			desc += "0"
		}
		if key.NullsFirst != nil && *key.NullsFirst {
			nulls += "f"
		} else {
			nulls += "l"
		}
	}
	args[0] = &binder.BoundExpression{Kind: binder.BoundLiteral, Type: types.LogicalType{ID: types.Varchar}, Literal: fmt.Sprintf("p=%d;o=%d;d=%s;n=%s;f=none", len(keys), len(order), desc, nulls)}
	rownum := &binder.BoundExpression{Kind: binder.BoundFunction, Type: types.LogicalType{ID: types.BigInt}, FunctionID: function.WindowRowNumberID, FunctionKind: function.Scalar, Children: args}
	input, indexes := appendSubqueryValues(input, []*binder.BoundExpression{rownum})
	row := subqueryColumn(input.Output, indexes[0])
	upper := uint64(math.MaxInt64)
	if offset < upper && limit < upper-offset {
		upper = offset + limit
	}
	if offset > math.MaxInt64 {
		offset = math.MaxInt64
	}
	predicate := andExpression(domainBinary(">", row, domainInt(offset)), domainBinary("<=", row, domainInt(upper)))
	return &LogicalPlan{Kind: LogicalFilter, Children: []*LogicalPlan{input}, Output: clonePlannerColumns(input.Output), Predicate: predicate}, nil
}

// planDomainQuery returns original query outputs followed by parameter columns.
func (p *QueryPlanner) planDomainQuery(ctx context.Context, query *binder.BoundQuery, domain *LogicalPlan, slots map[int]int, exists bool) (*LogicalPlan, []int, error) {
	innerInput, err := p.planDomainRelation(ctx, query.Source, domain, slots)
	if err != nil {
		return nil, nil, err
	}
	width := len(innerInput.Output) - len(domain.Output)
	global := len(query.GroupBy) == 0 && (queryHasAggregate(query) || query.Having != nil)
	marker := -1
	if global {
		projection := make([]*binder.BoundExpression, 0, len(innerInput.Output)+1)
		output := make([]Column, 0, len(innerInput.Output)+1)
		for i := 0; i < width; i++ {
			projection = append(projection, subqueryColumn(innerInput.Output, i))
			output = append(output, innerInput.Output[i])
		}
		marker = width
		projection = append(projection, domainInt(1))
		output = append(output, Column{Name: "__domain_present", Type: domainInt(1).Type})
		for i := range domain.Output {
			projection = append(projection, subqueryColumn(innerInput.Output, width+i))
			output = append(output, domain.Output[i])
		}
		innerInput = &LogicalPlan{Kind: LogicalProject, Children: []*LogicalPlan{innerInput}, Output: output, Projection: projection}
		width++
	}
	q, err := binder.RewriteOuterReferences(query, func(ref *binder.BoundExpression, depth int) (*binder.BoundExpression, error) {
		out := *ref
		out.ColumnIndex = width + slots[ref.ColumnIndex]
		if depth == 0 {
			out.Kind = binder.BoundColumn
			out.Depth = 0
		} else {
			out.Depth = depth
		}
		return &out, nil
	})
	if err != nil {
		return nil, nil, err
	}
	joined := innerInput
	if global {
		// The synthetic row for an empty parameter group must feed COUNT(*)
		// zero and every other aggregate NULL, including constant arguments.
		for i := 0; i < width; i++ {
			joined.Output[i].Nullable = true
		}
		joined, err = p.completeDomainGroups(ctx, joined, domain, q.Where, width)
		if err != nil {
			return nil, nil, err
		}
		q.Where = nil
		present := domainIsNull(subqueryColumn(joined.Output, marker), "IS NOT NULL")
		var mask func(*binder.BoundExpression) *binder.BoundExpression
		mask = func(e *binder.BoundExpression) *binder.BoundExpression {
			if e == nil {
				return nil
			}
			out := *e
			out.Children = append([]*binder.BoundExpression(nil), e.Children...)
			if e.Kind == binder.BoundFunction && e.FunctionKind == function.Aggregate {
				if e.FunctionID == function.CountID && len(e.Children) == 0 {
					out.Children = []*binder.BoundExpression{subqueryColumn(joined.Output, marker)}
				} else {
					for i, c := range e.Children {
						out.Children[i] = domainCase(present, c)
					}
				}
			} else {
				for i, c := range e.Children {
					out.Children[i] = mask(c)
				}
			}
			return &out
		}
		for i := range q.Projection {
			q.Projection[i].Expression = mask(q.Projection[i].Expression)
		}
		q.Having = mask(q.Having)
		for i := range q.OrderBy {
			if !q.OrderBy[i].UsesOutput {
				q.OrderBy[i].Expression = mask(q.OrderBy[i].Expression)
			}
		}
	}
	if exists && (!q.Distinct || q.Offset == nil) {
		q.Distinct = false
		setSubqueryProjection(q, []*binder.BoundExpression{domainInt(1)})
	}
	valueColumns := len(q.Output)
	for i := range domain.Output {
		key := subqueryColumn(joined.Output, width+i)
		if global || len(q.GroupBy) > 0 {
			q.GroupBy = append(q.GroupBy, key)
		}
		q.Projection = append(q.Projection, binder.BoundSelectItem{Expression: key, Output: binder.RelationColumn{Name: fmt.Sprintf("__domain_%d", i), Type: key.Type, Nullable: key.Nullable}})
		q.Output = append(q.Output, q.Projection[len(q.Projection)-1].Output)
	}
	limit, offset := q.Limit, q.Offset
	q.Limit, q.Offset = nil, nil
	order := q.OrderBy
	q.OrderBy = nil
	var sortKeys []SortKey
	if limit != nil || offset != nil {
		sortKeys, _, err = prepareStableOrder(q, order, q.Distinct)
		if err != nil {
			return nil, nil, err
		}
	}
	result, err := p.planOnDomainInput(ctx, q, joined)
	if err != nil {
		return nil, nil, err
	}
	keys := make([]int, len(domain.Output))
	for i := range keys {
		keys[i] = valueColumns + i
	}
	if limit != nil || offset != nil {
		result, err = limitDomainRows(result, keys, sortKeys, limit, offset)
		if err != nil {
			return nil, nil, err
		}
	}

	// Keep hidden ORDER BY columns and row numbers internal to this query block.
	keep := valueColumns + len(domain.Output)
	if len(result.Output) != keep {
		projection := make([]*binder.BoundExpression, keep)
		for i := range projection {
			projection[i] = subqueryColumn(result.Output, i)
		}
		result = &LogicalPlan{Kind: LogicalProject, Children: []*LogicalPlan{result}, Output: clonePlannerColumns(result.Output[:keep]), Projection: projection}
	}
	return result, keys, nil
}
