package planner

import "github.com/steamwo/fssql/internal/sql/binder"

// EXISTS consumes row presence. Keep expressions that affect filtering,
// grouping or distinct row counts, and suppress unused SELECT expressions.
func pruneExistsProjection(plan *LogicalPlan, needed map[int]bool) *LogicalPlan {
	out := *plan
	out.Children = append([]*LogicalPlan(nil), plan.Children...)
	childNeeded := map[int]bool{}
	for index := range needed {
		childNeeded[index] = true
	}
	collect := func(expr *binder.BoundExpression) { collectExistsColumns(expr, childNeeded) }
	switch plan.Kind {
	case LogicalProject:
		childNeeded = map[int]bool{}
		out.Projection = append([]*binder.BoundExpression(nil), plan.Projection...)
		for i, expr := range plan.Projection {
			if needed[i] {
				collectExistsColumns(expr, childNeeded)
			} else {
				out.Projection[i] = &binder.BoundExpression{Kind: binder.BoundNull, Type: expr.Type, Nullable: true}
			}
		}
	case LogicalFilter:
		collect(plan.Predicate)
	case LogicalSort:
		_, sample := IsPhase5SampleSort(plan.Sort)
		_, cte := IsPhase5CTESort(plan.Sort)
		_, window := IsPhase5GlobalWindowSort(plan.Sort)
		if len(needed) == 0 && !sample && !cte && !window {
			return pruneExistsProjection(plan.Children[0], needed)
		}
		for _, key := range plan.Sort {
			collect(key.Expr)
		}
	case LogicalDistinct:
		for i := range plan.Output {
			childNeeded[i] = true
		}
	case LogicalAggregate:
		childNeeded = map[int]bool{}
		for _, group := range plan.GroupBy {
			collectExistsColumns(group, childNeeded)
		}
		out.Aggregates = append([]AggregateSpec(nil), plan.Aggregates...)
		input := plan.Children[0]
		projection := phase5PassThrough(input.Output)
		columns := clonePlannerColumns(input.Output)
		for i, spec := range plan.Aggregates {
			if needed[len(plan.GroupBy)+i] {
				for _, arg := range spec.Arguments {
					collectExistsColumns(arg, childNeeded)
				}
				continue
			}
			out.Aggregates[i].Arguments = make([]*binder.BoundExpression, len(spec.Arguments))
			for j, arg := range spec.Arguments {
				index := len(columns)
				projection = append(projection, &binder.BoundExpression{Kind: binder.BoundNull, Type: arg.Type, Nullable: true})
				columns = append(columns, Column{Name: "__exists_unused", Type: arg.Type, Nullable: true})
				out.Aggregates[i].Arguments[j] = &binder.BoundExpression{Kind: binder.BoundColumn, Type: arg.Type, Nullable: true, ColumnIndex: index}
				childNeeded[index] = true
			}
		}
		if len(columns) != len(input.Output) {
			out.Children[0] = &LogicalPlan{Kind: LogicalProject, Children: []*LogicalPlan{input}, Output: columns, Projection: projection}
		}
	case LogicalJoin:
		collect(plan.Predicate)
		left, right := map[int]bool{}, map[int]bool{}
		width := len(plan.Children[0].Output)
		for index := range childNeeded {
			if index < width {
				left[index] = true
			} else {
				right[index-width] = true
			}
		}
		out.Children[0] = pruneExistsProjection(plan.Children[0], left)
		out.Children[1] = pruneExistsProjection(plan.Children[1], right)
		return &out
	}
	for i, child := range out.Children {
		out.Children[i] = pruneExistsProjection(child, childNeeded)
	}
	return &out
}

func collectExistsColumns(expr *binder.BoundExpression, needed map[int]bool) {
	if expr == nil {
		return
	}
	if expr.Kind == binder.BoundColumn {
		needed[expr.ColumnIndex] = true
	}
	for _, child := range expr.Children {
		collectExistsColumns(child, needed)
	}
}
