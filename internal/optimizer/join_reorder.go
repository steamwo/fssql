package optimizer

import (
	"context"
	"math"

	"github.com/steamwo/fssql/internal/core/types"
	"github.com/steamwo/fssql/internal/planner"
	"github.com/steamwo/fssql/internal/sql/binder"
)

type joinInput struct {
	plan        *planner.LogicalPlan
	start       int
	rows        float64
	selectivity float64
}

type joinEdge struct {
	left, right int // Original join tree column indexes.
	expr        *binder.BoundExpression
}

type joinGraph struct {
	inputs []joinInput
	owners []int // Original column index to input index.
	edges  []joinEdge
}

func (o *Optimizer) reorderJoins(ctx context.Context, plan *planner.LogicalPlan) (*planner.LogicalPlan, error) {
	if o.Statistics == nil {
		return plan, nil
	}
	if plan.Kind == planner.LogicalSort {
		if _, sample := planner.IsSampleSort(plan.Sort); sample {
			return plan, nil
		}
	}
	root := plan
	if plan.Kind == planner.LogicalFilter {
		root = plan.Children[0]
	}
	if root.Kind == planner.LogicalJoin && (root.Join == planner.JoinInner || root.Join == planner.JoinCross) {
		var graph joinGraph
		if graph.collect(root) && len(graph.inputs) >= 3 {
			var where *binder.BoundExpression
			if root != plan {
				where = plan.Predicate
			}
			rewritten, err := o.reorderJoinGraph(ctx, root, where, &graph)
			if err != nil {
				return nil, err
			}
			if rewritten != nil {
				return rewritten, nil
			}
		}
	}
	for i, child := range plan.Children {
		rewritten, err := o.reorderJoins(ctx, child)
		if err != nil {
			return nil, err
		}
		plan.Children[i] = rewritten
	}
	return plan, nil
}

func (g *joinGraph) collect(plan *planner.LogicalPlan) bool {
	start := len(g.owners)
	if plan.Kind != planner.LogicalJoin {
		if len(plan.Output) == 0 {
			return false
		}
		for range plan.Output {
			g.owners = append(g.owners, len(g.inputs))
		}
		g.inputs = append(g.inputs, joinInput{plan: plan, start: start, selectivity: 1})
		return true
	}
	if (plan.Join != planner.JoinInner && plan.Join != planner.JoinCross) || len(plan.RequiredOutput) != 0 {
		return false
	}
	left, right := plan.Children[0], plan.Children[1]
	if !isConcatenatedJoinOutput(plan, left, right) || !g.collect(left) || !g.collect(right) {
		return false
	}
	if plan.Join == planner.JoinCross {
		return plan.Predicate == nil
	}
	return visitJoinConjuncts(plan.Predicate, func(expr *binder.BoundExpression) bool {
		a, b, ok := columnEquality(expr)
		width := len(left.Output)
		if !ok || a < 0 || b < 0 || a >= len(plan.Output) || b >= len(plan.Output) || (a < width) == (b < width) {
			return false
		}
		g.edges = append(g.edges, joinEdge{start + a, start + b, expr})
		return true
	})
}

func isConcatenatedJoinOutput(plan, left, right *planner.LogicalPlan) bool {
	if len(plan.Output) != len(left.Output)+len(right.Output) {
		return false
	}
	match := func(actual, expected []planner.Column) bool {
		for i, col := range expected {
			if got := actual[i]; got.Name != col.Name || got.Nullable != col.Nullable || !got.Type.Same(col.Type) {
				return false
			}
		}
		return true
	}
	return match(plan.Output[:len(left.Output)], left.Output) && match(plan.Output[len(left.Output):], right.Output)
}

func visitJoinConjuncts(expr *binder.BoundExpression, visit func(*binder.BoundExpression) bool) bool {
	if expr == nil {
		return false
	}
	if expr.Kind == binder.BoundBinary && expr.Op == "AND" && len(expr.Children) == 2 {
		return visitJoinConjuncts(expr.Children[0], visit) && visitJoinConjuncts(expr.Children[1], visit)
	}
	return visit(expr)
}

func columnEquality(expr *binder.BoundExpression) (int, int, bool) {
	if expr.Kind != binder.BoundBinary || expr.Op != "=" || len(expr.Children) != 2 {
		return 0, 0, false
	}
	a, b := expr.Children[0], expr.Children[1]
	if a == nil || b == nil || a.Kind != binder.BoundColumn || b.Kind != binder.BoundColumn || !a.Type.Same(b.Type) {
		return 0, 0, false
	}
	return a.ColumnIndex, b.ColumnIndex, true
}

func (o *Optimizer) joinInputEstimate(ctx context.Context, node *planner.LogicalPlan) (planner.CardinalityEstimate, float64, error) {
	selectivity := 1.0
	for {
		switch node.Kind {
		case planner.LogicalScan:
			estimate, err := o.scanEstimate(ctx, node.Scan)
			return estimate, selectivity, err
		case planner.LogicalFilter:
			if !visitJoinConjuncts(node.Predicate, func(expr *binder.BoundExpression) bool {
				_, ok := simpleJoinFilter(expr)
				return ok
			}) {
				return planner.CardinalityEstimate{}, 0, nil
			}
			selectivity *= 0.1
		case planner.LogicalProject:
			if _, ok := projectionColumns(node.Projection); !ok {
				return planner.CardinalityEstimate{}, 0, nil
			}
		default:
			return planner.CardinalityEstimate{}, 0, nil
		}
		node = node.Children[0]
	}
}

func (o *Optimizer) reorderJoinGraph(ctx context.Context, root *planner.LogicalPlan, where *binder.BoundExpression, g *joinGraph) (*planner.LogicalPlan, error) {
	for i := range g.inputs {
		input := &g.inputs[i]
		estimate, selectivity, err := o.joinInputEstimate(ctx, input.plan)
		if err != nil {
			return nil, err
		}
		if estimate.Confidence <= 0 {
			return nil, nil
		}
		input.rows = float64(estimate.Rows)
		input.selectivity = selectivity
	}
	if where != nil && g.addWhere(where) {
		where = nil
	}
	plan := g.build()
	if plan == nil {
		return nil, nil
	}
	out, positions := plan.node, plan.positions
	for i, mapped := range positions {
		if i == mapped {
			continue
		}
		projection := make([]*binder.BoundExpression, len(root.Output))
		for index, col := range root.Output {
			projection[index] = &binder.BoundExpression{Kind: binder.BoundColumn, Type: col.Type, Nullable: col.Nullable, ColumnIndex: positions[index]}
		}
		out = &planner.LogicalPlan{Kind: planner.LogicalProject, Output: root.Output,
			Children: []*planner.LogicalPlan{out}, Projection: projection}
		break
	}
	if where != nil {
		out = &planner.LogicalPlan{Kind: planner.LogicalFilter, Output: root.Output,
			Children: []*planner.LogicalPlan{out}, Predicate: where}
	}
	return out, nil
}

// addWhere only moves conjunctions whose predicates are safe to evaluate on
// individual inputs or as hash keys. Other predicates stay above the restored
// join layout, including expressions with runtime constants or outer columns.
func (g *joinGraph) addWhere(where *binder.BoundExpression) bool {
	edges := g.edges
	filters := make([]*binder.BoundExpression, len(g.inputs))
	if !visitJoinConjuncts(where, func(expr *binder.BoundExpression) bool {
		if a, b, ok := columnEquality(expr); ok {
			if a >= 0 && b >= 0 && a < len(g.owners) && b < len(g.owners) && g.owners[a] != g.owners[b] {
				edges = append(edges, joinEdge{a, b, expr})
				return true
			}
		}
		col, ok := simpleJoinFilter(expr)
		if !ok || col < 0 || col >= len(g.owners) {
			return false
		}
		i := g.owners[col]
		local := remapJoinExpr(expr, func(index int) int { return index - g.inputs[i].start })
		filters[i] = joinAnd(filters[i], local)
		return true
	}) {
		return false
	}
	g.edges = edges
	for i, filter := range filters {
		if filter != nil {
			input := &g.inputs[i]
			input.selectivity *= 0.1
			input.plan = &planner.LogicalPlan{Kind: planner.LogicalFilter, Output: input.plan.Output,
				Children: []*planner.LogicalPlan{input.plan}, Predicate: filter}
		}
	}
	return true
}

func simpleJoinFilter(expr *binder.BoundExpression) (int, bool) {
	if expr.Kind == binder.BoundIsNull && len(expr.Children) == 1 {
		col := expr.Children[0]
		if col != nil && col.Kind == binder.BoundColumn {
			return col.ColumnIndex, true
		}
		return 0, false
	}
	if expr.Kind != binder.BoundBinary || len(expr.Children) != 2 {
		return 0, false
	}
	switch expr.Op {
	case "=", "<>", "!=", "<", "<=", ">", ">=":
	default:
		return 0, false
	}
	col, literal := expr.Children[0], expr.Children[1]
	if col == nil || literal == nil {
		return 0, false
	}
	if col.Kind != binder.BoundColumn {
		col, literal = literal, col
	}
	if col.Kind != binder.BoundColumn || literal.Kind != binder.BoundLiteral || !col.Type.Same(literal.Type) {
		return 0, false
	}
	switch col.Type.ID {
	case types.Boolean, types.BigInt, types.Integer, types.SmallInt, types.TinyInt, types.Varchar, types.DateType:
		return col.ColumnIndex, true
	default:
		return 0, false
	}
}

func joinAnd(left, right *binder.BoundExpression) *binder.BoundExpression {
	if left == nil {
		return right
	}
	return &binder.BoundExpression{Kind: binder.BoundBinary, Op: "AND", Type: types.LogicalType{ID: types.Boolean},
		Nullable: left.Nullable || right.Nullable, Children: []*binder.BoundExpression{left, right}}
}

func remapJoinExpr(expr *binder.BoundExpression, index func(int) int) *binder.BoundExpression {
	out := *expr
	if expr.Kind == binder.BoundColumn {
		out.ColumnIndex = index(expr.ColumnIndex)
	}
	if len(expr.Children) != 0 {
		out.Children = make([]*binder.BoundExpression, len(expr.Children))
		for i, child := range expr.Children {
			out.Children[i] = remapJoinExpr(child, index)
		}
	}
	return &out
}

type reorderedJoin struct {
	node      *planner.LogicalPlan
	positions []int // Original column index to current join column index.
}

func (g *joinGraph) build() *reorderedJoin {
	best := math.Inf(1)
	first, second := -1, -1
	var rows float64
	for _, edge := range g.edges {
		a, b := g.owners[edge.left], g.owners[edge.right]
		left, right := g.inputs[a], g.inputs[b]
		output := max(left.rows, right.rows) * left.selectivity * right.selectivity
		lrows, rrows := left.rows*left.selectivity, right.rows*right.selectivity
		score := output + min(lrows, rrows)
		if score < best {
			first, second, best, rows = a, b, score, output
			if lrows < rrows {
				first, second = b, a
			}
		}
	}
	if first < 0 {
		return nil
	}
	state := &reorderedJoin{node: g.inputs[first].plan, positions: make([]int, len(g.owners))}
	joined := make([]bool, len(g.inputs))
	for i := range state.node.Output {
		state.positions[g.inputs[first].start+i] = i
	}
	joined[first] = true
	g.appendJoin(state, joined, second, false)

	for count := 2; count < len(g.inputs); count++ {
		next, nextRows := -1, 0.0
		best = math.Inf(1)
		for _, edge := range g.edges {
			a, b := g.owners[edge.left], g.owners[edge.right]
			if joined[a] == joined[b] {
				continue
			}
			if joined[b] {
				b = a
			}
			input := g.inputs[b]
			output := max(rows, input.rows) * input.selectivity
			score := output + min(rows, input.rows*input.selectivity)
			if score < best {
				next, nextRows, best = b, output, score
			}
		}
		if next < 0 {
			return nil // The graph requires a Cartesian product.
		}
		input := g.inputs[next]
		g.appendJoin(state, joined, next, input.rows*input.selectivity > rows)
		rows = nextRows
	}
	return state
}

func (g *joinGraph) appendJoin(state *reorderedJoin, joined []bool, next int, inputFirst bool) {
	input, prior := g.inputs[next], state.node
	left, right := prior, input.plan
	offset := len(prior.Output)
	if inputFirst {
		left, right, offset = right, left, 0
		for i, owner := range g.owners {
			if joined[owner] {
				state.positions[i] += len(input.plan.Output)
			}
		}
	}
	for i := range input.plan.Output {
		state.positions[input.start+i] = offset + i
	}
	var predicate *binder.BoundExpression
	for _, edge := range g.edges {
		a, b := g.owners[edge.left], g.owners[edge.right]
		if (a == next && joined[b]) || (b == next && joined[a]) {
			expr := remapJoinExpr(edge.expr, func(i int) int { return state.positions[i] })
			predicate = joinAnd(predicate, expr)
		}
	}
	joined[next] = true
	output := append(append([]planner.Column(nil), left.Output...), right.Output...)
	state.node = &planner.LogicalPlan{Kind: planner.LogicalJoin, Join: planner.JoinInner, Output: output,
		Children: []*planner.LogicalPlan{left, right}, Predicate: predicate}
}
