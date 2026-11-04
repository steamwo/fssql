package optimizer

import (
	"context"
	"fmt"
	"math"
	"sort"

	"github.com/steamwo/fssql/internal/core/stats"
	"github.com/steamwo/fssql/internal/planner"
	"github.com/steamwo/fssql/internal/sql/binder"
)

// Optimizer 实现 rule optimizer + statistics/cardinality + join-order candidate + physical cost model。
type Optimizer struct {
	Statistics stats.Provider // Statistics 是唯一 statistics 数据源 contract；可为空表示 Unknown。
}

// Optimize 复制 logical tree、标记安全的 generic pushdown candidate，并生成 physical plan。
func (o *Optimizer) Optimize(ctx context.Context, logical *planner.LogicalPlan) (*planner.PhysicalPlan, error) {
	if logical == nil {
		return nil, fmt.Errorf("optimizer: nil logical plan")
	}
	if err := logical.Validate(); err != nil {
		return nil, err
	}
	copyPlan := cloneLogical(logical)
	o.markPushdownCandidates(copyPlan)
	physical, err := o.lower(ctx, copyPlan)
	if err != nil {
		return nil, err
	}
	if err := physical.Validate(); err != nil {
		return nil, err
	}
	return physical, nil
}

// markPushdownCandidates 只标记 candidate，不删除 residual logical operator；format adapter 未接受时语义仍正确。
func (o *Optimizer) markPushdownCandidates(p *planner.LogicalPlan) {
	for _, child := range p.Children {
		o.markPushdownCandidates(child)
	}
	if len(p.Children) != 1 {
		return
	}
	child := p.Children[0]
	switch p.Kind {
	case planner.LogicalFilter:
		if child.Kind == planner.LogicalScan && child.Scan != nil {
			child.Scan.Pushdown.Filters = append(child.Scan.Pushdown.Filters, p.Predicate)
		}
	case planner.LogicalProject:
		needed := make(map[int]struct{})
		for _, expr := range p.Projection {
			collectBoundColumnIndexes(expr, needed)
		}
		o.markRequiredScanColumns(child, needed)
	case planner.LogicalAggregate:
		needed := make(map[int]struct{})
		for _, expr := range p.GroupBy {
			collectBoundColumnIndexes(expr, needed)
		}
		for _, aggregate := range p.Aggregates {
			for _, expr := range aggregate.Arguments {
				collectBoundColumnIndexes(expr, needed)
			}
		}
		o.markRequiredScanColumns(child, needed)
	case planner.LogicalLimit:
		// LIMIT may cross a pure-column Project because it neither changes row
		// count nor evaluates expressions that could introduce observable errors.
		// It must not cross Filter/Sort/Aggregate/Join.
		scan := directScanBelowPureColumnProject(child)
		if scan == nil || scan.Scan == nil {
			return
		}
		// UnlimitedRows 只表示 OFFSET-only；不得把 sentinel 作为真实 scan LIMIT candidate 下推。
		// LIMIT 0 由 residual PhysicalLimit 精确执行，ScanPushdown.Limit 的 0 仍保留“未接受 pushdown”语义。
		if p.Offset == 0 && p.Limit != planner.UnlimitedRows && p.Limit > 0 && (scan.Scan.Pushdown.Limit == 0 || p.Limit < scan.Scan.Pushdown.Limit) {
			scan.Scan.Pushdown.Limit = p.Limit
		}
	}
}

// Only cross operators that preserve relation indexes. Every residual
// expression remains executable, including predicates unsupported by metadata.
func (o *Optimizer) markRequiredScanColumns(node *planner.LogicalPlan, needed map[int]struct{}) {
	for node != nil {
		switch node.Kind {
		case planner.LogicalScan:
			if node.Scan != nil && len(needed) > 0 {
				node.Scan.Pushdown.Projection = sortedColumnIndexes(needed)
			}
			return
		case planner.LogicalFilter:
			collectBoundColumnIndexes(node.Predicate, needed)
		case planner.LogicalSort:
			// Global windows append columns; their output indexes are not scan indexes.
			if _, ok := planner.IsPhase5GlobalWindowSort(node.Sort); ok {
				return
			}
			for _, key := range node.Sort {
				collectBoundColumnIndexes(key.Expr, needed)
			}
		case planner.LogicalLimit:
		default:
			return
		}
		if len(node.Children) != 1 {
			return
		}
		node = node.Children[0]
	}
}

// projectionColumns 仅将纯列投影转成 scan projection candidate；表达式投影保持 residual。
func projectionColumns(exprs []*binder.BoundExpression) ([]int, bool) {
	cols := make([]int, len(exprs))
	for i, e := range exprs {
		if e == nil || e.Kind != binder.BoundColumn || e.ColumnIndex < 0 {
			return nil, false
		}
		cols[i] = e.ColumnIndex
	}
	return cols, true
}

// scanBelowResidualFilters accepts only Filter* -> Scan. Filter outputs preserve
// the scan schema/order, so Project ColumnIndex values still refer to the same
// bound relation indexes. No other operator is crossed.
func scanBelowResidualFilters(node *planner.LogicalPlan) (*planner.LogicalPlan, []*binder.BoundExpression) {
	filters := make([]*binder.BoundExpression, 0, 2)
	for node != nil && node.Kind == planner.LogicalFilter && len(node.Children) == 1 {
		filters = append(filters, node.Predicate)
		node = node.Children[0]
	}
	if node != nil && node.Kind == planner.LogicalScan && node.Scan != nil {
		return node, filters
	}
	return nil, nil
}

// directScanBelowPureColumnProject permits exactly Project -> Scan in addition
// to direct Scan. Multiple project layers would require index remapping and are
// deliberately left residual until that contract exists.
func directScanBelowPureColumnProject(node *planner.LogicalPlan) *planner.LogicalPlan {
	if node == nil {
		return nil
	}
	if node.Kind == planner.LogicalScan && node.Scan != nil {
		return node
	}
	if node.Kind != planner.LogicalProject || len(node.Children) != 1 {
		return nil
	}
	if _, ok := projectionColumns(node.Projection); !ok {
		return nil
	}
	child := node.Children[0]
	if child != nil && child.Kind == planner.LogicalScan && child.Scan != nil {
		return child
	}
	return nil
}

func collectBoundColumnIndexes(expression *binder.BoundExpression, out map[int]struct{}) {
	if expression == nil {
		return
	}
	if expression.Kind == binder.BoundColumn && expression.ColumnIndex >= 0 {
		out[expression.ColumnIndex] = struct{}{}
	}
	for _, child := range expression.Children {
		collectBoundColumnIndexes(child, out)
	}
}

func sortedColumnIndexes(indexes map[int]struct{}) []int {
	out := make([]int, 0, len(indexes))
	for index := range indexes {
		out = append(out, index)
	}
	sort.Ints(out)
	return out
}

// lower 将 logical node 映射到 generic physical node，并保留 Push Pipeline 所需语义顺序。
func (o *Optimizer) lower(ctx context.Context, p *planner.LogicalPlan) (*planner.PhysicalPlan, error) {
	children := make([]*planner.PhysicalPlan, len(p.Children))
	for i, child := range p.Children {
		physical, err := o.lower(ctx, child)
		if err != nil {
			return nil, err
		}
		children[i] = physical
	}
	est, err := o.estimate(ctx, p, children)
	if err != nil {
		return nil, err
	}
	out := &planner.PhysicalPlan{
		Children:   children,
		Output:     append([]planner.Column(nil), p.Output...),
		Scan:       cloneScan(p.Scan),
		Predicate:  p.Predicate,
		Projection: append([]*binder.BoundExpression(nil), p.Projection...),
		GroupBy:    append([]*binder.BoundExpression(nil), p.GroupBy...),
		Aggregates: append([]planner.AggregateSpec(nil), p.Aggregates...),
		Join:       p.Join,
		Sort:       append([]planner.SortKey(nil), p.Sort...),
		Limit:      p.Limit,
		Offset:     p.Offset,
		Estimate:   est,
	}
	switch p.Kind {
	case planner.LogicalOneRow:
		out.Kind = planner.PhysicalOneRow
	case planner.LogicalScan:
		out.Kind = planner.PhysicalScan
	case planner.LogicalFilter:
		out.Kind = planner.PhysicalFilter
	case planner.LogicalProject:
		out.Kind = planner.PhysicalProject
	case planner.LogicalAggregate:
		out.Kind = planner.PhysicalHashAggregate
	case planner.LogicalDistinct:
		out.Kind = planner.PhysicalDistinct
	case planner.LogicalJoin:
		if p.Join == planner.JoinCross {
			out.Kind = planner.PhysicalCrossJoin
		} else {
			out.Kind = planner.PhysicalHashJoin
		}
	case planner.LogicalSort:
		out.Kind = planner.PhysicalSort
	case planner.LogicalLimit:
		out.Kind = planner.PhysicalLimit
	default:
		return nil, fmt.Errorf("optimizer: unsupported logical kind %d", p.Kind)
	}
	out.Cost = cost(out)
	return out, nil
}

// estimate 计算 conservative cardinality；Unknown statistics 保持低置信度而不是伪造精确结果。
func (o *Optimizer) estimate(ctx context.Context, p *planner.LogicalPlan, children []*planner.PhysicalPlan) (planner.CardinalityEstimate, error) {
	switch p.Kind {
	case planner.LogicalOneRow:
		return planner.CardinalityEstimate{Rows: 1, Confidence: 1}, nil
	case planner.LogicalScan:
		if o.Statistics == nil || p.Scan == nil || p.Scan.Fingerprint == "" {
			return planner.CardinalityEstimate{}, nil
		}
		s, ok, err := o.Statistics.Lookup(ctx, p.Scan.Fingerprint)
		if err != nil {
			return planner.CardinalityEstimate{}, err
		}
		if !ok || !s.Valid() || s.Fingerprint != p.Scan.Fingerprint {
			return planner.CardinalityEstimate{}, nil
		}
		return planner.CardinalityEstimate{Rows: s.Rows, Confidence: float64(s.Confidence)}, nil
	case planner.LogicalFilter:
		return scale(children[0].Estimate, 0.1, 0.7), nil
	case planner.LogicalProject:
		return children[0].Estimate, nil
	case planner.LogicalAggregate:
		return scale(children[0].Estimate, 0.1, 0.6), nil
	case planner.LogicalDistinct:
		return scale(children[0].Estimate, 0.5, 0.6), nil
	case planner.LogicalSort:
		return children[0].Estimate, nil
	case planner.LogicalLimit:
		e := children[0].Estimate
		// Rows==0 且 Confidence==0 表示 Unknown；OFFSET-only 不能把 UnlimitedRows sentinel 伪造成真实 cardinality。
		if e.Rows > 0 {
			if p.Offset >= e.Rows {
				e.Rows = 0
			} else {
				e.Rows -= p.Offset
			}
		}
		if p.Limit != planner.UnlimitedRows {
			if p.Limit == 0 {
				// LIMIT 0 即使 child statistics Unknown 也有精确结果 0。
				e.Rows = 0
				e.Confidence = 1
			} else if e.Rows == 0 || p.Limit < e.Rows {
				// Unknown child 用 LIMIT 作为保守上界；已知 child 取 min(rows, limit)。
				e.Rows = p.Limit
			}
		}
		return e, nil
	case planner.LogicalJoin:
		left, right := children[0].Estimate, children[1].Estimate
		rows := saturatingMul(left.Rows, right.Rows)
		if p.Join != planner.JoinCross {
			rows = uint64(float64(rows) * 0.1)
		}
		return planner.CardinalityEstimate{Rows: rows, Confidence: minFloat(left.Confidence, right.Confidence) * 0.5}, nil
	default:
		return planner.CardinalityEstimate{}, nil
	}
}

// JoinOrder 返回 inner/cross join leaf 的统计驱动候选顺序；它不重写已绑定树，避免 ColumnIndex 失配。
func (o *Optimizer) JoinOrder(ctx context.Context, root *planner.LogicalPlan) ([]string, error) {
	leaves := make([]joinLeaf, 0, 4)
	if !collectReorderableLeaves(root, &leaves) {
		return nil, fmt.Errorf("join order requires an inner/cross join tree of scans")
	}
	for i := range leaves {
		if o.Statistics == nil || leaves[i].fingerprint == "" {
			continue
		}
		s, ok, err := o.Statistics.Lookup(ctx, leaves[i].fingerprint)
		if err != nil {
			return nil, err
		}
		if ok && s.Valid() && s.Fingerprint == leaves[i].fingerprint {
			leaves[i].rows = s.Rows
			leaves[i].known = true
		}
	}
	sort.SliceStable(leaves, func(i, j int) bool {
		if leaves[i].known != leaves[j].known {
			return leaves[i].known
		}
		if leaves[i].known && leaves[i].rows != leaves[j].rows {
			return leaves[i].rows < leaves[j].rows
		}
		return leaves[i].relationID < leaves[j].relationID
	})
	out := make([]string, len(leaves))
	for i, leaf := range leaves {
		out[i] = leaf.relationID
	}
	return out, nil
}

type joinLeaf struct {
	relationID  string
	fingerprint stats.DatasetFingerprint
	rows        uint64
	known       bool
}

func collectReorderableLeaves(p *planner.LogicalPlan, out *[]joinLeaf) bool {
	if p == nil {
		return false
	}
	if p.Kind == planner.LogicalScan && p.Scan != nil {
		*out = append(*out, joinLeaf{relationID: p.Scan.RelationID, fingerprint: p.Scan.Fingerprint})
		return true
	}
	if p.Kind != planner.LogicalJoin || (p.Join != planner.JoinInner && p.Join != planner.JoinCross) || len(p.Children) != 2 {
		return false
	}
	return collectReorderableLeaves(p.Children[0], out) && collectReorderableLeaves(p.Children[1], out)
}

// cost 为 physical operator 计算相对成本；Memory 数值仅用于比较，不替代真实 reservation。
func cost(p *planner.PhysicalPlan) planner.PhysicalCost {
	rows := float64(p.Estimate.Rows)
	c := planner.PhysicalCost{}
	switch p.Kind {
	case planner.PhysicalOneRow:
		c.CPU = 0
	case planner.PhysicalScan:
		c.IO, c.CPU = rows, rows*0.1
	case planner.PhysicalFilter, planner.PhysicalProject, planner.PhysicalLimit:
		c.CPU = rows
	case planner.PhysicalHashAggregate, planner.PhysicalDistinct:
		c.CPU, c.Memory = rows*1.5, p.Estimate.Rows
	case planner.PhysicalHashJoin:
		c.CPU, c.Memory = rows*2, p.Estimate.Rows
	case planner.PhysicalCrossJoin:
		c.CPU = rows
	case planner.PhysicalSort:
		if rows > 1 {
			c.CPU = rows * math.Log2(rows)
		}
		c.Memory = p.Estimate.Rows
	}
	for _, child := range p.Children {
		c.CPU += child.Cost.CPU
		c.IO += child.Cost.IO
		if child.Cost.Memory > c.Memory {
			c.Memory = child.Cost.Memory
		}
	}
	return c
}

func scale(e planner.CardinalityEstimate, factor, confidenceFactor float64) planner.CardinalityEstimate {
	if e.Rows > 0 {
		e.Rows = uint64(float64(e.Rows) * factor)
		if e.Rows == 0 {
			e.Rows = 1
		}
	}
	e.Confidence *= confidenceFactor
	return e
}

func saturatingMul(a, b uint64) uint64 {
	if a == 0 || b == 0 {
		return 0
	}
	if a > math.MaxUint64/b {
		return math.MaxUint64
	}
	return a * b
}

func minFloat(a, b float64) float64 {
	if a < b {
		return a
	}
	return b
}

func cloneLogical(p *planner.LogicalPlan) *planner.LogicalPlan {
	if p == nil {
		return nil
	}
	out := *p
	out.Children = make([]*planner.LogicalPlan, len(p.Children))
	for i, child := range p.Children {
		out.Children[i] = cloneLogical(child)
	}
	out.Output = append([]planner.Column(nil), p.Output...)
	out.Scan = cloneScan(p.Scan)
	out.Projection = append([]*binder.BoundExpression(nil), p.Projection...)
	out.GroupBy = append([]*binder.BoundExpression(nil), p.GroupBy...)
	out.Aggregates = append([]planner.AggregateSpec(nil), p.Aggregates...)
	out.Sort = append([]planner.SortKey(nil), p.Sort...)
	return &out
}

func cloneScan(s *planner.ScanSpec) *planner.ScanSpec {
	if s == nil {
		return nil
	}
	out := *s
	out.Pushdown.Projection = append([]int(nil), s.Pushdown.Projection...)
	out.Pushdown.Filters = append([]*binder.BoundExpression(nil), s.Pushdown.Filters...)
	return &out
}
