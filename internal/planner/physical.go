package planner

import (
	"fmt"

	"github.com/steamwo/fssql/internal/sql/binder"
)

// PhysicalKind 定义 Push Pipeline lowering 之前的 physical operator 类型。
type PhysicalKind uint8

const (
	PhysicalScan PhysicalKind = iota
	PhysicalFilter
	PhysicalProject
	PhysicalHashAggregate
	PhysicalDistinct
	PhysicalHashJoin
	PhysicalCrossJoin
	PhysicalSort
	PhysicalLimit
	PhysicalOneRow // PhysicalOneRow 为 SELECT 无 FROM 提供单行零列 source；追加在末尾保持既有 kind 数值。
)

// CardinalityEstimate 是 physical cost model 使用的 row estimate。
type CardinalityEstimate struct {
	Rows       uint64  // Rows 是估计输出行数。
	Confidence float64 // Confidence 是 [0,1] 估计置信度。
}

// PhysicalCost 保存 generic cost model 的相对成本，不绑定具体硬件单位。
type PhysicalCost struct {
	CPU    float64 // CPU 是相对计算成本。
	Memory uint64  // Memory 是估计状态行数/单位，不替代 MemoryManager reservation。
	IO     float64 // IO 是相对读取成本。
}

// PhysicalPlan 是 Physical Plan -> Pipeline DAG 之间的 generic IR。
type PhysicalPlan struct {
	Kind       PhysicalKind              // Kind 是 physical operator 类型。
	Children   []*PhysicalPlan           // Children 保持 SQL 语义顺序。
	Output     []Column                  // Output 保持 logical output schema/order。
	Scan       *ScanSpec                 // Scan 仅对 PhysicalScan 有效。
	Predicate  *binder.BoundExpression   // Predicate 用于 filter/join residual。
	Projection []*binder.BoundExpression // Projection 用于 PhysicalProject。
	GroupBy    []*binder.BoundExpression // GroupBy 用于 hash aggregate。
	Aggregates []AggregateSpec           // Aggregates 用于 hash aggregate。
	Join       JoinType                  // Join 保存 SQL join 语义。
	Sort       []SortKey                 // Sort 保存排序语义。
	Limit      uint64                    // Limit 是 physical limit。
	Offset     uint64                    // Offset 是 physical offset。
	Estimate   CardinalityEstimate       // Estimate 是 optimizer 估计。
	Cost       PhysicalCost              // Cost 是 physical cost model 结果。

	RequiredOutput []bool // JOIN output mask; nil means unknown/all, schema indexes stay unchanged.
}

// Validate 验证 physical tree shape，并冻结同一 relation 的 scan 引用数。
// lowering 在 Validate 后打开 source，因此运行时可以据此决定单引用直通或多引用物化。
func (p *PhysicalPlan) Validate() error {
	if p == nil {
		return fmt.Errorf("physical plan is nil")
	}
	counts := make(map[string]int)
	countPhysicalScanReferences(p, counts)
	applyPhysicalScanReferences(p, counts)
	return p.validateShape()
}

func countPhysicalScanReferences(p *PhysicalPlan, counts map[string]int) {
	if p == nil {
		return
	}
	if p.Kind == PhysicalScan && p.Scan != nil && p.Scan.RelationID != "" {
		counts[p.Scan.RelationID]++
	}
	for _, child := range p.Children {
		countPhysicalScanReferences(child, counts)
	}
}

func applyPhysicalScanReferences(p *PhysicalPlan, counts map[string]int) {
	if p == nil {
		return
	}
	if p.Kind == PhysicalScan && p.Scan != nil {
		p.Scan.References = counts[p.Scan.RelationID]
	}
	for _, child := range p.Children {
		applyPhysicalScanReferences(child, counts)
	}
}

func (p *PhysicalPlan) validateShape() error {
	if p == nil {
		return fmt.Errorf("physical plan is nil")
	}
	switch p.Kind {
	case PhysicalOneRow:
		if len(p.Children) != 0 || len(p.Output) != 0 || p.Scan != nil {
			return fmt.Errorf("physical one-row source requires no children/output/scan")
		}
	case PhysicalScan:
		if p.Scan == nil || len(p.Children) != 0 {
			return fmt.Errorf("physical scan requires ScanSpec and no children")
		}
	case PhysicalFilter:
		if len(p.Children) != 1 || p.Predicate == nil {
			return fmt.Errorf("physical filter requires child and predicate")
		}
	case PhysicalProject:
		if len(p.Children) != 1 || len(p.Projection) == 0 {
			return fmt.Errorf("physical project requires child and projection")
		}
	case PhysicalHashAggregate, PhysicalDistinct, PhysicalSort, PhysicalLimit:
		if len(p.Children) != 1 {
			return fmt.Errorf("physical unary operator requires one child")
		}
	case PhysicalHashJoin:
		if len(p.Children) != 2 || p.Predicate == nil || p.Join == JoinCross {
			return fmt.Errorf("physical hash join requires two children and non-cross predicate")
		}
	case PhysicalCrossJoin:
		if len(p.Children) != 2 || p.Join != JoinCross {
			return fmt.Errorf("physical cross join requires two children and cross semantics")
		}
	default:
		return fmt.Errorf("unknown physical kind %d", p.Kind)
	}
	for _, child := range p.Children {
		if err := child.validateShape(); err != nil {
			return err
		}
	}
	return nil
}
