package planner

import (
	"fmt"

	"github.com/steamwo/fssql/internal/core/stats"
	"github.com/steamwo/fssql/internal/core/types"
	"github.com/steamwo/fssql/internal/sql/binder"
)

// LogicalKind 定义 optimizer 输入的逻辑算子类型。
type LogicalKind uint8

const (
	LogicalScan LogicalKind = iota
	LogicalFilter
	LogicalProject
	LogicalAggregate
	LogicalDistinct
	LogicalJoin
	LogicalSort
	LogicalLimit
	LogicalOneRow // LogicalOneRow 为 SELECT 无 FROM 提供单行零列关系；追加在末尾避免改变既有 kind 数值。
	LogicalUnionAll
	LogicalApply
)

// JoinType 是 planner 层的 SQL join 语义，不包含具体执行算法。
type JoinType uint8

const (
	JoinInner JoinType = iota
	JoinLeft
	JoinRight
	JoinFull
	JoinCross
	JoinSemi
	JoinAnti
)

// Column 描述 logical plan 输出列。
type Column struct {
	Name     string            // Name 是绑定后的输出名。
	Type     types.LogicalType // Type 是强类型 Binder 的结果。
	Nullable bool              // Nullable 表示运行时可能为 NULL。
}

// ScanPushdown 是 optimizer 与 format adapter 之间的 generic pushdown contract。
type ScanPushdown struct {
	Projection []int                     // Projection 是需要读取的输入列序号。
	Filters    []*binder.BoundExpression // Filters 是可安全下推的绑定谓词。
	Limit      uint64                    // Limit 为 0 表示未下推 limit。
}

// ScanSpec 描述 logical scan；不包含 Parquet/S3/SFTP 等具体类型。
type ScanSpec struct {
	RelationID  string                   // RelationID 是 planner 内部 relation identity。
	Function    string                   // Function 是 generic table function 名称。
	Source      string                   // Source 是 canonical source spec。
	Fingerprint stats.DatasetFingerprint // Fingerprint 绑定 Statement SourceSnapshot。
	Pushdown    ScanPushdown             // Pushdown 保存 optimizer 已批准的 generic 下推。
	References  int                      // References 是最终 physical tree 中同一 RelationID 的 scan 数；运行时用于选择直通或共享物化。
}

// AggregateSpec 描述一个 bound aggregate 调用。
type AggregateSpec struct {
	FunctionID uint16                    // FunctionID 与 FunctionCatalog internal ID 对应。
	Arguments  []*binder.BoundExpression // Arguments 是 aggregate 输入表达式。
	Distinct   bool                      // Distinct 表示 aggregate DISTINCT。
	Output     Column                    // Output 是绑定后的输出列。
}

// SortKey 描述逻辑排序键。
type SortKey struct {
	Expr       *binder.BoundExpression // Expr 是已绑定排序表达式。
	Descending bool                    // Descending 表示 DESC。
	NullsFirst *bool                   // NullsFirst 为 nil 时保留 SQL 默认。
}

// LogicalPlan 是 planner/optimizer 之间的 immutable-by-convention tree IR。
type LogicalPlan struct {
	Kind       LogicalKind               // Kind 是逻辑算子类别。
	Children   []*LogicalPlan            // Children 按算子语义顺序排列。
	Output     []Column                  // Output 是该节点输出 schema。
	Scan       *ScanSpec                 // Scan 仅对 LogicalScan 有效。
	Predicate  *binder.BoundExpression   // Predicate 用于 Filter/Join condition。
	Projection []*binder.BoundExpression // Projection 用于 Project。
	GroupBy    []*binder.BoundExpression // GroupBy 用于 Aggregate。
	Aggregates []AggregateSpec           // Aggregates 用于 Aggregate。
	Join       JoinType                  // Join 仅对 LogicalJoin 有效。
	Sort       []SortKey                 // Sort 仅对 LogicalSort 有效。
	Limit      uint64                    // Limit 是逻辑 limit；UnlimitedRows 表示 OFFSET-only。
	Offset     uint64                    // Offset 是逻辑 offset。

	RequiredOutput []bool // JOIN output mask; nil means unknown/all, schema indexes stay unchanged.
}

// Validate 验证 plan shape，防止后续 optimizer 在不完整节点上猜测语义。
func (p *LogicalPlan) Validate() error {
	if p == nil {
		return fmt.Errorf("logical plan is nil")
	}
	switch p.Kind {
	case LogicalApply:
		if len(p.Children)!=1||p.Scan==nil||len(p.Projection)==0{return fmt.Errorf("table apply requires input, function and arguments")}
	case LogicalUnionAll:
		if len(p.Children) != 2 {
			return fmt.Errorf("UNION ALL requires two inputs")
		}
		for _, child := range p.Children {
			if !plannerColumnsEqual(child.Output, p.Output) {
				return fmt.Errorf("UNION ALL input schema mismatch")
			}
		}
	case LogicalOneRow:
		if len(p.Children) != 0 || len(p.Output) != 0 || p.Scan != nil {
			return fmt.Errorf("one-row source requires no children/output/scan")
		}
	case LogicalScan:
		if p.Scan == nil || len(p.Children) != 0 {
			return fmt.Errorf("scan requires ScanSpec and no children")
		}
		if p.Scan.RelationID == "" || p.Scan.Function == "" {
			return fmt.Errorf("scan identity/function is empty")
		}
	case LogicalFilter:
		if len(p.Children) != 1 || p.Predicate == nil {
			return fmt.Errorf("filter requires one child and predicate")
		}
	case LogicalProject:
		if len(p.Children) != 1 || len(p.Projection) == 0 {
			return fmt.Errorf("project requires child and projection")
		}
	case LogicalAggregate:
		if len(p.Children) != 1 {
			return fmt.Errorf("aggregate requires one child")
		}
	case LogicalDistinct, LogicalSort, LogicalLimit:
		if len(p.Children) != 1 {
			return fmt.Errorf("unary logical operator requires one child")
		}
	case LogicalJoin:
		if len(p.Children) != 2 {
			return fmt.Errorf("join requires two children")
		}
		if p.Join != JoinCross && p.Predicate == nil {
			return fmt.Errorf("non-cross join requires predicate")
		}
	default:
		return fmt.Errorf("unknown logical kind %d", p.Kind)
	}
	for _, child := range p.Children {
		if err := child.Validate(); err != nil {
			return err
		}
	}
	return nil
}
