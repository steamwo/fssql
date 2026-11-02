package lowering

import (
	"context"
	"errors"
	"fmt"

	"github.com/steamwo/fssql/internal/execution/blocking"
	"github.com/steamwo/fssql/internal/execution/expression"
	"github.com/steamwo/fssql/internal/execution/pipeline"
	"github.com/steamwo/fssql/internal/execution/sqltransform"
	"github.com/steamwo/fssql/internal/planner"
	"github.com/steamwo/fssql/internal/sql/binder"
)

// ScanSourceFactory 把 generic ScanSpec 打开为 Push Pipeline Source。
// 具体 Parquet/CSV/JSON/VFS 构造必须留在 composition 层，lowering 不依赖格式或协议实现。
type ScanSourceFactory interface {
	Open(context.Context, planner.ScanSpec) (pipeline.Source, error)
}

// DistinctFactory 把 generic PhysicalDistinct 映射为真实 blocking DISTINCT operator。
// factory 负责选择 in-memory/external spill 策略；lowering 不依赖 concrete hash/temp 实现。
type DistinctFactory interface {
	OpenDistinct(context.Context, []planner.Column, planner.CardinalityEstimate) (blocking.Operator, error)
}

// SortFactory 把 generic PhysicalSort 映射为真实 blocking sort operator。
// keys 已由 Planner 重写到 child output ColumnIndex；factory 只做执行层 key/config 映射。
type SortFactory interface {
	OpenSort(context.Context, []planner.Column, []planner.SortKey, planner.CardinalityEstimate) (blocking.Operator, error)
}

// AggregateFactory 把 generic PhysicalHashAggregate 映射为真实 blocking aggregate operator。
// group/aggregate arguments 必须已经被 Planner materialize 成 child BoundColumn，factory 不重新执行 SQL expression。
type AggregateFactory interface {
	OpenAggregate(context.Context, []planner.Column, []*binder.BoundExpression, []planner.AggregateSpec, planner.CardinalityEstimate) (blocking.Operator, error)
}

// Builder 把已优化 PhysicalPlan 降低为 Push Pipeline DAG。
type Builder struct {
	Sources       ScanSourceFactory    // Sources 是唯一 runtime scan factory。
	Executor      *expression.Executor // Executor 执行 Binder BoundExpression。
	Distincts     DistinctFactory      // Distincts 为 PhysicalDistinct 创建 query-scoped blocking operator。
	Sorts         SortFactory          // Sorts 为 PhysicalSort 创建 query-scoped blocking operator。
	Aggregates    AggregateFactory     // Aggregates 为 PhysicalHashAggregate 创建 query-scoped blocking operator。
	BlockingQueue int                  // BlockingQueue 限制 blocking stage handoff；0 使用默认容量 1。
}

// NewBuilder 创建 PhysicalPlan lowering builder。
func NewBuilder(sources ScanSourceFactory, executor *expression.Executor) (*Builder, error) {
	if sources == nil {
		return nil, fmt.Errorf("physical pipeline lowering requires scan source factory")
	}
	if executor == nil || executor.Scope == nil || executor.Buffers == nil {
		return nil, fmt.Errorf("physical pipeline lowering requires managed expression executor")
	}
	return &Builder{Sources: sources, Executor: executor, BlockingQueue: 1}, nil
}

// loweredStage 保存一个 pipeline stage 的 Source/Transforms 以及必须先完成的 dependency pipelines。
type loweredStage struct {
	source       pipeline.Source
	transforms   []pipeline.TransformOperator
	relationID   string
	dependencies []*pipeline.Pipeline
}

// Build 接受当前已接通的 Scan/Filter/Project/Aggregate/Distinct/Sort/Limit；其他 blocking operator 在各自 lowering 完成前显式失败。
func (b *Builder) Build(ctx context.Context, plan *planner.PhysicalPlan, sink pipeline.Sink) (*pipeline.Pipeline, error) {
	if b == nil || b.Sources == nil || b.Executor == nil {
		return nil, fmt.Errorf("physical pipeline lowering builder is not initialized")
	}
	if sink == nil {
		return nil, fmt.Errorf("physical pipeline lowering requires sink")
	}
	if plan == nil {
		return nil, fmt.Errorf("physical pipeline lowering requires plan")
	}
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	if err := plan.Validate(); err != nil {
		return nil, err
	}
	stage, err := b.lowerNode(ctx, plan)
	if err != nil {
		return nil, err
	}
	return b.pipelineForStage(stage, sink), nil
}

// pipelineForStage 把已经完整 lowered 的 stage 组装为 Pipeline；该步骤不再打开资源，也不会失败。
func (b *Builder) pipelineForStage(stage loweredStage, sink pipeline.Sink) *pipeline.Pipeline {
	return &pipeline.Pipeline{
		ID:           "sql:" + stage.relationID,
		Source:       stage.source,
		Transforms:   stage.transforms,
		Sink:         sink,
		Dependencies: stage.dependencies,
	}
}

// lowerNode 自底向上构造 stage；Filter/Project/Limit 保持同 stage，Aggregate/Distinct/Sort 切出 dependency stage。
func (b *Builder) lowerNode(ctx context.Context, plan *planner.PhysicalPlan) (loweredStage, error) {
	if err := ctx.Err(); err != nil {
		return loweredStage{}, err
	}
	switch plan.Kind {
	case planner.PhysicalScan:
		if plan.Scan == nil {
			return loweredStage{}, fmt.Errorf("physical scan has no ScanSpec")
		}
		if plan.Scan.Fingerprint == "" {
			return loweredStage{}, fmt.Errorf("physical scan %q has empty dataset fingerprint", plan.Scan.RelationID)
		}
		source, err := b.Sources.Open(ctx, *plan.Scan)
		if err != nil {
			return loweredStage{}, fmt.Errorf("open scan %q: %w", plan.Scan.RelationID, err)
		}
		if source == nil {
			return loweredStage{}, fmt.Errorf("open scan %q returned nil source", plan.Scan.RelationID)
		}
		return loweredStage{source: source, relationID: plan.Scan.RelationID}, nil
	case planner.PhysicalFilter:
		// 先验证/构造 transform，再打开 child source，避免 transform 配置错误留下已打开 source。
		filter, err := sqltransform.NewFilter(b.Executor, plan.Predicate)
		if err != nil {
			return loweredStage{}, err
		}
		stage, err := b.lowerNode(ctx, plan.Children[0])
		if err != nil {
			return loweredStage{}, err
		}
		stage.transforms = append(stage.transforms, filter)
		return stage, nil
	case planner.PhysicalProject:
		project, err := sqltransform.NewProject(b.Executor, plan.Projection)
		if err != nil {
			return loweredStage{}, err
		}
		stage, err := b.lowerNode(ctx, plan.Children[0])
		if err != nil {
			return loweredStage{}, err
		}
		stage.transforms = append(stage.transforms, project)
		return stage, nil
	case planner.PhysicalHashAggregate:
		return b.lowerAggregate(ctx, plan)
	case planner.PhysicalDistinct:
		return b.lowerDistinct(ctx, plan)
	case planner.PhysicalSort:
		return b.lowerSort(ctx, plan)
	case planner.PhysicalLimit:
		limit, err := sqltransform.NewLimit(b.Executor, plan.Offset, plan.Limit, plan.Limit == planner.UnlimitedRows)
		if err != nil {
			return loweredStage{}, err
		}
		stage, err := b.lowerNode(ctx, plan.Children[0])
		if err != nil {
			return loweredStage{}, err
		}
		stage.transforms = append(stage.transforms, limit)
		return stage, nil
	default:
		return loweredStage{}, fmt.Errorf("physical operator %d is not wired to Push Pipeline yet", plan.Kind)
	}
}

// lowerAggregate 创建 aggregate operator 后复用统一 blocking-stage 生命周期。
func (b *Builder) lowerAggregate(ctx context.Context, plan *planner.PhysicalPlan) (loweredStage, error) {
	if b.Aggregates == nil {
		return loweredStage{}, fmt.Errorf("physical HASH AGGREGATE requires AggregateFactory")
	}
	if len(plan.Children) != 1 {
		return loweredStage{}, fmt.Errorf("physical HASH AGGREGATE requires one child")
	}
	input := append([]planner.Column(nil), plan.Children[0].Output...)
	groups := append([]*binder.BoundExpression(nil), plan.GroupBy...)
	aggregates := append([]planner.AggregateSpec(nil), plan.Aggregates...)
	operator, err := b.Aggregates.OpenAggregate(ctx, input, groups, aggregates, plan.Estimate)
	if err != nil {
		return loweredStage{}, err
	}
	return b.lowerBlockingStage(ctx, plan.Children[0], operator, "aggregate")
}

// lowerDistinct 创建 DISTINCT operator 后复用统一 blocking-stage 生命周期。
func (b *Builder) lowerDistinct(ctx context.Context, plan *planner.PhysicalPlan) (loweredStage, error) {
	if b.Distincts == nil {
		return loweredStage{}, fmt.Errorf("physical DISTINCT requires DistinctFactory")
	}
	if len(plan.Children) != 1 {
		return loweredStage{}, fmt.Errorf("physical DISTINCT requires one child")
	}
	input := append([]planner.Column(nil), plan.Children[0].Output...)
	operator, err := b.Distincts.OpenDistinct(ctx, input, plan.Estimate)
	if err != nil {
		return loweredStage{}, err
	}
	return b.lowerBlockingStage(ctx, plan.Children[0], operator, "distinct")
}

// lowerSort 创建 sort operator 后复用统一 blocking-stage 生命周期。
func (b *Builder) lowerSort(ctx context.Context, plan *planner.PhysicalPlan) (loweredStage, error) {
	if b.Sorts == nil {
		return loweredStage{}, fmt.Errorf("physical SORT requires SortFactory")
	}
	if len(plan.Children) != 1 || len(plan.Sort) == 0 {
		return loweredStage{}, fmt.Errorf("physical SORT requires one child and at least one key")
	}
	input := append([]planner.Column(nil), plan.Children[0].Output...)
	keys := append([]planner.SortKey(nil), plan.Sort...)
	operator, err := b.Sorts.OpenSort(ctx, input, keys, plan.Estimate)
	if err != nil {
		return loweredStage{}, err
	}
	return b.lowerBlockingStage(ctx, plan.Children[0], operator, "sort")
}

// lowerBlockingStage 用 bounded bridge 把 child Pipeline 的 Sink ownership 转成下一 stage Source。
// operator 已由 factory 创建；任何后续构造失败都必须在返回前 Release。
func (b *Builder) lowerBlockingStage(ctx context.Context, child *planner.PhysicalPlan, operator blocking.Operator, suffix string) (loweredStage, error) {
	if operator == nil {
		return loweredStage{}, fmt.Errorf("blocking %s factory returned nil operator", suffix)
	}
	queue := b.BlockingQueue
	if queue == 0 {
		queue = 1
	}
	bridge, err := blocking.New(operator, queue)
	if err != nil {
		operator.Release()
		return loweredStage{}, err
	}
	childStage, err := b.lowerNode(ctx, child)
	if err != nil {
		return loweredStage{}, errors.Join(err, bridge.Close())
	}
	childPipeline := b.pipelineForStage(childStage, bridge)
	return loweredStage{
		source:       bridge,
		relationID:   childStage.relationID + ":" + suffix,
		dependencies: []*pipeline.Pipeline{childPipeline},
	}, nil
}
