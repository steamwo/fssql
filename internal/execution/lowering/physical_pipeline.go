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

// AggregateScanSourceFactory optionally executes an exact global aggregate
// inside the source. Unsupported shapes retain the ordinary blocking path.
type AggregateScanSourceFactory interface {
	OpenAggregateScan(context.Context, planner.ScanSpec, []planner.AggregateSpec) (pipeline.Source, bool, error)
}

// DistinctFactory 把 generic PhysicalDistinct 映射为真实 blocking DISTINCT operator。
type DistinctFactory interface {
	OpenDistinct(context.Context, []planner.Column, planner.CardinalityEstimate) (blocking.Operator, error)
}

// SortFactory 把 generic PhysicalSort 映射为真实 blocking sort operator。
type SortFactory interface {
	OpenSort(context.Context, []planner.Column, []planner.SortKey, planner.CardinalityEstimate) (blocking.Operator, error)
}

// AggregateFactory 把 generic PhysicalHashAggregate 映射为真实 blocking aggregate operator。
type AggregateFactory interface {
	OpenAggregate(context.Context, []planner.Column, []*binder.BoundExpression, []planner.AggregateSpec, planner.CardinalityEstimate) (blocking.Operator, error)
}

// JoinFactory 把 generic PhysicalHashJoin/PhysicalCrossJoin 映射为 build/probe 双输入 Pipeline operator。
// predicate 仍是 Binder IR；factory 必须显式验证可执行条件，禁止 silently drop residual。
type JoinFactory interface {
	OpenJoin(context.Context, planner.JoinType, []planner.Column, []planner.Column, *binder.BoundExpression, planner.CardinalityEstimate, planner.CardinalityEstimate) (pipeline.BinaryOperator, error)
}

// Builder 把已优化 PhysicalPlan 降低为 Push Pipeline DAG。
type Builder struct {
	Sources       ScanSourceFactory    // Sources 是唯一 runtime scan factory。
	Executor      *expression.Executor // Executor 执行 Binder BoundExpression。
	Distincts     DistinctFactory      // Distincts 创建 query-scoped blocking DISTINCT。
	Sorts         SortFactory          // Sorts 创建 query-scoped blocking SORT。
	Aggregates    AggregateFactory     // Aggregates 创建 query-scoped blocking AGGREGATE。
	Joins         JoinFactory          // Joins 创建 query-scoped build/probe JOIN runtime。
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

// loweredStage 保存一个单输入 pipeline stage 的 Source/Transforms 以及 dependency pipelines。
type loweredStage struct {
	source       pipeline.Source
	transforms   []pipeline.TransformOperator
	relationID   string
	dependencies []*pipeline.Pipeline
	ordered      bool
}

// Build 构造 PhysicalPlan -> Push Pipeline DAG。
// 无 JOIN 的树沿用紧凑 source+transform lowering；含 JOIN 的树使用 sink-driven lowering，保证 join 输出上的 transform 位于正确一侧。
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
	if containsPhysicalJoin(plan) {
		return b.buildJoinAware(ctx, plan, sink)
	}
	stage, err := b.lowerNode(ctx, plan)
	if err != nil {
		return nil, err
	}
	return b.pipelineForStage(stage, sink), nil
}

func containsPhysicalJoin(plan *planner.PhysicalPlan) bool {
	if plan == nil {
		return false
	}
	if plan.Kind == planner.PhysicalHashJoin || plan.Kind == planner.PhysicalCrossJoin || plan.Kind == planner.PhysicalUnionAll {
		return true
	}
	for _, child := range plan.Children {
		if containsPhysicalJoin(child) {
			return true
		}
	}
	return false
}

// pipelineForStage 把已经完整 lowered 的单输入 stage 组装为 Pipeline。
func (b *Builder) pipelineForStage(stage loweredStage, sink pipeline.Sink) *pipeline.Pipeline {
	return &pipeline.Pipeline{
		ID:            "sql:" + stage.relationID,
		Source:        stage.source,
		Transforms:    stage.transforms,
		Sink:          sink,
		Dependencies:  stage.dependencies,
		PreserveOrder: stage.ordered,
	}
}

// lowerNode 处理不含 JOIN 的单输入树；Filter/Project/Limit 保持同 stage，Aggregate/Distinct/Sort 切出 blocking dependency stage。
func (b *Builder) lowerNode(ctx context.Context, plan *planner.PhysicalPlan) (loweredStage, error) {
	if err := ctx.Err(); err != nil {
		return loweredStage{}, err
	}
	switch plan.Kind {
	case planner.PhysicalOneRow:
		return loweredStage{source: &oneRowSource{}, relationID: "one-row"}, nil
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
		stage, err := b.lowerSort(ctx, plan)
		stage.ordered = sortStageOrdered(plan.Sort)
		return stage, err
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
	case planner.PhysicalHashJoin, planner.PhysicalCrossJoin:
		return loweredStage{}, fmt.Errorf("physical JOIN requires join-aware lowering")
	default:
		return loweredStage{}, fmt.Errorf("physical operator %d is not wired to Push Pipeline yet", plan.Kind)
	}
}

// buildJoinAware 以最终 downstream Sink 为锚点递归构造含 JOIN 的 Push DAG。
// JOIN 上方 streaming operator 通过 TransformSink 作用于 join 输出；blocking operator 仍建立 bridge stage。
func (b *Builder) buildJoinAware(ctx context.Context, plan *planner.PhysicalPlan, sink pipeline.Sink) (*pipeline.Pipeline, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	if !containsPhysicalJoin(plan) {
		stage, err := b.lowerNode(ctx, plan)
		if err != nil {
			return nil, err
		}
		return b.pipelineForStage(stage, sink), nil
	}
	switch plan.Kind {
	case planner.PhysicalUnionAll:
		return b.lowerUnion(ctx, plan, sink)
	case planner.PhysicalFilter:
		filter, err := sqltransform.NewFilter(b.Executor, plan.Predicate)
		if err != nil {
			return nil, err
		}
		wrapped, err := pipeline.NewTransformSink([]pipeline.TransformOperator{filter}, sink)
		if err != nil {
			return nil, err
		}
		return b.buildJoinAware(ctx, plan.Children[0], wrapped)
	case planner.PhysicalProject:
		project, err := sqltransform.NewProject(b.Executor, plan.Projection)
		if err != nil {
			return nil, err
		}
		wrapped, err := pipeline.NewTransformSink([]pipeline.TransformOperator{project}, sink)
		if err != nil {
			return nil, err
		}
		return b.buildJoinAware(ctx, plan.Children[0], wrapped)
	case planner.PhysicalLimit:
		limit, err := sqltransform.NewLimit(b.Executor, plan.Offset, plan.Limit, plan.Limit == planner.UnlimitedRows)
		if err != nil {
			return nil, err
		}
		wrapped, err := pipeline.NewTransformSink([]pipeline.TransformOperator{limit}, sink)
		if err != nil {
			return nil, err
		}
		return b.buildJoinAware(ctx, plan.Children[0], wrapped)
	case planner.PhysicalHashAggregate, planner.PhysicalDistinct, planner.PhysicalSort:
		operator, suffix, err := b.openBlockingOperator(ctx, plan)
		if err != nil {
			return nil, err
		}
		bridge, err := b.newBlockingBridge(operator)
		if err != nil {
			return nil, err
		}
		childRoot, err := b.buildJoinAware(ctx, plan.Children[0], materializerSink(operator, bridge))
		if err != nil {
			return nil, errors.Join(err, bridge.Close())
		}
		childRoot = materializerDependency(operator, bridge, childRoot)
		return &pipeline.Pipeline{
			ID:            childRoot.ID + ":" + suffix,
			Source:        bridge,
			Sink:          sink,
			Dependencies:  []*pipeline.Pipeline{childRoot},
			PreserveOrder: plan.Kind == planner.PhysicalSort && sortStageOrdered(plan.Sort),
		}, nil
	case planner.PhysicalHashJoin, planner.PhysicalCrossJoin:
		return b.lowerJoinPipeline(ctx, plan, sink)
	default:
		return nil, fmt.Errorf("physical operator %d is not wired to join-aware Push Pipeline yet", plan.Kind)
	}
}

func (b *Builder) openBlockingOperator(ctx context.Context, plan *planner.PhysicalPlan) (blocking.Operator, string, error) {
	switch plan.Kind {
	case planner.PhysicalHashAggregate:
		if b.Aggregates == nil || len(plan.Children) != 1 {
			return nil, "", fmt.Errorf("physical HASH AGGREGATE requires AggregateFactory and one child")
		}
		op, err := b.Aggregates.OpenAggregate(ctx, append([]planner.Column(nil), plan.Children[0].Output...), append([]*binder.BoundExpression(nil), plan.GroupBy...), append([]planner.AggregateSpec(nil), plan.Aggregates...), plan.Estimate)
		return op, "aggregate", err
	case planner.PhysicalDistinct:
		if b.Distincts == nil || len(plan.Children) != 1 {
			return nil, "", fmt.Errorf("physical DISTINCT requires DistinctFactory and one child")
		}
		op, err := b.Distincts.OpenDistinct(ctx, append([]planner.Column(nil), plan.Children[0].Output...), plan.Estimate)
		return op, "distinct", err
	case planner.PhysicalSort:
		if b.Sorts == nil || len(plan.Children) != 1 || len(plan.Sort) == 0 {
			return nil, "", fmt.Errorf("physical SORT requires SortFactory, one child and at least one key")
		}
		op, err := b.Sorts.OpenSort(ctx, append([]planner.Column(nil), plan.Children[0].Output...), append([]planner.SortKey(nil), plan.Sort...), plan.Estimate)
		return op, "sort", err
	default:
		return nil, "", fmt.Errorf("physical operator %d is not blocking-lowerable", plan.Kind)
	}
}

func (b *Builder) newBlockingBridge(operator blocking.Operator) (*blocking.Bridge, error) {
	if operator == nil {
		return nil, fmt.Errorf("blocking factory returned nil operator")
	}
	queue := b.BlockingQueue
	if queue == 0 {
		queue = 1
	}
	bridge, err := blocking.New(operator, queue)
	if err != nil {
		operator.Release()
		return nil, err
	}
	return bridge, nil
}

// lowerJoinPipeline 构造 build dependency + probe root；factory 在任何 child source 打开前完成语义/资源验证。
func (b *Builder) lowerJoinPipeline(ctx context.Context, plan *planner.PhysicalPlan, downstream pipeline.Sink) (*pipeline.Pipeline, error) {
	if b.Joins == nil {
		return nil, fmt.Errorf("physical JOIN requires JoinFactory")
	}
	if len(plan.Children) != 2 {
		return nil, fmt.Errorf("physical JOIN requires two children")
	}
	left, right := plan.Children[0], plan.Children[1]
	runtime, err := b.Joins.OpenJoin(ctx, plan.Join, append([]planner.Column(nil), left.Output...), append([]planner.Column(nil), right.Output...), plan.Predicate, left.Estimate, right.Estimate)
	if err != nil {
		return nil, err
	}
	if runtime == nil {
		return nil, fmt.Errorf("JoinFactory returned nil runtime")
	}
	// Optional configuration preserves compatibility with existing factories.
	if configurable, ok := runtime.(interface{ SetRequiredOutputColumns([]bool) error }); ok && plan.RequiredOutput != nil {
		if err := configurable.SetRequiredOutputColumns(plan.RequiredOutput); err != nil {
			runtime.Close()
			return nil, err
		}
	}

	buildPlan, probePlan := right, left
	if runtime.BuildSide() == pipeline.BinaryBuildLeft {
		buildPlan, probePlan = left, right
	} else if runtime.BuildSide() != pipeline.BinaryBuildRight {
		runtime.Close()
		return nil, fmt.Errorf("JoinFactory returned invalid build side %d", runtime.BuildSide())
	}
	buildSink := &binaryLifecycleSink{Sink: runtime.BuildSink(), runtime: runtime}
	if buildSink.Sink == nil {
		runtime.Close()
		return nil, fmt.Errorf("JOIN runtime returned nil build sink")
	}
	buildRoot, err := b.buildJoinAware(ctx, buildPlan, buildSink)
	if err != nil {
		runtime.Close()
		return nil, err
	}

	probeRaw := runtime.ProbeSink(downstream)
	if probeRaw == nil {
		return nil, errors.Join(fmt.Errorf("JOIN runtime returned nil probe sink"), pipeline.AbortUnstarted(buildRoot), closeBinaryRuntime(runtime))
	}
	probeSink := &binaryLifecycleSink{Sink: probeRaw, runtime: runtime}
	probeRoot, err := b.buildJoinAware(ctx, probePlan, probeSink)
	if err != nil {
		return nil, errors.Join(err, pipeline.AbortUnstarted(buildRoot), closeBinaryRuntime(runtime))
	}
	// A UNION probe can emit rows in its dependency stages. Finish the build
	// before any probe dependency, rather than only before its root source.
	probeRoot.Dependencies = append([]*pipeline.Pipeline{buildRoot}, probeRoot.Dependencies...)
	probeRoot.Abort = combineAbort(probeRoot.Abort, func() error { return closeBinaryRuntime(runtime) })
	return probeRoot, nil
}

// binaryLifecycleSink 让 Pipeline 自身失败时也能关闭双输入 runtime；成功 Finalize 不额外关闭。
type binaryLifecycleSink struct {
	pipeline.Sink
	runtime pipeline.BinaryOperator
}

func (s *binaryLifecycleSink) Close() error {
	var sinkErr error
	if closeable, ok := s.Sink.(pipeline.CloseableSink); ok {
		sinkErr = closeable.Close()
	}
	return errors.Join(sinkErr, closeBinaryRuntime(s.runtime))
}

func closeBinaryRuntime(runtime pipeline.BinaryOperator) error {
	if runtime != nil {
		runtime.Close()
	}
	return nil
}

func combineAbort(first, second func() error) func() error {
	return func() error {
		var firstErr, secondErr error
		if first != nil {
			firstErr = first()
		}
		if second != nil {
			secondErr = second()
		}
		return errors.Join(firstErr, secondErr)
	}
}

// lowerAggregate 创建 aggregate operator 后复用统一 blocking-stage 生命周期。
func (b *Builder) lowerAggregate(ctx context.Context, plan *planner.PhysicalPlan) (loweredStage, error) {
	child := plan.Children[0]
	if len(plan.GroupBy) == 0 && child.Kind == planner.PhysicalScan && child.Scan != nil {
		if sources, ok := b.Sources.(AggregateScanSourceFactory); ok {
			source, accepted, err := sources.OpenAggregateScan(ctx, *child.Scan, plan.Aggregates)
			if err != nil {
				return loweredStage{}, err
			}
			if accepted {
				if source == nil {
					return loweredStage{}, fmt.Errorf("source aggregate returned nil source")
				}
				return loweredStage{source: source, relationID: child.Scan.RelationID + ":source-aggregate"}, nil
			}
		}
	}
	operator, _, err := b.openBlockingOperator(ctx, plan)
	if err != nil {
		return loweredStage{}, err
	}
	return b.lowerBlockingStage(ctx, plan.Children[0], operator, "aggregate")
}

// lowerDistinct 创建 DISTINCT operator 后复用统一 blocking-stage 生命周期。
func (b *Builder) lowerDistinct(ctx context.Context, plan *planner.PhysicalPlan) (loweredStage, error) {
	operator, _, err := b.openBlockingOperator(ctx, plan)
	if err != nil {
		return loweredStage{}, err
	}
	return b.lowerBlockingStage(ctx, plan.Children[0], operator, "distinct")
}

// lowerSort 创建 sort operator 后复用统一 blocking-stage 生命周期。
func (b *Builder) lowerSort(ctx context.Context, plan *planner.PhysicalPlan) (loweredStage, error) {
	operator, _, err := b.openBlockingOperator(ctx, plan)
	if err != nil {
		return loweredStage{}, err
	}
	return b.lowerBlockingStage(ctx, plan.Children[0], operator, "sort")
}

// lowerBlockingStage 用 bounded bridge 把 child Pipeline 的 Sink ownership 转成下一 stage Source。
func (b *Builder) lowerBlockingStage(ctx context.Context, child *planner.PhysicalPlan, operator blocking.Operator, suffix string) (loweredStage, error) {
	bridge, err := b.newBlockingBridge(operator)
	if err != nil {
		return loweredStage{}, err
	}
	childStage, err := b.lowerNode(ctx, child)
	if err != nil {
		return loweredStage{}, errors.Join(err, bridge.Close())
	}
	childPipeline := b.pipelineForStage(childStage, materializerSink(operator, bridge))
	childPipeline = materializerDependency(operator, bridge, childPipeline)
	return loweredStage{
		source:       bridge,
		relationID:   childStage.relationID + ":" + suffix,
		dependencies: []*pipeline.Pipeline{childPipeline},
	}, nil
}
