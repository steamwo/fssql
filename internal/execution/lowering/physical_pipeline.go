package lowering

import (
	"context"
	"fmt"

	"github.com/steamwo/fssql/internal/execution/expression"
	"github.com/steamwo/fssql/internal/execution/pipeline"
	"github.com/steamwo/fssql/internal/execution/sqltransform"
	"github.com/steamwo/fssql/internal/planner"
)

// ScanSourceFactory 把 generic ScanSpec 打开为 Push Pipeline Source。
// 具体 Parquet/CSV/JSON/VFS 构造必须留在 composition 层，lowering 不依赖格式或协议实现。
type ScanSourceFactory interface {
	Open(context.Context, planner.ScanSpec) (pipeline.Source, error)
}

// Builder 把已优化 PhysicalPlan 降低为 Push Pipeline。
type Builder struct {
	Sources  ScanSourceFactory    // Sources 是唯一 runtime scan factory。
	Executor *expression.Executor // Executor 执行 Binder BoundExpression。
}

// NewBuilder 创建 PhysicalPlan lowering builder。
func NewBuilder(sources ScanSourceFactory, executor *expression.Executor) (*Builder, error) {
	if sources == nil {
		return nil, fmt.Errorf("physical pipeline lowering requires scan source factory")
	}
	if executor == nil || executor.Scope == nil || executor.Buffers == nil {
		return nil, fmt.Errorf("physical pipeline lowering requires managed expression executor")
	}
	return &Builder{Sources: sources, Executor: executor}, nil
}

// Build 接受当前已接通的 Scan/Filter/Project/Limit 链；blocking operator 在各自 lowering 完成前显式失败。
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
	source, transforms, relationID, err := b.lowerNode(ctx, plan)
	if err != nil {
		return nil, err
	}
	return &pipeline.Pipeline{
		ID:         "sql:" + relationID,
		Source:     source,
		Transforms: transforms,
		Sink:       sink,
	}, nil
}

// lowerNode 自底向上构造 source 与 transform 顺序，保证 Filter/Project/Limit 的 SQL 语义顺序与 PhysicalPlan 一致。
func (b *Builder) lowerNode(ctx context.Context, plan *planner.PhysicalPlan) (pipeline.Source, []pipeline.TransformOperator, string, error) {
	if err := ctx.Err(); err != nil {
		return nil, nil, "", err
	}
	switch plan.Kind {
	case planner.PhysicalScan:
		if plan.Scan == nil {
			return nil, nil, "", fmt.Errorf("physical scan has no ScanSpec")
		}
		if plan.Scan.Fingerprint == "" {
			return nil, nil, "", fmt.Errorf("physical scan %q has empty dataset fingerprint", plan.Scan.RelationID)
		}
		source, err := b.Sources.Open(ctx, *plan.Scan)
		if err != nil {
			return nil, nil, "", fmt.Errorf("open scan %q: %w", plan.Scan.RelationID, err)
		}
		if source == nil {
			return nil, nil, "", fmt.Errorf("open scan %q returned nil source", plan.Scan.RelationID)
		}
		return source, nil, plan.Scan.RelationID, nil
	case planner.PhysicalFilter:
		source, transforms, relationID, err := b.lowerNode(ctx, plan.Children[0])
		if err != nil {
			return nil, nil, "", err
		}
		filter, err := sqltransform.NewFilter(b.Executor, plan.Predicate)
		if err != nil {
			return nil, nil, "", err
		}
		return source, append(transforms, filter), relationID, nil
	case planner.PhysicalProject:
		source, transforms, relationID, err := b.lowerNode(ctx, plan.Children[0])
		if err != nil {
			return nil, nil, "", err
		}
		project, err := sqltransform.NewProject(b.Executor, plan.Projection)
		if err != nil {
			return nil, nil, "", err
		}
		return source, append(transforms, project), relationID, nil
	case planner.PhysicalLimit:
		source, transforms, relationID, err := b.lowerNode(ctx, plan.Children[0])
		if err != nil {
			return nil, nil, "", err
		}
		limit, err := sqltransform.NewLimit(b.Executor, plan.Offset, plan.Limit, plan.Limit == planner.UnlimitedRows)
		if err != nil {
			return nil, nil, "", err
		}
		return source, append(transforms, limit), relationID, nil
	default:
		return nil, nil, "", fmt.Errorf("physical operator %d is not wired to Push Pipeline yet", plan.Kind)
	}
}
