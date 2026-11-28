package session

import (
	"context"
	"fmt"

	"github.com/steamwo/fssql/internal/core/memory"
	"github.com/steamwo/fssql/internal/core/types"
	execjoin "github.com/steamwo/fssql/internal/execution/join"
	"github.com/steamwo/fssql/internal/execution/pipeline"
	"github.com/steamwo/fssql/internal/planner"
	"github.com/steamwo/fssql/internal/query"
	"github.com/steamwo/fssql/internal/sql/binder"
	storagetemp "github.com/steamwo/fssql/internal/storage/temp"
)

// ExternalJoinRuntimeConfig 是 Stable hash join spill 的显式资源配置。
type ExternalJoinRuntimeConfig struct {
	MemoryBudget        int64                   // MemoryBudget 控制 resident hash partitions 的 managed-memory 上限。
	RadixBits           uint8                   // RadixBits 控制 external hash partition 扇出。
	MaxDepth            int                     // MaxDepth 限制 skew repartition 深度。
	MinReductionPercent uint8                   // MinReductionPercent 防止病态 repartition 无限递归。
	SpillBlockBytes     int                     // SpillBlockBytes 是 TemporaryStorageManager page writer 目标大小。
	Compression         storagetemp.Compression // Compression 仅允许 TemporaryStorageManager 已实现编码。
}

// ExternalJoinFactory 把 Planner Stable JOIN 映射到现有 join runtime。
type ExternalJoinFactory struct {
	query   *query.Context             // query 提供 managed memory 与 query-scoped temporary ownership。
	buffers *memory.BufferManager     // buffers 创建 hash/row/spill managed buffers。
	config  ExternalJoinRuntimeConfig // config 是 Statement 已冻结的 runtime resource policy。
}

// NewExternalJoinFactory 创建 production JOIN factory；资源参数禁止从 benchmark/test 隐式继承。
func NewExternalJoinFactory(q *query.Context, buffers *memory.BufferManager, config ExternalJoinRuntimeConfig) (*ExternalJoinFactory, error) {
	if q == nil || q.Memory == nil || q.Temp == nil {
		return nil, fmt.Errorf("session: JOIN requires query memory and temporary storage scopes")
	}
	if buffers == nil {
		return nil, fmt.Errorf("session: JOIN requires BufferManager")
	}
	if config.MemoryBudget < 4096 {
		return nil, fmt.Errorf("session: JOIN memory budget must be at least 4096 bytes")
	}
	if config.RadixBits == 0 || config.RadixBits > 8 {
		return nil, fmt.Errorf("session: JOIN radix bits must be in [1,8]")
	}
	if config.MaxDepth <= 0 || int(config.RadixBits)*config.MaxDepth >= 64 {
		return nil, fmt.Errorf("session: JOIN max depth must fit 64-bit radix hash width")
	}
	if config.MinReductionPercent == 0 || config.MinReductionPercent > 100 {
		return nil, fmt.Errorf("session: JOIN minimum reduction percent must be in [1,100]")
	}
	if config.SpillBlockBytes <= 0 {
		return nil, fmt.Errorf("session: JOIN spill block bytes must be positive")
	}
	if config.Compression != storagetemp.CompressionNone && config.Compression != storagetemp.CompressionRLE {
		return nil, fmt.Errorf("session: unsupported JOIN spill compression %d", config.Compression)
	}
	return &ExternalJoinFactory{query: q, buffers: buffers, config: config}, nil
}

// OpenJoin 实现 lowering.JoinFactory。非 CROSS 只接受 Planner 已证明为纯 equi predicate 的 join keys。
func (f *ExternalJoinFactory) OpenJoin(ctx context.Context, kind planner.JoinType, left, right []planner.Column, predicate *binder.BoundExpression, leftEstimate, rightEstimate planner.CardinalityEstimate) (pipeline.BinaryOperator, error) {
	if f == nil || f.query == nil || f.query.Memory == nil || f.query.Temp == nil || f.buffers == nil {
		return nil, fmt.Errorf("session: JOIN factory is uninitialized")
	}
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	if f.query.Closed() {
		return nil, fmt.Errorf("session: JOIN query context is closed")
	}
	if len(left) == 0 || len(right) == 0 {
		return nil, fmt.Errorf("session: JOIN requires non-empty child schemas")
	}
	leftTypes, err := joinRuntimeTypes(left)
	if err != nil {
		return nil, fmt.Errorf("session: JOIN left schema: %w", err)
	}
	rightTypes, err := joinRuntimeTypes(right)
	if err != nil {
		return nil, fmt.Errorf("session: JOIN right schema: %w", err)
	}
	execType, err := mapJoinType(kind)
	if err != nil {
		return nil, err
	}
	if execType == execjoin.Cross {
		if predicate != nil {
			return nil, fmt.Errorf("session: CROSS JOIN must not carry a predicate")
		}
		core, err := execjoin.New(execjoin.Config{Type: execjoin.Cross, LeftTypes: leftTypes, RightTypes: rightTypes, Scope: f.query.Memory, Buffers: f.buffers})
		if err != nil {
			return nil, err
		}
		return &inMemoryJoinAdapter{core: core}, nil
	}
	keys, err := planner.ExtractHashJoinKeys(predicate, len(left), len(right))
	if err != nil {
		return nil, fmt.Errorf("session: JOIN predicate is not fully hash-executable: %w", err)
	}
	// Cardinality estimates are hints only. External join correctness/resource enforcement must not depend on them.
	leftBytes := estimateJoinBytes(leftEstimate, leftTypes)
	rightBytes := estimateJoinBytes(rightEstimate, rightTypes)
	core, err := execjoin.NewStableExternal(execjoin.ExternalConfig{
		Type:                execType,
		NullsEqual:          keys.NullsEqual,
		LeftTypes:           leftTypes,
		RightTypes:          rightTypes,
		LeftKeys:            append([]int(nil), keys.Left...),
		RightKeys:           append([]int(nil), keys.Right...),
		LeftEstimatedBytes:  leftBytes,
		RightEstimatedBytes: rightBytes,
		MemoryBudget:        f.config.MemoryBudget,
		RadixBits:           f.config.RadixBits,
		MaxDepth:            f.config.MaxDepth,
		MinReductionPercent: f.config.MinReductionPercent,
		SpillBlockBytes:     f.config.SpillBlockBytes,
		Scope:               f.query.Memory,
		Buffers:             f.buffers,
		Temp:                f.query.Temp,
		Compression:         f.config.Compression,
	})
	if err != nil {
		return nil, err
	}
	return &externalJoinAdapter{core: core}, nil
}

func mapJoinType(kind planner.JoinType) (execjoin.Type, error) {
	switch kind {
	case planner.JoinInner:
		return execjoin.Inner, nil
	case planner.JoinLeft:
		return execjoin.Left, nil
	case planner.JoinRight:
		return execjoin.Right, nil
	case planner.JoinFull:
		return execjoin.Full, nil
	case planner.JoinCross:
		return execjoin.Cross, nil
	case planner.JoinSemi:
		return execjoin.Semi, nil
	case planner.JoinAnti:
		return execjoin.Anti, nil
	default:
		return 0, fmt.Errorf("session: unsupported JOIN type %d", kind)
	}
}

func joinRuntimeTypes(columns []planner.Column) ([]types.LogicalType, error) {
	out := make([]types.LogicalType, len(columns))
	for i, column := range columns {
		if column.Type.ID == types.Invalid {
			return nil, fmt.Errorf("column %d has invalid logical type", i)
		}
		out[i] = cloneJoinRuntimeType(column.Type)
	}
	return out, nil
}

func cloneJoinRuntimeType(value types.LogicalType) types.LogicalType {
	out := value
	if value.Child != nil {
		child := cloneJoinRuntimeType(*value.Child)
		out.Child = &child
	}
	if value.Fields != nil {
		out.Fields = make([]types.StructField, len(value.Fields))
		for i, field := range value.Fields {
			out.Fields[i] = types.StructField{Name: field.Name, Type: cloneJoinRuntimeType(field.Type)}
		}
	}
	return out
}

// estimateJoinBytes 只提供 build-side hint；0 表示 unknown。实际内存仍由 MemoryManager reservation 决定。
func estimateJoinBytes(estimate planner.CardinalityEstimate, columns []types.LogicalType) uint64 {
	if estimate.Rows == 0 || estimate.Confidence <= 0 {
		return 0
	}
	rowWidth := uint64(0)
	for _, column := range columns {
		switch column.ID {
		case types.Boolean, types.TinyInt:
			rowWidth += 1
		case types.SmallInt:
			rowWidth += 2
		case types.Integer, types.Float, types.DateType:
			rowWidth += 4
		case types.BigInt, types.Double, types.TimestampType:
			rowWidth += 8
		case types.HugeInt:
			rowWidth += 16
		case types.Decimal:
			if column.Decimal.Width <= 18 {
				rowWidth += 8
			} else {
				rowWidth += 16
			}
		default:
			// Variable/nested values are deliberately not guessed; unknown estimate preserves correctness.
			return 0
		}
	}
	if rowWidth == 0 || estimate.Rows > ^uint64(0)/rowWidth {
		return 0
	}
	return estimate.Rows * rowWidth
}

// externalJoinAdapter 把 join.Side 转成 generic Pipeline BinaryBuildSide。
type externalJoinAdapter struct{ core *execjoin.StableExternalHashJoin }

func (a *externalJoinAdapter) SetRequiredOutputColumns(needed []bool) error {
	return a.core.SetRequiredOutputColumns(needed)
}

func (a *externalJoinAdapter) BuildSide() pipeline.BinaryBuildSide {
	if a.core.BuildSide() == execjoin.LeftSide {
		return pipeline.BinaryBuildLeft
	}
	return pipeline.BinaryBuildRight
}
func (a *externalJoinAdapter) BuildSink() pipeline.Sink                     { return a.core.BuildSink() }
func (a *externalJoinAdapter) ProbeSink(downstream pipeline.Sink) pipeline.Sink { return a.core.ProbeSink(downstream) }
func (a *externalJoinAdapter) Close()                                      { a.core.Close() }

// inMemoryJoinAdapter 包装既有 HashJoin；CROSS 固定以右侧 build、左侧 probe。
type inMemoryJoinAdapter struct{ core *execjoin.HashJoin }

func (a *inMemoryJoinAdapter) SetRequiredOutputColumns(needed []bool) error {
	return a.core.SetRequiredOutputColumns(needed)
}

func (a *inMemoryJoinAdapter) BuildSide() pipeline.BinaryBuildSide              { return pipeline.BinaryBuildRight }
func (a *inMemoryJoinAdapter) BuildSink() pipeline.Sink                          { return a.core.BuildSink() }
func (a *inMemoryJoinAdapter) ProbeSink(downstream pipeline.Sink) pipeline.Sink  { return a.core.ProbeSink(downstream) }
func (a *inMemoryJoinAdapter) Close()                                             { a.core.Close() }
