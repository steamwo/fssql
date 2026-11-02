package session

import (
	"context"
	"fmt"

	"github.com/steamwo/fssql/internal/core/memory"
	"github.com/steamwo/fssql/internal/core/types"
	"github.com/steamwo/fssql/internal/execution/blocking"
	"github.com/steamwo/fssql/internal/execution/distinct"
	"github.com/steamwo/fssql/internal/planner"
	"github.com/steamwo/fssql/internal/query"
	storagetemp "github.com/steamwo/fssql/internal/storage/temp"
)

// ExternalDistinctRuntimeConfig 是 production DISTINCT spill 的显式资源配置。
// 这些值由 engine/session composition 提供；factory 不把 benchmark/test 参数静默当成 production 默认值。
type ExternalDistinctRuntimeConfig struct {
	MemoryBudget    int64                   // MemoryBudget 是 ExternalDistinct local state 的 spill threshold。
	RadixBits       uint8                   // RadixBits 是 external hash partition 位数；Stable baseline 仅允许 1..8。
	SpillBlockBytes int                     // SpillBlockBytes 是 TemporaryStorageManager page writer 的目标 block 大小。
	Compression     storagetemp.Compression // Compression 只允许 TemporaryStorageManager 已实现的编码。
}

// ExternalDistinctFactory 把 generic PhysicalDistinct 映射到现有 ExternalDistinct。
// QueryContext 提供 query-scoped Memory/Temp ownership；factory 不创建第二套 spill 生命周期。
type ExternalDistinctFactory struct {
	query   *query.Context                // query 持有当前查询的 MemoryScope 与 TemporaryStorage QueryScope。
	buffers *memory.BufferManager         // buffers 是 query managed Vector/hash/spill buffer allocator。
	config  ExternalDistinctRuntimeConfig // config 是 query 已冻结的 runtime resource policy。
}

// NewExternalDistinctFactory 创建 production factory；缺少 temp scope 或非法资源策略时立即失败。
func NewExternalDistinctFactory(q *query.Context, buffers *memory.BufferManager, config ExternalDistinctRuntimeConfig) (*ExternalDistinctFactory, error) {
	if q == nil || q.Memory == nil || q.Temp == nil {
		return nil, fmt.Errorf("session: external DISTINCT requires query memory and temporary storage scopes")
	}
	if buffers == nil {
		return nil, fmt.Errorf("session: external DISTINCT requires BufferManager")
	}
	if config.MemoryBudget <= 0 {
		return nil, fmt.Errorf("session: external DISTINCT memory budget must be positive")
	}
	if config.RadixBits == 0 || config.RadixBits > 8 {
		return nil, fmt.Errorf("session: external DISTINCT radix bits must be in [1,8]")
	}
	if config.SpillBlockBytes <= 0 {
		return nil, fmt.Errorf("session: external DISTINCT spill block bytes must be positive")
	}
	if config.Compression != storagetemp.CompressionNone && config.Compression != storagetemp.CompressionRLE {
		return nil, fmt.Errorf("session: unsupported external DISTINCT spill compression %d", config.Compression)
	}
	return &ExternalDistinctFactory{query: q, buffers: buffers, config: config}, nil
}

// OpenDistinct 实现 lowering.DistinctFactory 的结构契约。
// estimate 当前不直接用于初始 hash allocation：ExternalAggregate 会按自身 bounded minimum 建表，再由 MemoryBudget 触发 spill；
// 在没有“estimate -> managed initial bytes”的冻结换算 contract 前，禁止用超大 cardinality estimate 预分配 query memory。
func (f *ExternalDistinctFactory) OpenDistinct(ctx context.Context, columns []planner.Column, estimate planner.CardinalityEstimate) (blocking.Operator, error) {
	if f == nil || f.query == nil || f.query.Memory == nil || f.query.Temp == nil || f.buffers == nil {
		return nil, fmt.Errorf("session: external DISTINCT factory is uninitialized")
	}
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	if f.query.Closed() {
		return nil, fmt.Errorf("session: external DISTINCT query context is closed")
	}
	if len(columns) == 0 {
		return nil, fmt.Errorf("session: DISTINCT requires at least one output column")
	}
	_ = estimate // 仅作为未来 resource-aware sizing hint；当前正确性/预算不能依赖未冻结换算规则。

	inputTypes := make([]types.LogicalType, len(columns))
	groupColumns := make([]int, len(columns))
	for i, column := range columns {
		if column.Type.ID == types.Invalid {
			return nil, fmt.Errorf("session: DISTINCT input column %d has invalid logical type", i)
		}
		inputTypes[i] = cloneDistinctLogicalType(column.Type)
		groupColumns[i] = i
	}
	operator, err := distinct.NewExternal(distinct.ExternalConfig{
		InputTypes:      inputTypes,
		Columns:         groupColumns,
		EstimatedRows:   0, // 让现有 ExternalAggregate 使用自身 bounded minimum，避免 estimate 驱动过度预分配。
		MemoryBudget:    f.config.MemoryBudget,
		RadixBits:       f.config.RadixBits,
		SpillBlockBytes: f.config.SpillBlockBytes,
		Scope:           f.query.Memory,
		Buffers:         f.buffers,
		Temp:            f.query.Temp,
		Compression:     f.config.Compression,
	})
	if err != nil {
		return nil, err
	}
	return operator, nil
}

// cloneDistinctLogicalType 深拷贝 LIST/STRUCT metadata，避免 Planner schema 与 runtime operator 共享可变 slice/pointer。
func cloneDistinctLogicalType(value types.LogicalType) types.LogicalType {
	out := value
	if value.Child != nil {
		child := cloneDistinctLogicalType(*value.Child)
		out.Child = &child
	}
	if value.Fields != nil {
		out.Fields = make([]types.StructField, len(value.Fields))
		for i, field := range value.Fields {
			out.Fields[i] = types.StructField{Name: field.Name, Type: cloneDistinctLogicalType(field.Type)}
		}
	}
	return out
}
