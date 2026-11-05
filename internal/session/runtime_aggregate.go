package session

import (
	"context"
	"fmt"

	"github.com/steamwo/fssql/internal/core/memory"
	"github.com/steamwo/fssql/internal/core/types"
	"github.com/steamwo/fssql/internal/execution/aggregate"
	"github.com/steamwo/fssql/internal/execution/blocking"
	"github.com/steamwo/fssql/internal/function"
	"github.com/steamwo/fssql/internal/planner"
	"github.com/steamwo/fssql/internal/query"
	"github.com/steamwo/fssql/internal/sql/binder"
	storagetemp "github.com/steamwo/fssql/internal/storage/temp"
)

// ExternalAggregateRuntimeConfig 是 production GROUP BY/aggregate spill 的显式资源配置。
type ExternalAggregateRuntimeConfig struct {
	MemoryBudget    int64                   // MemoryBudget 控制 local hash aggregate 触发 spill 的阈值。
	RadixBits       uint8                   // RadixBits 控制每轮 repartition 扇出，Stable baseline 仅允许 1..8。
	MaxDepth        int                     // MaxDepth 限制 skew repartition 深度，且必须落在 64-bit hash 宽度内。
	SpillBlockBytes int                     // SpillBlockBytes 是 TemporaryStorageManager page writer 的目标 block 大小。
	Compression     storagetemp.Compression // Compression 只允许 TemporaryStorageManager 已实现的编码。
}

// ExternalAggregateFactory 把 generic PhysicalHashAggregate 映射到现有 ExternalAggregate。
type ExternalAggregateFactory struct {
	query   *query.Context                 // query 提供 query-scoped MemoryScope/TemporaryStorage ownership。
	buffers *memory.BufferManager          // buffers 创建 managed hash/state/spill buffers。
	config  ExternalAggregateRuntimeConfig // config 是 query 已冻结的 runtime resource policy。
}

// NewExternalAggregateFactory 创建 production aggregate factory；所有 spill 资源策略都必须显式提供。
func NewExternalAggregateFactory(q *query.Context, buffers *memory.BufferManager, config ExternalAggregateRuntimeConfig) (*ExternalAggregateFactory, error) {
	if q == nil || q.Memory == nil || q.Temp == nil {
		return nil, fmt.Errorf("session: external AGGREGATE requires query memory and temporary storage scopes")
	}
	if buffers == nil {
		return nil, fmt.Errorf("session: external AGGREGATE requires BufferManager")
	}
	if config.MemoryBudget <= 0 {
		return nil, fmt.Errorf("session: external AGGREGATE memory budget must be positive")
	}
	if config.RadixBits == 0 || config.RadixBits > 8 {
		return nil, fmt.Errorf("session: external AGGREGATE radix bits must be in [1,8]")
	}
	if config.MaxDepth <= 0 || int(config.RadixBits)*config.MaxDepth >= 64 {
		return nil, fmt.Errorf("session: external AGGREGATE max depth must fit 64-bit radix hash width")
	}
	if config.SpillBlockBytes <= 0 {
		return nil, fmt.Errorf("session: external AGGREGATE spill block bytes must be positive")
	}
	if config.Compression != storagetemp.CompressionNone && config.Compression != storagetemp.CompressionRLE {
		return nil, fmt.Errorf("session: unsupported external AGGREGATE spill compression %d", config.Compression)
	}
	return &ExternalAggregateFactory{query: q, buffers: buffers, config: config}, nil
}

// OpenAggregate 实现 lowering.AggregateFactory。
// Planner 已把 group expression/aggregate argument materialize 成 child BoundColumn；factory 只映射列与函数 ID。
func (f *ExternalAggregateFactory) OpenAggregate(ctx context.Context, columns []planner.Column, groups []*binder.BoundExpression, specs []planner.AggregateSpec, estimate planner.CardinalityEstimate) (blocking.Operator, error) {
	if f == nil || f.query == nil || f.query.Memory == nil || f.query.Temp == nil || f.buffers == nil {
		return nil, fmt.Errorf("session: external AGGREGATE factory is uninitialized")
	}
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	if f.query.Closed() {
		return nil, fmt.Errorf("session: external AGGREGATE query context is closed")
	}
	// COUNT(*) consumes cardinality and needs no input value columns. Group and
	// aggregate argument indexes are validated individually below.
	_ = estimate // 未冻结 estimate->initial managed bytes 规则前，不允许 cardinality estimate 驱动大预分配。

	inputTypes := make([]types.LogicalType, len(columns))
	for i, column := range columns {
		if column.Type.ID == types.Invalid {
			return nil, fmt.Errorf("session: AGGREGATE input column %d has invalid logical type", i)
		}
		inputTypes[i] = cloneAggregateRuntimeType(column.Type)
	}
	groupColumns := make([]int, len(groups))
	for i, group := range groups {
		if group == nil || group.Kind != binder.BoundColumn {
			return nil, fmt.Errorf("session: AGGREGATE group %d is not a materialized BoundColumn", i)
		}
		if group.ColumnIndex < 0 || group.ColumnIndex >= len(columns) {
			return nil, fmt.Errorf("session: AGGREGATE group %d column index %d out of range", i, group.ColumnIndex)
		}
		if !aggregateRuntimeTypeEqual(group.Type, columns[group.ColumnIndex].Type) {
			return nil, fmt.Errorf("session: AGGREGATE group %d type does not match input column", i)
		}
		groupColumns[i] = group.ColumnIndex
	}

	executionSpecs := make([]aggregate.Spec, len(specs))
	for i, spec := range specs {
		if spec.Distinct {
			return nil, fmt.Errorf("session: aggregate DISTINCT is not part of Stable v1 execution")
		}
		mapped, err := mapAggregateSpec(spec, columns)
		if err != nil {
			return nil, fmt.Errorf("session: aggregate %d: %w", i, err)
		}
		executionSpecs[i] = mapped
	}
	bound, err := aggregate.Bind(inputTypes, executionSpecs)
	if err != nil {
		return nil, err
	}
	for i := range bound {
		if !aggregateRuntimeTypeEqual(bound[i].OutputType, specs[i].Output.Type) {
			return nil, fmt.Errorf("session: aggregate %d output type drift between Planner and execution", i)
		}
	}

	operator, err := aggregate.NewExternalAggregate(aggregate.ExternalConfig{
		InputTypes:      inputTypes,
		GroupColumns:    groupColumns,
		Specs:           executionSpecs,
		EstimatedGroups: 0,
		MemoryBudget:    f.config.MemoryBudget,
		RadixBits:       f.config.RadixBits,
		MaxDepth:        f.config.MaxDepth,
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

// mapAggregateSpec 把 Stable FunctionCatalog ID 显式映射到 execution aggregate kind。
func mapAggregateSpec(spec planner.AggregateSpec, columns []planner.Column) (aggregate.Spec, error) {
	id := function.ID(spec.FunctionID)
	if id == function.CountID && len(spec.Arguments) == 0 {
		return aggregate.Spec{Kind: aggregate.CountStar}, nil
	}
	if len(spec.Arguments) != 1 || spec.Arguments[0] == nil || spec.Arguments[0].Kind != binder.BoundColumn {
		return aggregate.Spec{}, fmt.Errorf("function %d requires one materialized input column", id)
	}
	input := spec.Arguments[0].ColumnIndex
	if input < 0 || input >= len(columns) {
		return aggregate.Spec{}, fmt.Errorf("input column index %d out of range", input)
	}
	if !aggregateRuntimeTypeEqual(spec.Arguments[0].Type, columns[input].Type) {
		return aggregate.Spec{}, fmt.Errorf("input type does not match materialized column %d", input)
	}
	kind := aggregate.Count
	switch id {
	case function.CountID:
		kind = aggregate.Count
	case function.SumID:
		kind = aggregate.Sum
	case function.AvgID:
		kind = aggregate.Avg
	case function.MinID:
		kind = aggregate.Min
	case function.MaxID:
		kind = aggregate.Max
	default:
		return aggregate.Spec{}, fmt.Errorf("unsupported Stable aggregate function id %d", id)
	}
	return aggregate.Spec{Kind: kind, Input: input}, nil
}

func cloneAggregateRuntimeType(value types.LogicalType) types.LogicalType {
	out := value
	if value.Child != nil {
		child := cloneAggregateRuntimeType(*value.Child)
		out.Child = &child
	}
	if value.Fields != nil {
		out.Fields = make([]types.StructField, len(value.Fields))
		for i, field := range value.Fields {
			out.Fields[i] = types.StructField{Name: field.Name, Type: cloneAggregateRuntimeType(field.Type)}
		}
	}
	return out
}

func aggregateRuntimeTypeEqual(left, right types.LogicalType) bool {
	if left.ID != right.ID || left.Decimal != right.Decimal {
		return false
	}
	if (left.Child == nil) != (right.Child == nil) {
		return false
	}
	if left.Child != nil && !aggregateRuntimeTypeEqual(*left.Child, *right.Child) {
		return false
	}
	if len(left.Fields) != len(right.Fields) {
		return false
	}
	for i := range left.Fields {
		if left.Fields[i].Name != right.Fields[i].Name || !aggregateRuntimeTypeEqual(left.Fields[i].Type, right.Fields[i].Type) {
			return false
		}
	}
	return true
}
