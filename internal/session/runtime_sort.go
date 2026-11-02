package session

import (
	"context"
	"fmt"

	"github.com/steamwo/fssql/internal/core/memory"
	"github.com/steamwo/fssql/internal/core/types"
	"github.com/steamwo/fssql/internal/execution/blocking"
	execsort "github.com/steamwo/fssql/internal/execution/sort"
	"github.com/steamwo/fssql/internal/planner"
	"github.com/steamwo/fssql/internal/query"
	"github.com/steamwo/fssql/internal/sql/binder"
	storagetemp "github.com/steamwo/fssql/internal/storage/temp"
)

// ExternalSortRuntimeConfig 是 production ORDER BY spill 的显式资源配置。
type ExternalSortRuntimeConfig struct {
	MemoryBudget    int64                   // MemoryBudget 控制 in-memory run builder 的 spill threshold。
	SpillBlockBytes int                     // SpillBlockBytes 是 TemporaryStorageManager page writer 的目标 block 大小。
	Compression     storagetemp.Compression // Compression 只允许 TemporaryStorageManager 已实现的编码。
}

// ExternalSortFactory 把 generic PhysicalSort 映射到现有 ExternalSort。
type ExternalSortFactory struct {
	query   *query.Context            // query 提供 query-scoped MemoryScope/TemporaryStorage ownership。
	buffers *memory.BufferManager     // buffers 创建 managed row/run/spill buffers。
	config  ExternalSortRuntimeConfig // config 是 query 已冻结的 runtime resource policy。
}

// NewExternalSortFactory 创建 production sort factory；资源策略必须显式提供，禁止静默套用 benchmark 参数。
func NewExternalSortFactory(q *query.Context, buffers *memory.BufferManager, config ExternalSortRuntimeConfig) (*ExternalSortFactory, error) {
	if q == nil || q.Memory == nil || q.Temp == nil {
		return nil, fmt.Errorf("session: external SORT requires query memory and temporary storage scopes")
	}
	if buffers == nil {
		return nil, fmt.Errorf("session: external SORT requires BufferManager")
	}
	if config.MemoryBudget <= 0 {
		return nil, fmt.Errorf("session: external SORT memory budget must be positive")
	}
	if config.SpillBlockBytes <= 0 {
		return nil, fmt.Errorf("session: external SORT spill block bytes must be positive")
	}
	if config.Compression != storagetemp.CompressionNone && config.Compression != storagetemp.CompressionRLE {
		return nil, fmt.Errorf("session: unsupported external SORT spill compression %d", config.Compression)
	}
	return &ExternalSortFactory{query: q, buffers: buffers, config: config}, nil
}

// OpenSort 实现 lowering.SortFactory 的结构契约。
// Planner 已把任意 ORDER BY expression materialize 成 child output 列；runtime 只接受 BoundColumn key，避免重新解释 SQL expression。
func (f *ExternalSortFactory) OpenSort(ctx context.Context, columns []planner.Column, keys []planner.SortKey, estimate planner.CardinalityEstimate) (blocking.Operator, error) {
	if f == nil || f.query == nil || f.query.Memory == nil || f.query.Temp == nil || f.buffers == nil {
		return nil, fmt.Errorf("session: external SORT factory is uninitialized")
	}
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	if f.query.Closed() {
		return nil, fmt.Errorf("session: external SORT query context is closed")
	}
	if len(columns) == 0 || len(keys) == 0 {
		return nil, fmt.Errorf("session: SORT requires input columns and sort keys")
	}
	_ = estimate // ExternalSort 当前不使用 cardinality 预分配；MemoryBudget 是唯一 run builder spill 边界。

	inputTypes := make([]types.LogicalType, len(columns))
	for i, column := range columns {
		if column.Type.ID == types.Invalid {
			return nil, fmt.Errorf("session: SORT input column %d has invalid logical type", i)
		}
		inputTypes[i] = cloneSortLogicalType(column.Type)
	}
	executionKeys := make([]execsort.Key, len(keys))
	for i, key := range keys {
		if key.Expr == nil || key.Expr.Kind != binder.BoundColumn {
			return nil, fmt.Errorf("session: SORT key %d is not a materialized BoundColumn", i)
		}
		index := key.Expr.ColumnIndex
		if index < 0 || index >= len(columns) {
			return nil, fmt.Errorf("session: SORT key %d column index %d out of range", i, index)
		}
		// DuckDB Stable 默认 NULLS LAST；Binder 的 nil 表示未显式指定。
		nullsFirst := false
		if key.NullsFirst != nil {
			nullsFirst = *key.NullsFirst
		}
		executionKeys[i] = execsort.Key{Column: index, Desc: key.Descending, NullsFirst: nullsFirst}
	}
	operator, err := execsort.New(execsort.Config{
		InputTypes:      inputTypes,
		Keys:            executionKeys,
		MemoryBudget:    f.config.MemoryBudget,
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

// cloneSortLogicalType 深拷贝 LIST/STRUCT metadata，避免 Planner schema 与 runtime sort 共享可变 metadata。
func cloneSortLogicalType(value types.LogicalType) types.LogicalType {
	out := value
	if value.Child != nil {
		child := cloneSortLogicalType(*value.Child)
		out.Child = &child
	}
	if value.Fields != nil {
		out.Fields = make([]types.StructField, len(value.Fields))
		for i, field := range value.Fields {
			out.Fields[i] = types.StructField{Name: field.Name, Type: cloneSortLogicalType(field.Type)}
		}
	}
	return out
}
