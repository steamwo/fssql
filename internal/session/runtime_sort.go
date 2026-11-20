package session

import (
	"context"
	"errors"
	"fmt"
	"math"

	"github.com/steamwo/fssql/internal/core/memory"
	"github.com/steamwo/fssql/internal/core/types"
	"github.com/steamwo/fssql/internal/execution/blocking"
	execsort "github.com/steamwo/fssql/internal/execution/sort"
	"github.com/steamwo/fssql/internal/planner"
	"github.com/steamwo/fssql/internal/query"
	"github.com/steamwo/fssql/internal/sql/binder"
	storagetemp "github.com/steamwo/fssql/internal/storage/temp"
)

type ExternalSortRuntimeConfig struct {
	SampleCompatibility string
	MemoryBudget        int64
	// TopNMemoryBudget 是 TopN 候选集可用的查询级内存上限；<=0 时回退到 MemoryBudget。
	// 固定宽度 TopN 是 N 有界的算子，DuckDB 同样按查询内存而非溢写预算约束它。
	TopNMemoryBudget int64
	SpillBlockBytes  int
	Compression      storagetemp.Compression
}

type ExternalSortFactory struct {
	query   *query.Context
	buffers *memory.BufferManager
	config  ExternalSortRuntimeConfig
}

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
func (f *ExternalSortFactory) OpenSort(ctx context.Context, columns []planner.Column, keys []planner.SortKey, _ planner.CardinalityEstimate) (blocking.Operator, error) {
	return f.openSort(ctx, columns, keys, 0)
}

func (f *ExternalSortFactory) OpenTopNSort(ctx context.Context, columns []planner.Column, keys []planner.SortKey, _ planner.CardinalityEstimate, rows uint64) (blocking.Operator, error) {
	return f.openSort(ctx, columns, keys, rows)
}

func (f *ExternalSortFactory) openSort(ctx context.Context, columns []planner.Column, keys []planner.SortKey, topNRows uint64) (blocking.Operator, error) {
	if f == nil || f.query == nil || f.query.Memory == nil || f.query.Temp == nil || f.buffers == nil {
		return nil, fmt.Errorf("session: external SORT factory is uninitialized")
	}
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	if f.query.Closed() {
		return nil, fmt.Errorf("session: external SORT query context is closed")
	}
	if spec, ok := planner.IsSampleSort(keys); ok {
		if f.config.SampleCompatibility != "" {
			parsed, err := parseSampleSpec(spec)
			if err != nil {
				return nil, err
			}
			if parsed.method == 'r' || parsed.method == 'p' {
				return newDuckDBReservoirOperator(f.query, f.buffers, spec, f.config.SampleCompatibility)
			}
		}
		return newSampleOperator(f.query, f.buffers, spec)
	}
	if key, ok := planner.IsCTESort(keys); ok {
		return newCTEOperator(f, key)
	}
	if spec, ok := planner.IsGlobalWindowSort(keys); ok {
		return newGlobalWindowOperator(f, columns, spec)
	}
	if len(columns) == 0 || len(keys) == 0 {
		return nil, fmt.Errorf("session: SORT requires input columns and sort keys")
	}
	inputTypes, executionKeys, err := prepareSortRuntime(columns, keys)
	if err != nil {
		return nil, err
	}
	topNBudget := f.config.TopNMemoryBudget
	if topNBudget <= 0 {
		topNBudget = f.config.MemoryBudget
	}
	fixedWidth := fixedWidthSortInput(inputTypes)
	// The packed heap needs at least four bytes per candidate before row data.
	if topNRows > 0 && topNRows <= uint64(topNBudget/4) && topNRows <= uint64(math.MaxInt/4) && fixedWidth {
		packed, packedErr := execsort.NewPackedTopN(execsort.PackedTopNConfig{InputTypes: inputTypes, Keys: executionKeys, N: int(topNRows), MemoryBudget: topNBudget, Scope: f.query.Memory, Buffers: f.buffers})
		if packedErr == nil {
			return packed, nil
		}
		if !errors.Is(packedErr, memory.ErrLimitExceeded) {
			return nil, packedErr
		}
		topN, err := execsort.NewTopN(execsort.TopNConfig{InputTypes: inputTypes, Keys: executionKeys, N: int(topNRows), MemoryBudget: topNBudget, Scope: f.query.Memory, Buffers: f.buffers, PreallocateFixed: true})
		if err == nil {
			return topN, nil
		}
		if !errors.Is(err, memory.ErrLimitExceeded) {
			return nil, err
		}
	}
	if topNRows > 0 && topNRows <= uint64(math.MaxInt) && !fixedWidth {
		op, err := execsort.NewSpillableTopN(execsort.Config{InputTypes: inputTypes, Keys: executionKeys, MemoryBudget: f.config.MemoryBudget, SpillBlockBytes: f.config.SpillBlockBytes, Scope: f.query.Memory, Buffers: f.buffers, Temp: f.query.Temp, Compression: f.config.Compression}, int(topNRows))
		if err != nil {
			return nil, err
		}
		return op, nil
	}
	op, err := execsort.New(execsort.Config{InputTypes: inputTypes, Keys: executionKeys, MemoryBudget: f.config.MemoryBudget, SpillBlockBytes: f.config.SpillBlockBytes, Scope: f.query.Memory, Buffers: f.buffers, Temp: f.query.Temp, Compression: f.config.Compression})
	if err != nil {
		return nil, err
	}
	return op, nil
}

func prepareSortRuntime(columns []planner.Column, keys []planner.SortKey) ([]types.LogicalType, []execsort.Key, error) {
	inputTypes := make([]types.LogicalType, len(columns))
	for i, column := range columns {
		if column.Type.ID == types.Invalid {
			return nil, nil, fmt.Errorf("session: SORT input column %d has invalid logical type", i)
		}
		inputTypes[i] = cloneSortLogicalType(column.Type)
	}
	executionKeys := make([]execsort.Key, len(keys))
	for i, key := range keys {
		if key.Expr == nil || key.Expr.Kind != binder.BoundColumn {
			return nil, nil, fmt.Errorf("session: SORT key %d is not a materialized BoundColumn", i)
		}
		index := key.Expr.ColumnIndex
		if index < 0 || index >= len(columns) {
			return nil, nil, fmt.Errorf("session: SORT key %d column index %d out of range", i, index)
		}
		nullsFirst := false
		if key.NullsFirst != nil {
			nullsFirst = *key.NullsFirst
		}
		executionKeys[i] = execsort.Key{Column: index, Desc: key.Descending, NullsFirst: nullsFirst}
	}
	return inputTypes, executionKeys, nil
}
func fixedWidthSortInput(inputTypes []types.LogicalType) bool {
	if len(inputTypes) == 0 {
		return false
	}
	for _, inputType := range inputTypes {
		if _, ok := inputType.FixedSize(); !ok {
			return false
		}
	}
	return true
}
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
