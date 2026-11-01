package distinct

import (
	"context"
	"errors"

	"github.com/steamwo/fssql/internal/core/memory"
	"github.com/steamwo/fssql/internal/core/types"
	"github.com/steamwo/fssql/internal/core/vector"
	"github.com/steamwo/fssql/internal/execution/aggregate"
	storagetemp "github.com/steamwo/fssql/internal/storage/temp"
)

// Distinct is GROUP BY over the selected key columns with no aggregate states.
// This intentionally reuses the centralized Group/Hash/NULL semantics instead
// of introducing a second equality implementation.
type Distinct struct{ agg *aggregate.HashAggregate }

func New(scope *memory.Scope, bm *memory.BufferManager, inputTypes []types.LogicalType, columns []int, estimated int) (*Distinct, error) {
	if len(columns) == 0 {
		return nil, errors.New("DISTINCT requires at least one column")
	}
	a, err := aggregate.NewHashAggregate(scope, bm, inputTypes, columns, nil, estimated)
	if err != nil {
		return nil, err
	}
	return &Distinct{agg: a}, nil
}
func (d *Distinct) Consume(ctx context.Context, c *vector.DataChunk) error {
	if d == nil || d.agg == nil {
		return errors.New("distinct closed")
	}
	return d.agg.Consume(ctx, c)
}
func (d *Distinct) Emit(ctx context.Context, emit func(*vector.DataChunk) error) error {
	if d == nil || d.agg == nil {
		return errors.New("distinct closed")
	}
	return d.agg.Emit(ctx, emit)
}
func (d *Distinct) Release() {
	if d != nil && d.agg != nil {
		d.agg.Release()
		d.agg = nil
	}
}

type ExternalConfig struct {
	InputTypes      []types.LogicalType
	Columns         []int
	EstimatedRows   int
	MemoryBudget    int64
	RadixBits       uint8
	SpillBlockBytes int
	Scope           *memory.Scope
	Buffers         *memory.BufferManager
	Temp            *storagetemp.QueryScope
	Compression     storagetemp.Compression
}
type ExternalDistinct struct{ agg *aggregate.ExternalAggregate }

func NewExternal(cfg ExternalConfig) (*ExternalDistinct, error) {
	if len(cfg.Columns) == 0 {
		return nil, errors.New("DISTINCT requires at least one column")
	}
	a, err := aggregate.NewExternalAggregate(aggregate.ExternalConfig{InputTypes: cfg.InputTypes, GroupColumns: cfg.Columns, EstimatedGroups: cfg.EstimatedRows, MemoryBudget: cfg.MemoryBudget, RadixBits: cfg.RadixBits, SpillBlockBytes: cfg.SpillBlockBytes, Scope: cfg.Scope, Buffers: cfg.Buffers, Temp: cfg.Temp, Compression: cfg.Compression})
	if err != nil {
		return nil, err
	}
	return &ExternalDistinct{agg: a}, nil
}
func (d *ExternalDistinct) Consume(ctx context.Context, c *vector.DataChunk) error {
	if d == nil || d.agg == nil {
		return errors.New("distinct closed")
	}
	return d.agg.Consume(ctx, c)
}
func (d *ExternalDistinct) Emit(ctx context.Context, emit func(*vector.DataChunk) error) error {
	if d == nil || d.agg == nil {
		return errors.New("distinct closed")
	}
	return d.agg.Emit(ctx, emit)
}
func (d *ExternalDistinct) Stats() aggregate.ExternalStats {
	if d == nil || d.agg == nil {
		return aggregate.ExternalStats{}
	}
	return d.agg.Stats()
}
func (d *ExternalDistinct) Release() {
	if d != nil && d.agg != nil {
		d.agg.Release()
		d.agg = nil
	}
}
