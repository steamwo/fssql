package session

import (
	"context"
	"fmt"
	"strconv"
	"strings"

	"github.com/steamwo/fssql/internal/core/types"
	"github.com/steamwo/fssql/internal/core/vector"
	"github.com/steamwo/fssql/internal/execution/aggregate"
	"github.com/steamwo/fssql/internal/execution/blocking"
	"github.com/steamwo/fssql/internal/planner"
	storagetemp "github.com/steamwo/fssql/internal/storage/temp"
)

// phase5GlobalWindowOperator consumes its upstream exactly once, computes all
// OVER () aggregates from that stream, and replays the same materialized rows
// with the aggregate values appended as constants.
type phase5GlobalWindowOperator struct {
	factory *ExternalSortFactory
	agg     *aggregate.HashAggregate
	store   *storagetemp.SpillablePageStore
	pages   []storagetemp.PageID
}

var _ blocking.Operator = (*phase5GlobalWindowOperator)(nil)

func newPhase5GlobalWindowOperator(f *ExternalSortFactory, columns []planner.Column, raw string) (blocking.Operator, error) {
	if f == nil || f.query == nil || f.buffers == nil {
		return nil, fmt.Errorf("session: invalid phase 5 global window factory")
	}
	inputTypes := make([]types.LogicalType, len(columns))
	for i, column := range columns {
		inputTypes[i] = cloneSortLogicalType(column.Type)
	}
	parts := strings.Split(raw, ";")
	specs := make([]aggregate.Spec, 0, len(parts))
	for _, part := range parts {
		switch {
		case part == "c":
			specs = append(specs, aggregate.Spec{Kind: aggregate.CountStar})
		case strings.HasPrefix(part, "s:"):
			index, err := strconv.Atoi(strings.TrimPrefix(part, "s:"))
			if err != nil || index < 0 || index >= len(columns) {
				return nil, fmt.Errorf("session: invalid phase 5 global SUM input %q", part)
			}
			specs = append(specs, aggregate.Spec{Kind: aggregate.Sum, Input: index})
		default:
			return nil, fmt.Errorf("session: invalid phase 5 global window spec %q", part)
		}
	}
	if len(specs) == 0 {
		return nil, fmt.Errorf("session: empty phase 5 global window spec")
	}
	agg, err := aggregate.NewHashAggregate(f.query.Memory, f.buffers, inputTypes, nil, specs, 1)
	if err != nil {
		return nil, err
	}
	store, err := storagetemp.NewSpillablePageStore(f.query.Temp, f.query.Memory, f.buffers, storagetemp.PageStoreConfig{
		SegmentMaxBytes: 64 << 20,
		Compression:     storagetemp.CompressionNone,
	})
	if err != nil {
		agg.Release()
		return nil, err
	}
	return &phase5GlobalWindowOperator{factory: f, agg: agg, store: store}, nil
}

func (o *phase5GlobalWindowOperator) Consume(ctx context.Context, chunk *vector.DataChunk) error {
	if o == nil || o.agg == nil || o.store == nil || chunk == nil {
		return fmt.Errorf("session: invalid phase 5 global window operator")
	}
	if err := o.agg.Consume(ctx, chunk); err != nil {
		return err
	}
	encoded, err := vector.SerializeChunk(chunk, o.factory.query.Memory, o.factory.buffers)
	if err != nil {
		return err
	}
	defer encoded.Release()
	page, err := o.store.WritePage(ctx, encoded.Bytes())
	if err != nil {
		return err
	}
	o.pages = append(o.pages, page)
	return nil
}

func (o *phase5GlobalWindowOperator) Emit(ctx context.Context, emit func(*vector.DataChunk) error) error {
	if o == nil || o.agg == nil || o.store == nil || emit == nil {
		return fmt.Errorf("session: invalid phase 5 global window emit")
	}
	result, err := o.agg.Finalize(ctx)
	if err != nil {
		return err
	}
	defer result.Release()
	for _, id := range o.pages {
		if err := ctx.Err(); err != nil {
			return err
		}
		page, err := o.store.ReadPage(ctx, id)
		if err != nil {
			return err
		}
		chunk, err := vector.DeserializeChunk(page.Bytes(), o.factory.query.Memory, o.factory.buffers)
		page.Release()
		if err != nil {
			return err
		}
		for _, value := range result.Columns {
			constant, err := vector.NewConstant(o.factory.query.Memory, o.factory.buffers, value.Type, chunk.Cardinality)
			if err != nil {
				chunk.Release()
				return err
			}
			if err := vector.CopyValue(value, 0, constant, 0); err != nil {
				constant.Release()
				chunk.Release()
				return err
			}
			chunk.Columns = append(chunk.Columns, constant)
		}
		if err := emit(chunk); err != nil {
			chunk.Release()
			return err
		}
	}
	return nil
}

func (o *phase5GlobalWindowOperator) Release() {
	if o == nil {
		return
	}
	if o.agg != nil {
		o.agg.Release()
		o.agg = nil
	}
	if o.store != nil {
		_ = o.store.Close()
		o.store = nil
	}
	o.pages = nil
}
