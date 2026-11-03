package json

import (
	"context"
	"fmt"
	"math"
	"sync"

	"github.com/steamwo/fssql/internal/core/memory"
	"github.com/steamwo/fssql/internal/core/types"
	"github.com/steamwo/fssql/internal/core/vector"
)

// NDJSONCountSource shares binding, snapshot checks, bounded reads and framing
// with the normal source. COUNT(*) validates records but needs no typed payload.
type NDJSONCountSource struct {
	mu     sync.Mutex
	source *NDJSONSource
	done   bool
}

func NewNDJSONCountSource(ctx context.Context, cfg NDJSONRuntimeSourceConfig) (*NDJSONCountSource, error) {
	cfg.Scan.CountOnly = true
	cfg.Scan.EnableParallel = false
	cfg.Projection = nil
	source, err := NewNDJSONRuntimeSource(ctx, cfg)
	if err != nil {
		return nil, err
	}
	return &NDJSONCountSource{source: source}, nil
}

func (s *NDJSONCountSource) stop() {
	s.source.mu.Lock()
	s.source.jobs = nil
	s.source.mu.Unlock()
}

func (s *NDJSONCountSource) Close() error {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.done = true
	s.stop()
	return nil
}

func (s *NDJSONCountSource) Next(ctx context.Context) (*vector.DataChunk, bool, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if err := ctx.Err(); err != nil {
		s.done = true
		s.stop()
		return nil, false, err
	}
	if s.done {
		return nil, false, nil
	}
	s.done = true
	defer s.stop()
	var total int64
	for {
		chunk, ok, err := s.source.Next(ctx)
		if err != nil {
			return nil, false, err
		}
		if !ok {
			break
		}
		n := int64(chunk.Cardinality)
		chunk.Release()
		if n > math.MaxInt64-total {
			return nil, false, fmt.Errorf("json: NDJSON COUNT(*) overflow")
		}
		total += n
	}
	v, err := vector.NewFlat(s.source.scope, s.source.bm, types.LogicalType{ID: types.BigInt}, 1)
	if err != nil {
		return nil, false, err
	}
	v.SetInt64(0, total)
	chunk, err := vector.NewDataChunk(v)
	if err != nil {
		v.Release()
		return nil, false, err
	}
	return chunk, true, nil
}

// Reuse the checked record-count pass: it validates complete JSON structure,
// duplicate bound object fields and nested schema paths without allocating
// vector payloads, unescaping values or converting numeric fields.
func validateCountRecords(ctx context.Context, records [][]byte, schema []types.StructField, scope *memory.Scope, bm *memory.BufferManager) error {
	plans := make([]*vectorPlan, len(schema))
	for i, field := range schema {
		plan, err := newPlan(field.Type)
		if err != nil {
			return err
		}
		plans[i] = plan
	}
	d, err := newTypedDecoder(ProjectedPathSet{}, scope, bm)
	if err != nil {
		return err
	}
	defer d.close()
	d.recordSeen = make([]bool, len(schema))
	for _, record := range records {
		if err := ctx.Err(); err != nil {
			return err
		}
		if err := d.countRecord(record, schema, plans); err != nil {
			return err
		}
	}
	return nil
}
