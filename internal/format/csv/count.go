package csv

import (
	"context"
	"fmt"
	"math"
	"sync"

	"github.com/steamwo/fssql/internal/core/types"
	"github.com/steamwo/fssql/internal/core/vector"
)

// CountSource uses the same frozen binding, range windows and CSV lexer, but
// counts complete records without allocating or converting column payloads.
type CountSource struct {
	mu     sync.Mutex
	source *Source
	done   bool
}

func NewCountSource(ctx context.Context, cfg RuntimeSourceConfig) (*CountSource, error) {
	cfg.Scan.CountOnly = true
	cfg.Scan.EnableParallel = false
	cfg.Scan.Limit = 0
	source, err := NewRuntimeSource(ctx, cfg)
	if err != nil {
		return nil, err
	}
	return &CountSource{source: source}, nil
}

func (s *CountSource) Next(ctx context.Context) (*vector.DataChunk, bool, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if err := ctx.Err(); err != nil {
		_ = s.source.Close()
		return nil, false, err
	}
	if s.done {
		return nil, false, nil
	}
	s.done = true
	defer s.source.Close()
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
			return nil, false, fmt.Errorf("csv: COUNT(*) overflow")
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

func (s *CountSource) Close() error {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.done = true
	return s.source.Close()
}

func countCSVRecords(input []byte, lex LexConfig, final bool, maxRows, width int) (rows, consumed int, err error) {
	m, err := NewMachine(lex)
	if err != nil {
		return 0, 0, err
	}
	fields, lastRecordStart := 0, 0
	for i := 0; i < len(input); i++ {
		if n := m.advanceData(input[i:]); n > 0 {
			i += n - 1
			continue
		}
		b := input[i]
		event, e := m.Step(b)
		if e != nil {
			return 0, 0, e
		}
		if event.Has(EventFieldEnd) {
			fields++
		}
		if !event.Has(EventRecordEnd) {
			continue
		}
		if fields != width {
			return 0, 0, fmt.Errorf("csv: row has %d columns, want %d", fields, width)
		}
		fields = 0
		rows++
		consumed = i + 1
		if b == '\r' && i+1 < len(input) && input[i+1] == '\n' {
			consumed++
		}
		lastRecordStart = consumed
		if rows == maxRows {
			return rows, consumed, nil
		}
	}
	if final {
		if err := m.Finish(); err != nil {
			return 0, 0, err
		}
		if lastRecordStart < len(input) {
			if fields+1 != width {
				return 0, 0, fmt.Errorf("csv: final row has %d columns, want %d", fields+1, width)
			}
			rows++
			consumed = len(input)
		}
	}
	return rows, consumed, nil
}
