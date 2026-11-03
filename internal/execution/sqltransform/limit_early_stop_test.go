package sqltransform

import (
	"context"
	"testing"

	"github.com/steamwo/fssql/internal/core/memory"
	"github.com/steamwo/fssql/internal/core/types"
	"github.com/steamwo/fssql/internal/core/vector"
	"github.com/steamwo/fssql/internal/execution/expression"
	"github.com/steamwo/fssql/internal/execution/pipeline"
)

type limitCountingSource struct {
	calls  int
	scope  *memory.Scope
	bm     *memory.BufferManager
	closed bool
}

func (s *limitCountingSource) Next(context.Context) (*vector.DataChunk, bool, error) {
	s.calls++
	v, err := vector.NewFlat(s.scope, s.bm, types.LogicalType{ID: types.BigInt}, 10)
	if err != nil {
		return nil, false, err
	}
	c, err := vector.NewDataChunk(v)
	return c, true, err
}
func (s *limitCountingSource) Close() error { s.closed = true; return nil }

type limitCountingSink struct {
	rows      int
	finalized bool
}

func (s *limitCountingSink) Consume(_ context.Context, c *vector.DataChunk) error {
	s.rows += c.Cardinality
	return nil
}
func (s *limitCountingSink) Finalize(context.Context) error { s.finalized = true; return nil }

func TestPipelineLimitStopsClaimingSourceAndFinalizesNormally(t *testing.T) {
	for _, n := range []uint64{0, 3, 13} {
		m := memory.NewManager(1 << 20)
		scope := m.NewScope()
		bm := memory.NewBufferManager(nil)
		executor := &expression.Executor{Scope: scope, Buffers: bm}
		limit, err := NewLimit(executor, 0, n, false)
		if err != nil {
			t.Fatal(err)
		}
		source := &limitCountingSource{scope: scope, bm: bm}
		sink := &limitCountingSink{}
		p := &pipeline.Pipeline{Source: source, Transforms: []pipeline.TransformOperator{limit}, Sink: sink}
		if err := p.Run(context.Background(), 1, nil); err != nil {
			t.Fatal(err)
		}
		if sink.rows != int(n) || source.calls != int((n+9)/10) || !source.closed || !sink.finalized {
			t.Fatalf("limit=%d source=%+v sink=%+v", n, source, sink)
		}
		if m.Used() != 0 {
			t.Fatalf("managed leak=%d", m.Used())
		}
	}
}
