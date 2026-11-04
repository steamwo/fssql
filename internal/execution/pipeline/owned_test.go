package pipeline

import (
	"context"
	"sync/atomic"
	"testing"

	"github.com/steamwo/fssql/internal/core/memory"
	"github.com/steamwo/fssql/internal/core/types"
	"github.com/steamwo/fssql/internal/core/vector"
)

type ownershipModeTransform struct{ owned, borrowed atomic.Int32 }

func (t *ownershipModeTransform) Transform(_ context.Context, in *vector.DataChunk) (*vector.DataChunk, error) {
	t.borrowed.Add(1)
	return in, nil
}
func (t *ownershipModeTransform) TransformOwned(_ context.Context, in *vector.DataChunk) (*vector.DataChunk, error) {
	t.owned.Add(1)
	return in, nil
}

func TestOnlyOwnedRunnerCallsOwnedTransform(t *testing.T) {
	m := memory.NewManager(1 << 20)
	scope := m.NewScope()
	bm := memory.NewBufferManager(nil)
	transform := &ownershipModeTransform{}
	p := &Pipeline{Source: &intSource{end: 8, scope: scope, bm: bm}, Sink: &sumSink{}, Transforms: []TransformOperator{transform}}
	if err := p.Run(context.Background(), 1, nil); err != nil {
		t.Fatal(err)
	}
	if transform.owned.Load() == 0 || transform.borrowed.Load() != 0 {
		t.Fatal("runner did not select owned interface")
	}
	v, _ := vector.NewFlat(scope, bm, types.LogicalType{ID: types.BigInt}, 1)
	chunk, _ := vector.NewDataChunk(v)
	sink, _ := NewTransformSink([]TransformOperator{transform}, &sumSink{})
	if err := sink.Consume(context.Background(), chunk); err != nil {
		t.Fatal(err)
	}
	if transform.borrowed.Load() != 1 {
		t.Fatal("borrowed sink selected ownership transfer")
	}
	chunk.Release()
	if err := scope.Close(); err != nil {
		t.Fatal(err)
	}
}
