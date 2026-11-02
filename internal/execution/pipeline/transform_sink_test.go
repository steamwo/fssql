package pipeline

import (
	"context"
	"errors"
	"sync/atomic"
	"testing"

	"github.com/steamwo/fssql/internal/core/memory"
	"github.com/steamwo/fssql/internal/core/types"
	"github.com/steamwo/fssql/internal/core/vector"
)

type replacementTransform struct {
	scope   *memory.Scope
	buffers *memory.BufferManager
	value   int64
	err     error
}

func (t replacementTransform) Transform(context.Context, *vector.DataChunk) (*vector.DataChunk, error) {
	column, err := vector.NewFlat(t.scope, t.buffers, types.LogicalType{ID: types.BigInt}, 1)
	if err != nil {
		return nil, err
	}
	column.SetInt64(0, t.value)
	chunk, err := vector.NewDataChunk(column)
	if err != nil {
		column.Release()
		return nil, err
	}
	return chunk, t.err
}

type transformSinkCollector struct {
	value      int64
	consumed   atomic.Int64
	finalized  atomic.Int64
	closed     atomic.Int64
	consumeErr error
}

func (s *transformSinkCollector) Consume(_ context.Context, chunk *vector.DataChunk) error {
	s.consumed.Add(1)
	if len(chunk.Columns) != 0 && chunk.Cardinality != 0 {
		s.value, _ = chunk.Columns[0].Int64(0)
	}
	return s.consumeErr
}
func (s *transformSinkCollector) Finalize(context.Context) error { s.finalized.Add(1); return nil }
func (s *transformSinkCollector) Close() error                  { s.closed.Add(1); return nil }

// TestTransformSinkReplacementOwnership 验证 replacement 由 adapter 释放，原 input ownership 仍由调用方持有。
func TestTransformSinkReplacementOwnership(t *testing.T) {
	manager := memory.NewManager(1 << 20)
	scope := manager.NewScope()
	buffers := memory.NewBufferManager(nil)
	inputColumn, err := vector.NewFlat(scope, buffers, types.LogicalType{ID: types.BigInt}, 1)
	if err != nil {
		t.Fatal(err)
	}
	inputColumn.SetInt64(0, 7)
	input, _ := vector.NewDataChunk(inputColumn)
	downstream := &transformSinkCollector{}
	sink, err := NewTransformSink([]TransformOperator{replacementTransform{scope: scope, buffers: buffers, value: 9}}, downstream)
	if err != nil {
		t.Fatal(err)
	}
	if err := sink.Consume(context.Background(), input); err != nil {
		t.Fatal(err)
	}
	if downstream.value != 9 || downstream.consumed.Load() != 1 {
		t.Fatalf("value=%d consumed=%d", downstream.value, downstream.consumed.Load())
	}
	// replacement 已释放，但 input 仍在：只应剩 input 的 managed allocation。
	if manager.Used() == 0 {
		t.Fatal("input was released by TransformSink")
	}
	input.Release()
	if manager.Used() != 0 {
		t.Fatalf("managed leak=%d", manager.Used())
	}
	if err := scope.Close(); err != nil {
		t.Fatal(err)
	}
}

// TestTransformSinkErrorReleasesReplacement 验证 transform 返回 replacement+error 时 adapter 回收 replacement，不碰 input。
func TestTransformSinkErrorReleasesReplacement(t *testing.T) {
	manager := memory.NewManager(1 << 20)
	scope := manager.NewScope()
	buffers := memory.NewBufferManager(nil)
	inputColumn, _ := vector.NewFlat(scope, buffers, types.LogicalType{ID: types.BigInt}, 1)
	input, _ := vector.NewDataChunk(inputColumn)
	want := errors.New("transform failed")
	sink, err := NewTransformSink([]TransformOperator{replacementTransform{scope: scope, buffers: buffers, value: 9, err: want}}, &transformSinkCollector{})
	if err != nil {
		t.Fatal(err)
	}
	if err := sink.Consume(context.Background(), input); !errors.Is(err, want) {
		t.Fatalf("err=%v want=%v", err, want)
	}
	if manager.Used() == 0 {
		t.Fatal("input was released by TransformSink")
	}
	input.Release()
	if manager.Used() != 0 {
		t.Fatalf("managed leak=%d", manager.Used())
	}
	if err := scope.Close(); err != nil {
		t.Fatal(err)
	}
}

// TestTransformSinkFinalizeAndClosePassThrough 验证成功终态与 abort cleanup 分别透传。
func TestTransformSinkFinalizeAndClosePassThrough(t *testing.T) {
	downstream := &transformSinkCollector{}
	sink, err := NewTransformSink(nil, downstream)
	if err != nil {
		t.Fatal(err)
	}
	if err := sink.Finalize(context.Background()); err != nil {
		t.Fatal(err)
	}
	if err := sink.Close(); err != nil {
		t.Fatal(err)
	}
	if downstream.finalized.Load() != 1 || downstream.closed.Load() != 1 {
		t.Fatalf("finalized=%d closed=%d", downstream.finalized.Load(), downstream.closed.Load())
	}
}
