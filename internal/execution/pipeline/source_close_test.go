package pipeline

import (
	"context"
	"errors"
	"testing"

	"github.com/steamwo/fssql/internal/core/memory"
	"github.com/steamwo/fssql/internal/core/types"
	"github.com/steamwo/fssql/internal/core/vector"
	"github.com/steamwo/fssql/internal/query"
)

// closeTrackingSource 记录 Close 调用，并可模拟读取/关闭错误。
type closeTrackingSource struct {
	scope    *memory.Scope
	buffers  *memory.BufferManager
	emitted  bool
	closed   bool
	closeErr error
}

func (s *closeTrackingSource) Next(context.Context) (*vector.DataChunk, bool, error) {
	if s.emitted {
		return nil, false, nil
	}
	s.emitted = true
	column, err := vector.NewFlat(s.scope, s.buffers, types.LogicalType{ID: types.BigInt}, 1)
	if err != nil {
		return nil, false, err
	}
	column.SetInt64(0, 7)
	chunk, err := vector.NewDataChunk(column)
	if err != nil {
		column.Release()
		return nil, false, err
	}
	return chunk, true, nil
}

func (s *closeTrackingSource) Close() error {
	if s.closed {
		return errors.New("source closed twice")
	}
	s.closed = true
	return s.closeErr
}

// finalizeAfterCloseSink 验证成功路径必须先关闭 source，再 Finalize sink。
type finalizeAfterCloseSink struct {
	source    *closeTrackingSource
	finalized bool
}

func (*finalizeAfterCloseSink) Consume(context.Context, *vector.DataChunk) error { return nil }
func (s *finalizeAfterCloseSink) Finalize(context.Context) error {
	if !s.source.closed {
		return errors.New("sink finalized before source close")
	}
	s.finalized = true
	return nil
}

// failingSink 用于验证 downstream 失败时 source 仍会关闭。
type failingSink struct{ err error }

func (s failingSink) Consume(context.Context, *vector.DataChunk) error { return s.err }
func (failingSink) Finalize(context.Context) error                     { return nil }

func TestCloseableSourceClosesBeforeFinalize(t *testing.T) {
	manager := memory.NewManager(1 << 20)
	q := query.New(context.Background(), manager)
	source := &closeTrackingSource{scope: q.Memory, buffers: memory.NewBufferManager(nil)}
	sink := &finalizeAfterCloseSink{source: source}
	p := &Pipeline{ID: "close-before-finalize", Source: source, Sink: sink}
	if err := p.Run(q.Context(), 1, q); err != nil {
		t.Fatal(err)
	}
	if !source.closed || !sink.finalized {
		t.Fatalf("closed=%v finalized=%v", source.closed, sink.finalized)
	}
	if manager.Used() != 0 {
		t.Fatalf("managed leak=%d", manager.Used())
	}
	if err := q.Close(); err != nil {
		t.Fatal(err)
	}
}

func TestCloseableSourceClosesOnDownstreamFailure(t *testing.T) {
	manager := memory.NewManager(1 << 20)
	q := query.New(context.Background(), manager)
	source := &closeTrackingSource{scope: q.Memory, buffers: memory.NewBufferManager(nil)}
	want := errors.New("sink failed")
	p := &Pipeline{ID: "close-on-error", Source: source, Sink: failingSink{err: want}}
	if err := p.Run(q.Context(), 1, q); !errors.Is(err, want) {
		t.Fatalf("err=%v", err)
	}
	if !source.closed {
		t.Fatal("source was not closed after downstream failure")
	}
	if manager.Used() != 0 {
		t.Fatalf("managed leak=%d", manager.Used())
	}
	if q.ActiveTasks() != 0 {
		t.Fatalf("task leak=%d", q.ActiveTasks())
	}
	if err := q.Close(); err != nil {
		t.Fatal(err)
	}
}

func TestCloseableSourcePreservesRunAndCloseErrors(t *testing.T) {
	manager := memory.NewManager(1 << 20)
	q := query.New(context.Background(), manager)
	runErr := errors.New("sink failed")
	closeErr := errors.New("source close failed")
	source := &closeTrackingSource{scope: q.Memory, buffers: memory.NewBufferManager(nil), closeErr: closeErr}
	p := &Pipeline{ID: "joined-errors", Source: source, Sink: failingSink{err: runErr}}
	err := p.Run(q.Context(), 1, q)
	if !errors.Is(err, runErr) || !errors.Is(err, closeErr) {
		t.Fatalf("joined err=%v", err)
	}
	if manager.Used() != 0 {
		t.Fatalf("managed leak=%d", manager.Used())
	}
	if err := q.Close(); err != nil {
		t.Fatal(err)
	}
}

func TestCloseableSourceCloseErrorStopsFinalize(t *testing.T) {
	manager := memory.NewManager(1 << 20)
	q := query.New(context.Background(), manager)
	closeErr := errors.New("source close failed")
	source := &closeTrackingSource{scope: q.Memory, buffers: memory.NewBufferManager(nil), closeErr: closeErr}
	sink := &finalizeAfterCloseSink{source: source}
	p := &Pipeline{ID: "close-error", Source: source, Sink: sink}
	if err := p.Run(q.Context(), 1, q); !errors.Is(err, closeErr) {
		t.Fatalf("err=%v", err)
	}
	if sink.finalized {
		t.Fatal("sink finalized after source close failure")
	}
	if manager.Used() != 0 {
		t.Fatalf("managed leak=%d", manager.Used())
	}
	if err := q.Close(); err != nil {
		t.Fatal(err)
	}
}
