package export

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/steamwo/fssql/internal/core/memory"
	"github.com/steamwo/fssql/internal/core/vector"
)

type stubFormatWriter struct {
	closeErr error
	aborted  bool
}

func (*stubFormatWriter) WriteChunk(context.Context, *vector.DataChunk) error { return nil }
func (*stubFormatWriter) Flush(context.Context) error                        { return nil }
func (w *stubFormatWriter) Close(context.Context) (WriteResult, error) {
	if w.closeErr != nil {
		return WriteResult{}, w.closeErr
	}
	return WriteResult{Bytes: 7, Checksum: "ok"}, nil
}
func (w *stubFormatWriter) Abort(context.Context) error {
	w.aborted = true
	return nil
}

func TestWriterPoolBoundsAndCancellation(t *testing.T) {
	mm := memory.NewManager(1024)
	scope := mm.NewScope()
	pool, err := NewWriterPool(1, 64, scope)
	if err != nil {
		t.Fatal(err)
	}
	first, err := pool.Acquire(context.Background(), func(context.Context) (FormatWriter, error) { return &stubFormatWriter{}, nil })
	if err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Millisecond)
	defer cancel()
	_, err = pool.Acquire(ctx, func(context.Context) (FormatWriter, error) { return &stubFormatWriter{}, nil })
	if !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("expected bounded wait cancellation, got %v", err)
	}
	if pool.Active() != 1 {
		t.Fatalf("active=%d", pool.Active())
	}
	if err := first.Abort(context.Background()); err != nil {
		t.Fatal(err)
	}
	if pool.Active() != 0 || scope.Used() != 0 {
		t.Fatalf("leak active=%d memory=%d", pool.Active(), scope.Used())
	}
}

func TestWriterPoolReservesBeforeFactoryAndRollsBack(t *testing.T) {
	mm := memory.NewManager(32)
	scope := mm.NewScope()
	pool, _ := NewWriterPool(1, 64, scope)
	called := false
	_, err := pool.Acquire(context.Background(), func(context.Context) (FormatWriter, error) {
		called = true
		return &stubFormatWriter{}, nil
	})
	if !errors.Is(err, memory.ErrLimitExceeded) {
		t.Fatalf("expected memory limit, got %v", err)
	}
	if called {
		t.Fatal("factory must not run before reservation succeeds")
	}
	if pool.Active() != 0 || scope.Used() != 0 {
		t.Fatal("reservation failure leaked ownership")
	}
}

func TestWriterPoolFactoryAndCloseFailureReleaseEverything(t *testing.T) {
	mm := memory.NewManager(1024)
	scope := mm.NewScope()
	pool, _ := NewWriterPool(1, 64, scope)
	boom := errors.New("boom")
	_, err := pool.Acquire(context.Background(), func(context.Context) (FormatWriter, error) { return nil, boom })
	if !errors.Is(err, boom) {
		t.Fatalf("expected factory error, got %v", err)
	}
	if scope.Used() != 0 || pool.Active() != 0 {
		t.Fatal("factory failure leaked ownership")
	}
	underlying := &stubFormatWriter{closeErr: boom}
	w, err := pool.Acquire(context.Background(), func(context.Context) (FormatWriter, error) { return underlying, nil })
	if err != nil {
		t.Fatal(err)
	}
	if _, err := w.Close(context.Background()); !errors.Is(err, boom) {
		t.Fatalf("expected close error, got %v", err)
	}
	if !underlying.aborted {
		t.Fatal("close failure must abort incomplete writer")
	}
	if scope.Used() != 0 || pool.Active() != 0 {
		t.Fatal("close failure leaked ownership")
	}
}

func TestPooledWriterTerminalStateRejectsFurtherUse(t *testing.T) {
	mm := memory.NewManager(1024)
	scope := mm.NewScope()
	pool, _ := NewWriterPool(1, 64, scope)
	w, err := pool.Acquire(context.Background(), func(context.Context) (FormatWriter, error) { return &stubFormatWriter{}, nil })
	if err != nil {
		t.Fatal(err)
	}
	if _, err := w.Close(context.Background()); err != nil {
		t.Fatal(err)
	}
	if err := w.WriteChunk(context.Background(), nil); !errors.Is(err, ErrWriterClosed) {
		t.Fatalf("expected write after close rejection, got %v", err)
	}
	if err := w.Flush(context.Background()); !errors.Is(err, ErrWriterClosed) {
		t.Fatalf("expected flush after close rejection, got %v", err)
	}
	if _, err := w.Close(context.Background()); !errors.Is(err, ErrWriterClosed) {
		t.Fatalf("expected double close rejection, got %v", err)
	}
	if err := w.Abort(context.Background()); err != nil {
		t.Fatalf("abort after terminal should be idempotent: %v", err)
	}
	if pool.Active() != 0 || scope.Used() != 0 {
		t.Fatalf("terminal state leaked ownership: active=%d used=%d", pool.Active(), scope.Used())
	}
}

func BenchmarkWriterPoolAcquireRelease(b *testing.B) {
	mm := memory.NewManager(1 << 30)
	scope := mm.NewScope()
	pool, _ := NewWriterPool(8, 64, scope)
	ctx := context.Background()
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		w, err := pool.Acquire(ctx, func(context.Context) (FormatWriter, error) { return &stubFormatWriter{}, nil })
		if err != nil {
			b.Fatal(err)
		}
		if err := w.Abort(ctx); err != nil {
			b.Fatal(err)
		}
	}
}
