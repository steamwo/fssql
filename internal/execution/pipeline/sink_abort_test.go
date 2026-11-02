package pipeline

import (
	"context"
	"errors"
	"sync/atomic"
	"testing"

	"github.com/steamwo/fssql/internal/core/vector"
)

type abortSource struct {
	nextErr  error
	closeErr error
}

func (s *abortSource) Next(context.Context) (*vector.DataChunk, bool, error) {
	if s.nextErr != nil {
		return nil, false, s.nextErr
	}
	return nil, false, nil
}

func (s *abortSource) Close() error { return s.closeErr }

type closeableTestSink struct {
	finalizeErr error
	closeErr    error
	finalized   atomic.Int64
	closed      atomic.Int64
}

func (*closeableTestSink) Consume(context.Context, *vector.DataChunk) error { return nil }
func (s *closeableTestSink) Finalize(context.Context) error {
	s.finalized.Add(1)
	return s.finalizeErr
}
func (s *closeableTestSink) Close() error {
	s.closed.Add(1)
	return s.closeErr
}

// TestPipelineRunFailureClosesStatefulSinkAndJoinsErrors 验证 worker/source-close 失败都会触发 Sink abort cleanup，且错误链全部保留。
func TestPipelineRunFailureClosesStatefulSinkAndJoinsErrors(t *testing.T) {
	runErr := errors.New("source next failed")
	sourceCloseErr := errors.New("source close failed")
	sinkCloseErr := errors.New("sink close failed")
	sink := &closeableTestSink{closeErr: sinkCloseErr}
	p := &Pipeline{ID: "abort", Source: &abortSource{nextErr: runErr, closeErr: sourceCloseErr}, Sink: sink}
	err := p.Run(context.Background(), 1, nil)
	for _, want := range []error{runErr, sourceCloseErr, sinkCloseErr} {
		if !errors.Is(err, want) {
			t.Fatalf("error %v does not contain %v", err, want)
		}
	}
	if sink.closed.Load() != 1 {
		t.Fatalf("sink close count=%d want=1", sink.closed.Load())
	}
	if sink.finalized.Load() != 0 {
		t.Fatalf("Finalize must not run after execution failure: %d", sink.finalized.Load())
	}
}

// TestPipelineSourceCloseFailureClosesStatefulSink 验证正常 worker 完成但 Source.Close 失败仍会回收 Sink state。
func TestPipelineSourceCloseFailureClosesStatefulSink(t *testing.T) {
	want := errors.New("source close failed")
	sink := &closeableTestSink{}
	p := &Pipeline{ID: "source-close", Source: &abortSource{closeErr: want}, Sink: sink}
	if err := p.Run(context.Background(), 1, nil); !errors.Is(err, want) {
		t.Fatalf("err=%v want=%v", err, want)
	}
	if sink.closed.Load() != 1 || sink.finalized.Load() != 0 {
		t.Fatalf("closed=%d finalized=%d", sink.closed.Load(), sink.finalized.Load())
	}
}

// TestPipelineFinalizeFailureClosesStatefulSink 验证 Finalize 未成功移交 ownership 时立即 abort cleanup。
func TestPipelineFinalizeFailureClosesStatefulSink(t *testing.T) {
	finalizeErr := errors.New("finalize failed")
	closeErr := errors.New("cleanup failed")
	sink := &closeableTestSink{finalizeErr: finalizeErr, closeErr: closeErr}
	p := &Pipeline{ID: "finalize", Source: &abortSource{}, Sink: sink}
	err := p.Run(context.Background(), 1, nil)
	if !errors.Is(err, finalizeErr) || !errors.Is(err, closeErr) {
		t.Fatalf("err=%v", err)
	}
	if sink.finalized.Load() != 1 || sink.closed.Load() != 1 {
		t.Fatalf("finalized=%d closed=%d", sink.finalized.Load(), sink.closed.Load())
	}
}

// TestPipelineSuccessfulFinalizeKeepsStatefulSinkOpen 验证成功 dependency stage 不会提前关闭下一 stage 仍要使用的 bridge。
func TestPipelineSuccessfulFinalizeKeepsStatefulSinkOpen(t *testing.T) {
	sink := &closeableTestSink{}
	p := &Pipeline{ID: "success", Source: &abortSource{}, Sink: sink}
	if err := p.Run(context.Background(), 1, nil); err != nil {
		t.Fatal(err)
	}
	if sink.finalized.Load() != 1 || sink.closed.Load() != 0 {
		t.Fatalf("finalized=%d closed=%d", sink.finalized.Load(), sink.closed.Load())
	}
}
