package pipeline

import (
	"context"
	"errors"
	"sync/atomic"
	"testing"

	"github.com/steamwo/fssql/internal/core/vector"
)

type dependencyAbortSource struct{ err error }

func (s dependencyAbortSource) Next(context.Context) (*vector.DataChunk, bool, error) {
	if s.err != nil {
		return nil, false, s.err
	}
	return nil, false, nil
}

// TestRunDAGDependencyFailureCallsAbort 验证 dependency 失败时未运行 parent 的 ownership cleanup 会执行一次并保留两条错误链。
func TestRunDAGDependencyFailureCallsAbort(t *testing.T) {
	dependencyErr := errors.New("dependency failed")
	abortErr := errors.New("abort cleanup failed")
	var calls atomic.Int64
	dependency := &Pipeline{ID: "dependency", Source: dependencyAbortSource{err: dependencyErr}, Sink: &sumSink{}}
	root := &Pipeline{
		ID: "root", Source: dependencyAbortSource{}, Sink: &sumSink{}, Dependencies: []*Pipeline{dependency},
		Abort: func() error {
			calls.Add(1)
			return abortErr
		},
	}
	err := RunDAG(context.Background(), root, 1, nil)
	if !errors.Is(err, dependencyErr) || !errors.Is(err, abortErr) {
		t.Fatalf("err=%v", err)
	}
	if calls.Load() != 1 {
		t.Fatalf("abort calls=%d want=1", calls.Load())
	}
}

// TestRunDAGSuccessDoesNotCallAbort 验证正常 DAG 不会触发 dependency-abort cleanup。
func TestRunDAGSuccessDoesNotCallAbort(t *testing.T) {
	var calls atomic.Int64
	dependency := &Pipeline{ID: "dependency", Source: dependencyAbortSource{}, Sink: &sumSink{}}
	root := &Pipeline{
		ID: "root", Source: dependencyAbortSource{}, Sink: &sumSink{}, Dependencies: []*Pipeline{dependency},
		Abort: func() error {
			calls.Add(1)
			return nil
		},
	}
	if err := RunDAG(context.Background(), root, 1, nil); err != nil {
		t.Fatal(err)
	}
	if calls.Load() != 0 {
		t.Fatalf("abort calls=%d want=0", calls.Load())
	}
}

type trackedDependencySource struct {
	closes, reads int
	err           error
}

func (s *trackedDependencySource) Next(context.Context) (*vector.DataChunk, bool, error) {
	s.reads++
	return nil, false, s.err
}
func (s *trackedDependencySource) Close() error { s.closes++; return nil }

func TestRunDAGFailureClosesUnstartedSiblingsOnce(t *testing.T) {
	want := errors.New("failed branch")
	completedSource := &trackedDependencySource{}
	completed := &Pipeline{ID: "completed", Source: completedSource, Sink: &sumSink{}}
	failedSource := &trackedDependencySource{err: want}
	failed := &Pipeline{ID: "failed", Source: failedSource, Sink: &sumSink{}}
	unstartedSource := &trackedDependencySource{}
	unstarted := &Pipeline{ID: "unstarted", Source: unstartedSource, Sink: &sumSink{}, Dependencies: []*Pipeline{completed}}
	rootSource := &trackedDependencySource{}
	var aborts atomic.Int32
	root := &Pipeline{ID: "root", Source: rootSource, Sink: &sumSink{}, Dependencies: []*Pipeline{completed, failed, unstarted}, Abort: func() error { aborts.Add(1); return nil }}
	if err := RunDAG(context.Background(), root, 1, nil); !errors.Is(err, want) {
		t.Fatalf("error=%v", err)
	}
	if completedSource.closes != 1 || failedSource.closes != 1 || unstartedSource.closes != 1 || rootSource.closes != 1 || aborts.Load() != 1 {
		t.Fatalf("close counts=%d,%d,%d,%d aborts=%d", completedSource.closes, failedSource.closes, unstartedSource.closes, rootSource.closes, aborts.Load())
	}
	if unstartedSource.reads != 0 || rootSource.reads != 0 {
		t.Fatal("failed dependency allowed later execution")
	}
}
