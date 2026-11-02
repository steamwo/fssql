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
