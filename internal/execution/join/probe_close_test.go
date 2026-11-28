package join

import (
	"context"
	"errors"
	"testing"

	"github.com/steamwo/fssql/internal/core/types"
	"github.com/steamwo/fssql/internal/core/vector"
	"github.com/steamwo/fssql/internal/execution/pipeline"
)

type probeCloseSink struct {
	err    error
	closes int
}

func (*probeCloseSink) Consume(context.Context, *vector.DataChunk) error { return nil }
func (*probeCloseSink) Finalize(context.Context) error                   { return nil }
func (s *probeCloseSink) Close() error                                   { s.closes++; return s.err }

func TestProbeCloseReleasesNestedJoins(t *testing.T) {
	for _, external := range []bool{false, true} {
		name := "in_memory"
		if external {
			name = "external"
		}
		t.Run(name, func(t *testing.T) {
			manager, buffers, _, q := newStableExternalHarness(t)
			defer q.Close()
			types1 := []types.LogicalType{{ID: types.BigInt}}
			downstream, err := New(Config{Type: Cross, LeftTypes: types1, RightTypes: types1, Scope: q.Memory, Buffers: buffers})
			if err != nil {
				t.Fatal(err)
			}
			defer downstream.Close()
			wantErr := errors.New("downstream cleanup failed")
			terminal := &probeCloseSink{err: wantErr}
			transform, err := pipeline.NewTransformSink(nil, downstream.ProbeSink(terminal))
			if err != nil {
				t.Fatal(err)
			}
			var sink pipeline.Sink
			if external {
				upstream, err := NewStableExternal(ExternalConfig{Type: Inner, LeftTypes: types1, RightTypes: types1, LeftKeys: []int{0}, RightKeys: []int{0}, MemoryBudget: 4096, Scope: q.Memory, Buffers: buffers, Temp: q.Temp})
				if err != nil {
					t.Fatal(err)
				}
				defer upstream.Close()
				sink = upstream.ProbeSink(transform)
			} else {
				upstream, err := New(Config{Type: Cross, LeftTypes: types1, RightTypes: types1, Scope: q.Memory, Buffers: buffers})
				if err != nil {
					t.Fatal(err)
				}
				defer upstream.Close()
				sink = upstream.ProbeSink(transform)
			}
			if manager.Used() == 0 {
				t.Fatal("joins did not allocate managed state")
			}
			if err := sink.(pipeline.CloseableSink).Close(); !errors.Is(err, wantErr) {
				t.Fatalf("cleanup error=%v, want downstream error", err)
			}
			if terminal.closes != 1 || manager.Used() != 0 {
				t.Fatalf("downstream closes=%d managed bytes=%d", terminal.closes, manager.Used())
			}
		})
	}
}
