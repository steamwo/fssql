package pipeline

import (
	"context"
	"testing"

	"github.com/steamwo/fssql/internal/core/vector"
)

type reuseGateSource struct{ reads, closes int }

func (s *reuseGateSource) Next(context.Context) (*vector.DataChunk, bool, error) {
	s.reads++
	return nil, false, nil
}
func (s *reuseGateSource) Close() error { s.closes++; return nil }

type reuseGateSink struct{ finalizes int }

func (*reuseGateSink) Consume(context.Context, *vector.DataChunk) error { return nil }
func (s *reuseGateSink) Finalize(context.Context) error                 { s.finalizes++; return nil }

func TestMaterializedDependencyReuseClosesWithoutEvaluation(t *testing.T) {
	for _, ready := range []bool{false, true} {
		input, rootSource := &reuseGateSource{}, &reuseGateSource{}
		inputSink, rootSink := &reuseGateSink{}, &reuseGateSink{}
		child := &Pipeline{ID: "input", Source: input, Sink: inputSink}
		root := &Pipeline{ID: "reuse", Source: rootSource, Sink: rootSink, Dependencies: []*Pipeline{child}, SkipDependencies: func() bool { return ready }}
		if err := RunDAG(context.Background(), root, 1, nil); err != nil {
			t.Fatal(err)
		}
		reads, finalizes := 1, 1
		if ready {
			reads, finalizes = 0, 0
		}
		if input.reads != reads || input.closes != 1 || inputSink.finalizes != finalizes || rootSink.finalizes != 1 {
			t.Fatalf("ready=%v reads=%d closes=%d input-finalizes=%d root-finalizes=%d", ready, input.reads, input.closes, inputSink.finalizes, rootSink.finalizes)
		}
	}
}
