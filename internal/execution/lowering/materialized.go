package lowering

import (
	"context"

	"github.com/steamwo/fssql/internal/core/vector"
	"github.com/steamwo/fssql/internal/execution/blocking"
	"github.com/steamwo/fssql/internal/execution/pipeline"
)

// A reusable materializer must gate the entire child DAG, not just discard its
// rows: otherwise volatile projections would still be evaluated a second time.
func materializerSink(operator blocking.Operator, sink pipeline.Sink) pipeline.Sink {
	if _, ok := operator.(interface{ ReuseReady() bool }); ok {
		return &materializerInputSink{sink: sink}
	}
	return sink
}
func materializerDependency(operator blocking.Operator, bridge *blocking.Bridge, child *pipeline.Pipeline) *pipeline.Pipeline {
	if cache, ok := operator.(interface{ ReuseReady() bool }); ok {
		return &pipeline.Pipeline{ID: child.ID + ":materialize", Source: emptyUnionSource{}, Sink: bridge, Dependencies: []*pipeline.Pipeline{child}, SkipDependencies: cache.ReuseReady, Abort: bridge.Close}
	}
	return child
}

type materializerInputSink struct{ sink pipeline.Sink }

func (s *materializerInputSink) Consume(ctx context.Context, chunk *vector.DataChunk) error {
	return s.sink.Consume(ctx, chunk)
}
func (*materializerInputSink) Finalize(context.Context) error { return nil }
