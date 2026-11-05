package lowering

import (
	"context"
	"errors"
	"sync"

	"github.com/steamwo/fssql/internal/core/vector"
	"github.com/steamwo/fssql/internal/execution/pipeline"
	"github.com/steamwo/fssql/internal/planner"
)

// Dependency pipelines stream their rows to one downstream sink in branch
// order. Only the union's root finalizes that sink; no branch buffers results.
func (b *Builder) lowerUnion(ctx context.Context, plan *planner.PhysicalPlan, sink pipeline.Sink) (*pipeline.Pipeline, error) {
	leftSink := &unionBranchSink{downstream: sink}
	left, err := b.buildJoinAware(ctx, plan.Children[0], leftSink)
	if err != nil {
		return nil, err
	}
	protectUnionSources(left, map[*pipeline.Pipeline]bool{})
	right, err := b.buildJoinAware(ctx, plan.Children[1], &unionBranchSink{downstream: sink})
	if err != nil {
		return nil, errors.Join(err, pipeline.AbortUnstarted(left))
	}
	protectUnionSources(right, map[*pipeline.Pipeline]bool{})
	return &pipeline.Pipeline{ID: "union-all", Source: emptyUnionSource{}, Sink: sink, Dependencies: []*pipeline.Pipeline{left, right}, Abort: func() error {
		var err error
		if !leftSink.done {
			err = pipeline.AbortUnstarted(right)
		}
		if closeable, ok := sink.(pipeline.CloseableSink); ok {
			return errors.Join(err, closeable.Close())
		}
		return err
	}}, nil
}

type unionBranchSink struct {
	downstream pipeline.Sink
	done       bool
}

func (s *unionBranchSink) Consume(ctx context.Context, c *vector.DataChunk) error {
	return s.downstream.Consume(ctx, c)
}
func (s *unionBranchSink) Finalize(context.Context) error { s.done = true; return nil }
func (s *unionBranchSink) TakesChunkOwnership() bool {
	return pipeline.SinkTakesChunkOwnership(s.downstream)
}

type emptyUnionSource struct{}

func (emptyUnionSource) Next(context.Context) (*vector.DataChunk, bool, error) {
	return nil, false, nil
}

// An unstarted nested union can be reached both through dependency cleanup and
// a sibling-abort hook. Preserve the Source contract's exactly-once close.
func protectUnionSources(root *pipeline.Pipeline, seen map[*pipeline.Pipeline]bool) {
	if root == nil || seen[root] {
		return
	}
	seen[root] = true
	for _, child := range root.Dependencies {
		protectUnionSources(child, seen)
	}
	if source, ok := root.Source.(pipeline.CloseableSource); ok {
		if _, wrapped := source.(*unionCloseSource); !wrapped {
			root.Source = &unionCloseSource{CloseableSource: source}
		}
	}
}

type unionCloseSource struct {
	pipeline.CloseableSource
	once sync.Once
	err  error
}

func (s *unionCloseSource) Close() error {
	s.once.Do(func() { s.err = s.CloseableSource.Close() })
	return s.err
}
