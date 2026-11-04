package pipeline

import (
	"context"
	"fmt"
	"sync"
	"testing"
	"time"

	"github.com/steamwo/fssql/internal/core/memory"
	"github.com/steamwo/fssql/internal/core/vector"
)

type orderDependencySource struct {
	mu    sync.Mutex
	seen  map[int]bool
	ready chan struct{}
}

func (s *orderDependencySource) Next(ctx context.Context) (*vector.DataChunk, bool, error) {
	id, _ := WorkerID(ctx)
	s.mu.Lock()
	s.seen[id] = true
	if len(s.seen) == 4 {
		select {
		case <-s.ready:
		default:
			close(s.ready)
		}
	}
	s.mu.Unlock()
	select {
	case <-s.ready:
		return nil, false, nil
	case <-ctx.Done():
		return nil, false, ctx.Err()
	}
}

type orderedIntSink struct{ next int64 }

func (s *orderedIntSink) Consume(_ context.Context, chunk *vector.DataChunk) error {
	for row := 0; row < chunk.Cardinality; row++ {
		value, ok := chunk.Columns[0].Int64(row)
		if !ok || value != s.next {
			return fmt.Errorf("ordered value=%d want=%d", value, s.next)
		}
		s.next++
	}
	return nil
}
func (*orderedIntSink) Finalize(context.Context) error { return nil }

func TestOrderedStageKeepsDependencyParallel(t *testing.T) {
	manager := memory.NewManager(1 << 20)
	scope := manager.NewScope()
	buffers := memory.NewBufferManager(nil)
	dependencySource := &orderDependencySource{seen: make(map[int]bool), ready: make(chan struct{})}
	dependency := &Pipeline{Source: dependencySource, Sink: &sumSink{}}
	sink := &orderedIntSink{}
	root := &Pipeline{Source: &intSource{end: 10000, scope: scope, bm: buffers}, Sink: sink, PreserveOrder: true, Dependencies: []*Pipeline{dependency}}
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	if err := RunDAG(ctx, root, 4, nil); err != nil {
		t.Fatal(err)
	}
	if len(dependencySource.seen) != 4 || sink.next != 10000 {
		t.Fatalf("dependency workers=%d rows=%d", len(dependencySource.seen), sink.next)
	}
	if err := scope.Close(); err != nil {
		t.Fatal(err)
	}
	if manager.Used() != 0 {
		t.Fatalf("managed leak=%d", manager.Used())
	}
}
