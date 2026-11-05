package lowering

import (
	"context"
	"errors"
	"testing"

	"github.com/steamwo/fssql/internal/core/vector"
	"github.com/steamwo/fssql/internal/execution/pipeline"
	"github.com/steamwo/fssql/internal/planner"
)

type unionTrackedSource struct {
	err    error
	closes int
}

func (s *unionTrackedSource) Next(context.Context) (*vector.DataChunk, bool, error) {
	return nil, false, s.err
}
func (s *unionTrackedSource) Close() error {
	s.closes++
	if s.closes > 1 {
		return errors.New("source closed twice")
	}
	return nil
}

type unionTrackedFactory struct {
	sources map[string]*unionTrackedSource
}

func (f unionTrackedFactory) Open(_ context.Context, s planner.ScanSpec) (pipeline.Source, error) {
	return f.sources[s.RelationID], nil
}

func TestUnionDependencyFailureClosesUnstartedNestedSourcesOnce(t *testing.T) {
	failure := errors.New("first branch failed")
	sources := map[string]*unionTrackedSource{"fail": {err: failure}, "right-a": {}, "right-b": {}}
	scan := func(id string) *planner.PhysicalPlan { p := physicalScan(); p.Scan.RelationID = id; return p }
	plan := &planner.PhysicalPlan{Kind: planner.PhysicalUnionAll, Children: []*planner.PhysicalPlan{scan("fail"), {Kind: planner.PhysicalUnionAll, Children: []*planner.PhysicalPlan{scan("right-a"), scan("right-b")}}}}
	executor := loweringExecutor()
	defer executor.Scope.Close()
	builder, err := NewBuilder(unionTrackedFactory{sources: sources}, executor)
	if err != nil {
		t.Fatal(err)
	}
	root, err := builder.Build(context.Background(), plan, emptySink{})
	if err != nil {
		t.Fatal(err)
	}
	if err := pipeline.RunDAG(context.Background(), root, 1, nil); !errors.Is(err, failure) {
		t.Fatalf("failure=%v", err)
	}
	for name, source := range sources {
		if source.closes != 1 {
			t.Errorf("%s close count=%d", name, source.closes)
		}
	}
}
