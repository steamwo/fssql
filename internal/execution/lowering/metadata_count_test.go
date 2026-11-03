package lowering

import (
	"context"
	"testing"

	"github.com/steamwo/fssql/internal/core/types"
	"github.com/steamwo/fssql/internal/execution/pipeline"
	"github.com/steamwo/fssql/internal/function"
	"github.com/steamwo/fssql/internal/planner"
)

type metadataAggregateSources struct {
	fakeScanSourceFactory
	accepted      bool
	metadataCalls int
}

func (s *metadataAggregateSources) OpenAggregateScan(context.Context, planner.ScanSpec, []planner.AggregateSpec) (pipeline.Source, bool, error) {
	s.metadataCalls++
	return emptySource{}, s.accepted, nil
}

func TestMetadataAggregateUsesPushSourceWithoutBlockingOperator(t *testing.T) {
	sources := &metadataAggregateSources{accepted: true}
	builder, err := NewBuilder(sources, loweringExecutor())
	if err != nil {
		t.Fatal(err)
	}
	plan := &planner.PhysicalPlan{Kind: planner.PhysicalHashAggregate, Children: []*planner.PhysicalPlan{twoColumnScan()}, Output: []planner.Column{{Type: types.LogicalType{ID: types.BigInt}}}, Aggregates: []planner.AggregateSpec{{FunctionID: uint16(function.CountID), Output: planner.Column{Type: types.LogicalType{ID: types.BigInt}}}}}
	root, err := builder.Build(context.Background(), plan, emptySink{})
	if err != nil {
		t.Fatal(err)
	}
	if len(root.Dependencies) != 0 || sources.calls != 0 || sources.metadataCalls != 1 {
		t.Fatalf("dependencies=%d sources=%+v", len(root.Dependencies), sources)
	}
}
