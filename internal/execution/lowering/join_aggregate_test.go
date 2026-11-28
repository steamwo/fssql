package lowering

import (
	"context"
	"testing"

	"github.com/steamwo/fssql/internal/core/types"
	"github.com/steamwo/fssql/internal/execution/pipeline"
	"github.com/steamwo/fssql/internal/function"
	"github.com/steamwo/fssql/internal/planner"
	"github.com/steamwo/fssql/internal/sql/binder"
)

func TestJoinGlobalAggregateRemapsPureProjection(t *testing.T) {
	ts := types.LogicalType{ID: types.BigInt}
	join := physicalEquiJoin()
	project := &planner.PhysicalPlan{Kind: planner.PhysicalProject, Children: []*planner.PhysicalPlan{join}, Output: []planner.Column{join.Output[1]}, Projection: []*binder.BoundExpression{{Kind: binder.BoundColumn, Type: ts, ColumnIndex: 1}}}
	plan := &planner.PhysicalPlan{Kind: planner.PhysicalHashAggregate, Children: []*planner.PhysicalPlan{project}, Output: []planner.Column{{Name: "sum", Type: types.LogicalType{ID: types.HugeInt}}}, Aggregates: []planner.AggregateSpec{{FunctionID: uint16(function.SumID), Arguments: []*binder.BoundExpression{{Kind: binder.BoundColumn, Type: ts, ColumnIndex: 0}}, Output: planner.Column{Type: types.LogicalType{ID: types.HugeInt}}}}}
	runtime := &binaryTestRuntime{buildSide: pipeline.BinaryBuildRight}
	factory := &aggregateTestFactory{operator: &distinctTestOperator{}}
	builder, _ := NewBuilder(&fakeScanSourceFactory{src: emptySource{}}, loweringExecutor())
	builder.Joins = &joinTestFactory{runtime: runtime}
	builder.Aggregates = factory
	root, err := builder.Build(context.Background(), plan, emptySink{})
	if err != nil {
		t.Fatal(err)
	}
	defer root.Source.(pipeline.CloseableSource).Close()
	if len(factory.columns) != 2 || factory.aggregates[0].Arguments[0].ColumnIndex != 1 {
		t.Fatalf("factory schema=%v arguments=%v", factory.columns, factory.aggregates)
	}
	if _, ok := runtime.probeDown.(pipeline.JoinBatchConsumer); !ok {
		t.Fatalf("join consumer %T hides match batches", runtime.probeDown)
	}
	if plan.Children[0] != project || plan.Aggregates[0].Arguments[0].ColumnIndex != 0 {
		t.Fatal("lowering mutated shared plan")
	}
}
