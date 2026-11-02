package lowering

import (
	"context"
	"errors"
	"sync/atomic"
	"testing"

	"github.com/steamwo/fssql/internal/core/types"
	"github.com/steamwo/fssql/internal/execution/blocking"
	"github.com/steamwo/fssql/internal/execution/pipeline"
	"github.com/steamwo/fssql/internal/execution/sqltransform"
	"github.com/steamwo/fssql/internal/function"
	"github.com/steamwo/fssql/internal/planner"
	"github.com/steamwo/fssql/internal/sql/binder"
)

type aggregateTestFactory struct {
	calls      atomic.Int64
	operator   *distinctTestOperator
	err        error
	columns    []planner.Column
	groups     []*binder.BoundExpression
	aggregates []planner.AggregateSpec
	estimate   planner.CardinalityEstimate
}

func (f *aggregateTestFactory) OpenAggregate(_ context.Context, columns []planner.Column, groups []*binder.BoundExpression, aggregates []planner.AggregateSpec, estimate planner.CardinalityEstimate) (blocking.Operator, error) {
	f.calls.Add(1)
	f.columns = append([]planner.Column(nil), columns...)
	f.groups = append([]*binder.BoundExpression(nil), groups...)
	f.aggregates = append([]planner.AggregateSpec(nil), aggregates...)
	f.estimate = estimate
	if f.err != nil {
		return nil, f.err
	}
	return f.operator, nil
}

func physicalAggregatePlan() *planner.PhysicalPlan {
	group := &binder.BoundExpression{Kind: binder.BoundColumn, Type: types.LogicalType{ID: types.BigInt}, ColumnIndex: 0}
	arg := &binder.BoundExpression{Kind: binder.BoundColumn, Type: types.LogicalType{ID: types.BigInt}, Nullable: true, ColumnIndex: 1}
	child := &planner.PhysicalPlan{
		Kind:     planner.PhysicalProject,
		Children: []*planner.PhysicalPlan{twoColumnScan()},
		Output: []planner.Column{
			{Name: "__fssql_group_0", Type: types.LogicalType{ID: types.BigInt}},
			{Name: "__fssql_agg_arg_0", Type: types.LogicalType{ID: types.BigInt}, Nullable: true},
		},
		Projection: []*binder.BoundExpression{
			{Kind: binder.BoundColumn, Type: types.LogicalType{ID: types.BigInt}, ColumnIndex: 0},
			{Kind: binder.BoundColumn, Type: types.LogicalType{ID: types.BigInt}, Nullable: true, ColumnIndex: 1},
		},
	}
	return &planner.PhysicalPlan{
		Kind:     planner.PhysicalHashAggregate,
		Children: []*planner.PhysicalPlan{child},
		Output: []planner.Column{
			{Name: "__fssql_group_0", Type: types.LogicalType{ID: types.BigInt}},
			{Name: "__fssql_aggregate_0", Type: types.LogicalType{ID: types.HugeInt}, Nullable: true},
		},
		GroupBy: []*binder.BoundExpression{group},
		Aggregates: []planner.AggregateSpec{{
			FunctionID: uint16(function.SumID), Arguments: []*binder.BoundExpression{arg},
			Output: planner.Column{Name: "__fssql_aggregate_0", Type: types.LogicalType{ID: types.HugeInt}, Nullable: true},
		}},
		Estimate: planner.CardinalityEstimate{Rows: 5, Confidence: 0.6},
	}
}

// TestBuildAggregateRequiresFactoryBeforeOpeningChild 验证缺少/失败 AggregateFactory 时不会先打开 source。
func TestBuildAggregateRequiresFactoryBeforeOpeningChild(t *testing.T) {
	plan := physicalAggregatePlan()
	sources := &fakeScanSourceFactory{src: emptySource{}}
	builder, err := NewBuilder(sources, loweringExecutor())
	if err != nil {
		t.Fatal(err)
	}
	if _, err := builder.Build(context.Background(), plan, emptySink{}); err == nil {
		t.Fatal("expected missing AggregateFactory failure")
	}
	if sources.calls != 0 {
		t.Fatalf("source opened before AggregateFactory validation: %d", sources.calls)
	}
	want := errors.New("aggregate factory failed")
	builder.Aggregates = &aggregateTestFactory{err: want}
	if _, err := builder.Build(context.Background(), plan, emptySink{}); !errors.Is(err, want) {
		t.Fatalf("err=%v want=%v", err, want)
	}
	if sources.calls != 0 {
		t.Fatalf("source opened after AggregateFactory failure: %d", sources.calls)
	}
}

// TestBuildAggregateHavingProjectDAG 验证 pre-project 在 dependency，HAVING/final-project 在 aggregate root stage。
func TestBuildAggregateHavingProjectDAG(t *testing.T) {
	op := &distinctTestOperator{}
	aggregates := &aggregateTestFactory{operator: op}
	sources := &fakeScanSourceFactory{src: emptySource{}}
	builder, err := NewBuilder(sources, loweringExecutor())
	if err != nil {
		t.Fatal(err)
	}
	builder.Aggregates = aggregates
	agg := physicalAggregatePlan()
	havingPredicate := &binder.BoundExpression{
		Kind: binder.BoundBinary, Type: types.LogicalType{ID: types.Boolean}, Op: ">",
		Children: []*binder.BoundExpression{
			{Kind: binder.BoundColumn, Type: types.LogicalType{ID: types.HugeInt}, Nullable: true, ColumnIndex: 1},
			{Kind: binder.BoundLiteral, Type: types.LogicalType{ID: types.HugeInt}, Literal: "0"},
		},
	}
	having := &planner.PhysicalPlan{Kind: planner.PhysicalFilter, Children: []*planner.PhysicalPlan{agg}, Output: agg.Output, Predicate: havingPredicate}
	final := &planner.PhysicalPlan{
		Kind: planner.PhysicalProject, Children: []*planner.PhysicalPlan{having}, Output: agg.Output,
		Projection: []*binder.BoundExpression{
			{Kind: binder.BoundColumn, Type: types.LogicalType{ID: types.BigInt}, ColumnIndex: 0},
			{Kind: binder.BoundColumn, Type: types.LogicalType{ID: types.HugeInt}, Nullable: true, ColumnIndex: 1},
		},
	}
	root, err := builder.Build(context.Background(), final, emptySink{})
	if err != nil {
		t.Fatal(err)
	}
	bridge, ok := root.Source.(*blocking.Bridge)
	if !ok {
		t.Fatalf("root source=%T want bridge", root.Source)
	}
	if aggregates.calls.Load() != 1 || len(aggregates.groups) != 1 || len(aggregates.aggregates) != 1 || aggregates.groups[0].ColumnIndex != 0 || aggregates.aggregates[0].Arguments[0].ColumnIndex != 1 {
		t.Fatalf("factory groups=%+v aggregates=%+v", aggregates.groups, aggregates.aggregates)
	}
	if len(root.Transforms) != 2 {
		t.Fatalf("root transforms=%d", len(root.Transforms))
	}
	if _, ok := root.Transforms[0].(*sqltransform.Filter); !ok {
		t.Fatalf("root transform[0]=%T want Filter", root.Transforms[0])
	}
	if _, ok := root.Transforms[1].(*sqltransform.Project); !ok {
		t.Fatalf("root transform[1]=%T want Project", root.Transforms[1])
	}
	if len(root.Dependencies) != 1 || root.Dependencies[0].Sink != bridge || len(root.Dependencies[0].Transforms) != 1 {
		t.Fatalf("dependency=%+v", root.Dependencies)
	}
	if _, ok := root.Dependencies[0].Transforms[0].(*sqltransform.Project); !ok {
		t.Fatalf("dependency transform=%T want pre-aggregate Project", root.Dependencies[0].Transforms[0])
	}
	if err := pipeline.RunDAG(context.Background(), root, 1, nil); err != nil {
		t.Fatal(err)
	}
	if op.emitCalls.Load() != 1 || op.releaseCalls.Load() != 1 {
		t.Fatalf("emit=%d release=%d", op.emitCalls.Load(), op.releaseCalls.Load())
	}
}

// TestAggregateDependencyFailureReleasesOperator 验证 pre-aggregate dependency runtime 失败时回收 aggregate state。
func TestAggregateDependencyFailureReleasesOperator(t *testing.T) {
	want := errors.New("scan failed")
	op := &distinctTestOperator{}
	sources := &fakeScanSourceFactory{src: errorSource{err: want}}
	builder, err := NewBuilder(sources, loweringExecutor())
	if err != nil {
		t.Fatal(err)
	}
	builder.Aggregates = &aggregateTestFactory{operator: op}
	root, err := builder.Build(context.Background(), physicalAggregatePlan(), emptySink{})
	if err != nil {
		t.Fatal(err)
	}
	if err := pipeline.RunDAG(context.Background(), root, 1, nil); !errors.Is(err, want) {
		t.Fatalf("err=%v want=%v", err, want)
	}
	if op.emitCalls.Load() != 0 || op.releaseCalls.Load() != 1 {
		t.Fatalf("emit=%d release=%d", op.emitCalls.Load(), op.releaseCalls.Load())
	}
}
