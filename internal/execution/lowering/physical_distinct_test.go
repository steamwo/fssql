package lowering

import (
	"context"
	"errors"
	"sync/atomic"
	"testing"

	"github.com/steamwo/fssql/internal/core/types"
	"github.com/steamwo/fssql/internal/core/vector"
	"github.com/steamwo/fssql/internal/execution/blocking"
	"github.com/steamwo/fssql/internal/execution/pipeline"
	"github.com/steamwo/fssql/internal/execution/sqltransform"
	"github.com/steamwo/fssql/internal/planner"
	"github.com/steamwo/fssql/internal/sql/binder"
)

type distinctTestOperator struct {
	consumeCalls atomic.Int64
	emitCalls    atomic.Int64
	releaseCalls atomic.Int64
	emitErr      error
}

func (o *distinctTestOperator) Consume(context.Context, *vector.DataChunk) error {
	o.consumeCalls.Add(1)
	return nil
}
func (o *distinctTestOperator) Emit(context.Context, func(*vector.DataChunk) error) error {
	o.emitCalls.Add(1)
	return o.emitErr
}
func (o *distinctTestOperator) Release() { o.releaseCalls.Add(1) }

type distinctTestFactory struct {
	calls    atomic.Int64
	operator *distinctTestOperator
	err      error
	columns  []planner.Column
	estimate planner.CardinalityEstimate
}

func (f *distinctTestFactory) OpenDistinct(_ context.Context, columns []planner.Column, estimate planner.CardinalityEstimate) (blocking.Operator, error) {
	f.calls.Add(1)
	f.columns = append([]planner.Column(nil), columns...)
	f.estimate = estimate
	if f.err != nil {
		return nil, f.err
	}
	return f.operator, nil
}

type errorSource struct{ err error }

func (s errorSource) Next(context.Context) (*vector.DataChunk, bool, error) { return nil, false, s.err }

func distinctInputColumn() planner.Column {
	return planner.Column{Name: "id", Type: types.LogicalType{ID: types.BigInt}, Nullable: true}
}

func physicalProjectOverScan() *planner.PhysicalPlan {
	column := &binder.BoundExpression{Kind: binder.BoundColumn, Type: types.LogicalType{ID: types.BigInt}, Nullable: true, ColumnIndex: 0}
	scan := physicalScan()
	scan.Output = []planner.Column{distinctInputColumn()}
	return &planner.PhysicalPlan{
		Kind:       planner.PhysicalProject,
		Children:   []*planner.PhysicalPlan{scan},
		Output:     []planner.Column{distinctInputColumn()},
		Projection: []*binder.BoundExpression{column},
	}
}

// TestBuildDistinctRequiresFactoryBeforeOpeningChild 验证缺少/失败 factory 时不会先打开 scan source。
func TestBuildDistinctRequiresFactoryBeforeOpeningChild(t *testing.T) {
	plan := &planner.PhysicalPlan{Kind: planner.PhysicalDistinct, Children: []*planner.PhysicalPlan{physicalProjectOverScan()}, Output: []planner.Column{distinctInputColumn()}}
	factory := &fakeScanSourceFactory{src: emptySource{}}
	builder, err := NewBuilder(factory, loweringExecutor())
	if err != nil {
		t.Fatal(err)
	}
	if _, err := builder.Build(context.Background(), plan, emptySink{}); err == nil {
		t.Fatal("expected missing DistinctFactory failure")
	}
	if factory.calls != 0 {
		t.Fatalf("source opened before DistinctFactory validation: %d", factory.calls)
	}

	want := errors.New("distinct factory failed")
	builder.Distincts = &distinctTestFactory{err: want}
	if _, err := builder.Build(context.Background(), plan, emptySink{}); !errors.Is(err, want) {
		t.Fatalf("err=%v want=%v", err, want)
	}
	if factory.calls != 0 {
		t.Fatalf("source opened after factory failure: %d", factory.calls)
	}
}

// TestBuildProjectDistinctLimitDAG 验证 Project 在 dependency stage、Distinct 在 bridge 边界、Limit 在 root stage。
func TestBuildProjectDistinctLimitDAG(t *testing.T) {
	op := &distinctTestOperator{}
	distincts := &distinctTestFactory{operator: op}
	sources := &fakeScanSourceFactory{src: emptySource{}}
	builder, err := NewBuilder(sources, loweringExecutor())
	if err != nil {
		t.Fatal(err)
	}
	builder.Distincts = distincts
	project := physicalProjectOverScan()
	distinct := &planner.PhysicalPlan{
		Kind: planner.PhysicalDistinct, Children: []*planner.PhysicalPlan{project}, Output: []planner.Column{distinctInputColumn()},
		Estimate: planner.CardinalityEstimate{Rows: 7, Confidence: 0.5},
	}
	limit := &planner.PhysicalPlan{Kind: planner.PhysicalLimit, Children: []*planner.PhysicalPlan{distinct}, Output: []planner.Column{distinctInputColumn()}, Limit: 2, Offset: 1}
	root, err := builder.Build(context.Background(), limit, emptySink{})
	if err != nil {
		t.Fatal(err)
	}
	if sources.calls != 1 || distincts.calls.Load() != 1 {
		t.Fatalf("source calls=%d distinct calls=%d", sources.calls, distincts.calls.Load())
	}
	if len(distincts.columns) != 1 || distincts.columns[0].Name != "id" || distincts.estimate.Rows != 7 {
		t.Fatalf("factory columns=%+v estimate=%+v", distincts.columns, distincts.estimate)
	}
	bridge, ok := root.Source.(*blocking.Bridge)
	if !ok {
		t.Fatalf("root source=%T want *blocking.Bridge", root.Source)
	}
	if len(root.Transforms) != 1 {
		t.Fatalf("root transforms=%d", len(root.Transforms))
	}
	if _, ok := root.Transforms[0].(*sqltransform.Limit); !ok {
		t.Fatalf("root transform=%T want *sqltransform.Limit", root.Transforms[0])
	}
	if len(root.Dependencies) != 1 {
		t.Fatalf("dependencies=%d", len(root.Dependencies))
	}
	dependency := root.Dependencies[0]
	if dependency.Sink != bridge {
		t.Fatalf("dependency sink=%T is not root bridge", dependency.Sink)
	}
	if len(dependency.Transforms) != 1 {
		t.Fatalf("dependency transforms=%d", len(dependency.Transforms))
	}
	if _, ok := dependency.Transforms[0].(*sqltransform.Project); !ok {
		t.Fatalf("dependency transform=%T want *sqltransform.Project", dependency.Transforms[0])
	}
	if err := pipeline.RunDAG(context.Background(), root, 1, nil); err != nil {
		t.Fatal(err)
	}
	if op.emitCalls.Load() != 1 || op.releaseCalls.Load() != 1 {
		t.Fatalf("emit=%d release=%d", op.emitCalls.Load(), op.releaseCalls.Load())
	}
}

// TestDistinctDependencyFailureReleasesOperator 验证 dependency runtime 失败时 CloseableSink abort 路径回收 blocking state。
func TestDistinctDependencyFailureReleasesOperator(t *testing.T) {
	want := errors.New("scan failed")
	op := &distinctTestOperator{}
	sources := &fakeScanSourceFactory{src: errorSource{err: want}}
	builder, err := NewBuilder(sources, loweringExecutor())
	if err != nil {
		t.Fatal(err)
	}
	builder.Distincts = &distinctTestFactory{operator: op}
	scan := physicalScan()
	scan.Output = []planner.Column{distinctInputColumn()}
	plan := &planner.PhysicalPlan{Kind: planner.PhysicalDistinct, Children: []*planner.PhysicalPlan{scan}, Output: []planner.Column{distinctInputColumn()}}
	root, err := builder.Build(context.Background(), plan, emptySink{})
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
