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
	"github.com/steamwo/fssql/internal/planner"
	"github.com/steamwo/fssql/internal/sql/binder"
)

type sortTestFactory struct {
	calls    atomic.Int64
	operator *distinctTestOperator
	err      error
	columns  []planner.Column
	keys     []planner.SortKey
	estimate planner.CardinalityEstimate
}

func (f *sortTestFactory) OpenSort(_ context.Context, columns []planner.Column, keys []planner.SortKey, estimate planner.CardinalityEstimate) (blocking.Operator, error) {
	f.calls.Add(1)
	f.columns = append([]planner.Column(nil), columns...)
	f.keys = append([]planner.SortKey(nil), keys...)
	f.estimate = estimate
	if f.err != nil {
		return nil, f.err
	}
	return f.operator, nil
}

func twoColumnScan() *planner.PhysicalPlan {
	scan := physicalScan()
	scan.Output = []planner.Column{
		{Name: "a", Type: types.LogicalType{ID: types.BigInt}},
		{Name: "__fssql_order_0", Type: types.LogicalType{ID: types.BigInt}, Nullable: true},
	}
	return scan
}

func hiddenProjectPhysical() *planner.PhysicalPlan {
	first := &binder.BoundExpression{Kind: binder.BoundColumn, Type: types.LogicalType{ID: types.BigInt}, ColumnIndex: 0}
	second := &binder.BoundExpression{Kind: binder.BoundColumn, Type: types.LogicalType{ID: types.BigInt}, Nullable: true, ColumnIndex: 1}
	return &planner.PhysicalPlan{
		Kind:       planner.PhysicalProject,
		Children:   []*planner.PhysicalPlan{twoColumnScan()},
		Output:     twoColumnScan().Output,
		Projection: []*binder.BoundExpression{first, second},
	}
}

func physicalSortOverHiddenProject() *planner.PhysicalPlan {
	key := &binder.BoundExpression{Kind: binder.BoundColumn, Type: types.LogicalType{ID: types.BigInt}, Nullable: true, ColumnIndex: 1}
	child := hiddenProjectPhysical()
	return &planner.PhysicalPlan{
		Kind: planner.PhysicalSort, Children: []*planner.PhysicalPlan{child}, Output: child.Output,
		Sort: []planner.SortKey{{Expr: key, Descending: true}},
		Estimate: planner.CardinalityEstimate{Rows: 11, Confidence: 0.7},
	}
}

// TestBuildSortRequiresFactoryBeforeOpeningChild 验证 SortFactory 缺失/失败时不会打开 reader。
func TestBuildSortRequiresFactoryBeforeOpeningChild(t *testing.T) {
	plan := physicalSortOverHiddenProject()
	sources := &fakeScanSourceFactory{src: emptySource{}}
	builder, err := NewBuilder(sources, loweringExecutor())
	if err != nil {
		t.Fatal(err)
	}
	if _, err := builder.Build(context.Background(), plan, emptySink{}); err == nil {
		t.Fatal("expected missing SortFactory failure")
	}
	if sources.calls != 0 {
		t.Fatalf("source opened before SortFactory validation: %d", sources.calls)
	}
	want := errors.New("sort factory failed")
	builder.Sorts = &sortTestFactory{err: want}
	if _, err := builder.Build(context.Background(), plan, emptySink{}); !errors.Is(err, want) {
		t.Fatalf("err=%v want=%v", err, want)
	}
	if sources.calls != 0 {
		t.Fatalf("source opened after SortFactory failure: %d", sources.calls)
	}
}

// TestBuildHiddenProjectSortDropLimitDAG 验证 hidden Project -> Sort -> drop Project -> Limit 的 stage 顺序。
func TestBuildHiddenProjectSortDropLimitDAG(t *testing.T) {
	op := &distinctTestOperator{}
	sorts := &sortTestFactory{operator: op}
	sources := &fakeScanSourceFactory{src: emptySource{}}
	builder, err := NewBuilder(sources, loweringExecutor())
	if err != nil {
		t.Fatal(err)
	}
	builder.Sorts = sorts
	sortPlan := physicalSortOverHiddenProject()
	visible := planner.Column{Name: "a", Type: types.LogicalType{ID: types.BigInt}}
	drop := &planner.PhysicalPlan{
		Kind: planner.PhysicalProject, Children: []*planner.PhysicalPlan{sortPlan}, Output: []planner.Column{visible},
		Projection: []*binder.BoundExpression{{Kind: binder.BoundColumn, Type: visible.Type, ColumnIndex: 0}},
	}
	limit := &planner.PhysicalPlan{Kind: planner.PhysicalLimit, Children: []*planner.PhysicalPlan{drop}, Output: drop.Output, Limit: 3}
	root, err := builder.Build(context.Background(), limit, emptySink{})
	if err != nil {
		t.Fatal(err)
	}
	if sorts.calls.Load() != 1 || sources.calls != 1 || len(sorts.keys) != 1 || sorts.keys[0].Expr.ColumnIndex != 1 || sorts.estimate.Rows != 11 {
		t.Fatalf("sort calls=%d source=%d keys=%+v estimate=%+v", sorts.calls.Load(), sources.calls, sorts.keys, sorts.estimate)
	}
	bridge, ok := root.Source.(*blocking.Bridge)
	if !ok {
		t.Fatalf("root source=%T want bridge", root.Source)
	}
	if len(root.Transforms) != 2 {
		t.Fatalf("root transforms=%d", len(root.Transforms))
	}
	if _, ok := root.Transforms[0].(*sqltransform.Project); !ok {
		t.Fatalf("root transform[0]=%T want Project", root.Transforms[0])
	}
	if _, ok := root.Transforms[1].(*sqltransform.Limit); !ok {
		t.Fatalf("root transform[1]=%T want Limit", root.Transforms[1])
	}
	if len(root.Dependencies) != 1 || root.Dependencies[0].Sink != bridge {
		t.Fatalf("dependencies=%+v", root.Dependencies)
	}
	dependency := root.Dependencies[0]
	if len(dependency.Transforms) != 1 {
		t.Fatalf("dependency transforms=%d", len(dependency.Transforms))
	}
	if _, ok := dependency.Transforms[0].(*sqltransform.Project); !ok {
		t.Fatalf("dependency transform=%T want hidden Project", dependency.Transforms[0])
	}
	if err := pipeline.RunDAG(context.Background(), root, 1, nil); err != nil {
		t.Fatal(err)
	}
	if op.emitCalls.Load() != 1 || op.releaseCalls.Load() != 1 {
		t.Fatalf("emit=%d release=%d", op.emitCalls.Load(), op.releaseCalls.Load())
	}
}

// TestSortDependencyFailureReleasesOperator 验证 dependency runtime 失败时 sort state 通过 abort lifecycle 释放。
func TestSortDependencyFailureReleasesOperator(t *testing.T) {
	want := errors.New("scan failed")
	op := &distinctTestOperator{}
	sources := &fakeScanSourceFactory{src: errorSource{err: want}}
	builder, err := NewBuilder(sources, loweringExecutor())
	if err != nil {
		t.Fatal(err)
	}
	builder.Sorts = &sortTestFactory{operator: op}
	plan := physicalSortOverHiddenProject()
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
