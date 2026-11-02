package lowering

import (
	"context"
	"testing"

	"github.com/steamwo/fssql/internal/execution/sqltransform"
	"github.com/steamwo/fssql/internal/planner"
	"github.com/steamwo/fssql/internal/sql/binder"
)

// TestBuildPhysicalLimitAfterProject 验证 output modifier 在 Project 之后执行，并保留精确 LIMIT 0/offset 数值。
func TestBuildPhysicalLimitAfterProject(t *testing.T) {
	factory := &fakeScanSourceFactory{src: emptySource{}}
	builder, err := NewBuilder(factory, loweringExecutor())
	if err != nil {
		t.Fatal(err)
	}
	column := &binder.BoundExpression{Kind: binder.BoundColumn, ColumnIndex: 0}
	project := &planner.PhysicalPlan{Kind: planner.PhysicalProject, Children: []*planner.PhysicalPlan{physicalScan()}, Projection: []*binder.BoundExpression{column}}
	limitPlan := &planner.PhysicalPlan{Kind: planner.PhysicalLimit, Children: []*planner.PhysicalPlan{project}, Limit: 0, Offset: 2}
	pipe, err := builder.Build(context.Background(), limitPlan, emptySink{})
	if err != nil {
		t.Fatal(err)
	}
	if len(pipe.Transforms) != 2 {
		t.Fatalf("transforms=%d", len(pipe.Transforms))
	}
	if _, ok := pipe.Transforms[0].(*sqltransform.Project); !ok {
		t.Fatalf("transform[0]=%T", pipe.Transforms[0])
	}
	limit, ok := pipe.Transforms[1].(*sqltransform.Limit)
	if !ok {
		t.Fatalf("transform[1]=%T", pipe.Transforms[1])
	}
	if limit.Unlimited || limit.Limit != 0 || limit.Offset != 2 {
		t.Fatalf("limit=%+v", limit)
	}
}

// TestBuildOffsetOnlyConvertsUnlimitedSentinel 验证 Planner sentinel 只在 lowering boundary 转成 runtime Unlimited flag。
func TestBuildOffsetOnlyConvertsUnlimitedSentinel(t *testing.T) {
	factory := &fakeScanSourceFactory{src: emptySource{}}
	builder, err := NewBuilder(factory, loweringExecutor())
	if err != nil {
		t.Fatal(err)
	}
	plan := &planner.PhysicalPlan{Kind: planner.PhysicalLimit, Children: []*planner.PhysicalPlan{physicalScan()}, Limit: planner.UnlimitedRows, Offset: 3}
	pipe, err := builder.Build(context.Background(), plan, emptySink{})
	if err != nil {
		t.Fatal(err)
	}
	limit, ok := pipe.Transforms[0].(*sqltransform.Limit)
	if !ok || !limit.Unlimited || limit.Offset != 3 {
		t.Fatalf("limit=%+v type=%T", limit, pipe.Transforms[0])
	}
}
