package lowering

import (
	"context"
	"errors"
	"testing"

	"github.com/steamwo/fssql/internal/core/memory"
	"github.com/steamwo/fssql/internal/core/stats"
	"github.com/steamwo/fssql/internal/core/types"
	"github.com/steamwo/fssql/internal/core/vector"
	"github.com/steamwo/fssql/internal/execution/expression"
	"github.com/steamwo/fssql/internal/execution/pipeline"
	"github.com/steamwo/fssql/internal/execution/sqltransform"
	"github.com/steamwo/fssql/internal/planner"
	"github.com/steamwo/fssql/internal/sql/binder"
)

// fakeScanSourceFactory 记录 Open 调用，验证 lowering 不会提前触发未支持 operator 下的 I/O。
type fakeScanSourceFactory struct {
	calls int
	last  planner.ScanSpec
	src   pipeline.Source
	err   error
}

func (f *fakeScanSourceFactory) Open(_ context.Context, spec planner.ScanSpec) (pipeline.Source, error) {
	f.calls++
	f.last = spec
	return f.src, f.err
}

// emptySource 仅用于 Build 结构测试，不执行真实数据读取。
type emptySource struct{}

func (emptySource) Next(context.Context) (*vector.DataChunk, bool, error) { return nil, false, nil }

// emptySink 仅验证 lowering 能接收 generic pipeline sink。
type emptySink struct{}

func (emptySink) Consume(context.Context, *vector.DataChunk) error { return nil }
func (emptySink) Finalize(context.Context) error                  { return nil }

func loweringExecutor() *expression.Executor {
	manager := memory.NewManager(1 << 20)
	return expression.NewExecutor(manager.NewScope(), memory.NewBufferManager(nil))
}

func physicalScan() *planner.PhysicalPlan {
	return &planner.PhysicalPlan{
		Kind: planner.PhysicalScan,
		Scan: &planner.ScanSpec{
			RelationID:  "relation-1",
			Function:    "read_parquet",
			Source:      "memory://data.parquet",
			Fingerprint: stats.DatasetFingerprint("dataset-fingerprint"),
		},
	}
}

// TestBuildScanFilterProjectPipeline 验证 lowering 顺序固定为 source -> filter -> project -> sink。
func TestBuildScanFilterProjectPipeline(t *testing.T) {
	factory := &fakeScanSourceFactory{src: emptySource{}}
	builder, err := NewBuilder(factory, loweringExecutor())
	if err != nil {
		t.Fatal(err)
	}
	column := &binder.BoundExpression{Kind: binder.BoundColumn, Type: types.LogicalType{ID: types.BigInt}, ColumnIndex: 0}
	one := &binder.BoundExpression{Kind: binder.BoundLiteral, Type: types.LogicalType{ID: types.BigInt}, Literal: "1"}
	predicate := &binder.BoundExpression{Kind: binder.BoundBinary, Type: types.LogicalType{ID: types.Boolean}, Op: ">", Children: []*binder.BoundExpression{column, one}}
	filter := &planner.PhysicalPlan{Kind: planner.PhysicalFilter, Children: []*planner.PhysicalPlan{physicalScan()}, Predicate: predicate}
	project := &planner.PhysicalPlan{Kind: planner.PhysicalProject, Children: []*planner.PhysicalPlan{filter}, Projection: []*binder.BoundExpression{column}}
	pipe, err := builder.Build(context.Background(), project, emptySink{})
	if err != nil {
		t.Fatal(err)
	}
	if factory.calls != 1 || factory.last.Fingerprint != stats.DatasetFingerprint("dataset-fingerprint") {
		t.Fatalf("factory calls=%d fingerprint=%q", factory.calls, factory.last.Fingerprint)
	}
	if pipe.Source != factory.src || pipe.Sink == nil || pipe.ID != "sql:relation-1" {
		t.Fatalf("unexpected pipeline source/sink/id: %#v", pipe)
	}
	if len(pipe.Transforms) != 2 {
		t.Fatalf("transforms=%d", len(pipe.Transforms))
	}
	if _, ok := pipe.Transforms[0].(*sqltransform.Filter); !ok {
		t.Fatalf("transform[0]=%T", pipe.Transforms[0])
	}
	if _, ok := pipe.Transforms[1].(*sqltransform.Project); !ok {
		t.Fatalf("transform[1]=%T", pipe.Transforms[1])
	}
}

// TestBuildRejectsEmptyFingerprintBeforeOpen 验证没有 frozen SourceSnapshot identity 时禁止打开源。
func TestBuildRejectsEmptyFingerprintBeforeOpen(t *testing.T) {
	factory := &fakeScanSourceFactory{src: emptySource{}}
	builder, err := NewBuilder(factory, loweringExecutor())
	if err != nil {
		t.Fatal(err)
	}
	scan := physicalScan()
	scan.Scan.Fingerprint = ""
	if _, err := builder.Build(context.Background(), scan, emptySink{}); err == nil {
		t.Fatal("expected empty fingerprint failure")
	}
	if factory.calls != 0 {
		t.Fatalf("source opened before fingerprint validation: %d", factory.calls)
	}
}

// TestBuildRejectsUnsupportedOperatorBeforeOpen 验证未接通的 Sort 不会降级或半执行其 child scan。
func TestBuildRejectsUnsupportedOperatorBeforeOpen(t *testing.T) {
	factory := &fakeScanSourceFactory{src: emptySource{}}
	builder, err := NewBuilder(factory, loweringExecutor())
	if err != nil {
		t.Fatal(err)
	}
	plan := &planner.PhysicalPlan{Kind: planner.PhysicalSort, Children: []*planner.PhysicalPlan{physicalScan()}}
	if _, err := builder.Build(context.Background(), plan, emptySink{}); err == nil {
		t.Fatal("expected unsupported sort failure")
	}
	if factory.calls != 0 {
		t.Fatalf("unsupported plan opened source: %d", factory.calls)
	}
}

// TestBuildPropagatesSourceOpenError 验证 scan factory 错误保持显式，不返回半初始化 Pipeline。
func TestBuildPropagatesSourceOpenError(t *testing.T) {
	want := errors.New("open failed")
	factory := &fakeScanSourceFactory{err: want}
	builder, err := NewBuilder(factory, loweringExecutor())
	if err != nil {
		t.Fatal(err)
	}
	if pipe, err := builder.Build(context.Background(), physicalScan(), emptySink{}); !errors.Is(err, want) || pipe != nil {
		t.Fatalf("pipe=%v err=%v", pipe, err)
	}
}
