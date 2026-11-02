package session

import (
	"context"
	"errors"
	"testing"

	"github.com/steamwo/fssql/internal/core/stats"
	"github.com/steamwo/fssql/internal/core/vector"
	"github.com/steamwo/fssql/internal/execution/pipeline"
	"github.com/steamwo/fssql/internal/function"
	"github.com/steamwo/fssql/internal/planner"
)

// runtimeTestSource 只用于 factory dispatch 测试，不执行真实格式 I/O。
type runtimeTestSource struct{}

func (runtimeTestSource) Next(context.Context) (*vector.DataChunk, bool, error) { return nil, false, nil }

func runtimeScanSpec(functionName string) planner.ScanSpec {
	return planner.ScanSpec{
		RelationID:  "relation-1",
		Function:    functionName,
		Source:      "memory://data",
		Fingerprint: stats.DatasetFingerprint("fingerprint-1"),
		Pushdown: planner.ScanPushdown{
			Projection: []int{0, 2},
		},
	}
}

// TestRuntimeScanSourceFactoryDispatchesByFunctionCatalogID 验证名称解析只经过统一 FunctionCatalog，callback 按内部 FunctionID 分派。
func TestRuntimeScanSourceFactoryDispatchesByFunctionCatalogID(t *testing.T) {
	catalog := function.NewBuiltinCatalog()
	calls := 0
	factory, err := NewRuntimeScanSourceFactory(catalog, []ScanOpenAdapter{{
		FunctionID: function.ReadParquetID,
		Open: func(_ context.Context, spec planner.ScanSpec) (pipeline.Source, error) {
			calls++
			if spec.Function != "read_parquet" || spec.Fingerprint != stats.DatasetFingerprint("fingerprint-1") {
				t.Fatalf("unexpected spec: %#v", spec)
			}
			return runtimeTestSource{}, nil
		},
	}})
	if err != nil {
		t.Fatal(err)
	}
	source, err := factory.Open(context.Background(), runtimeScanSpec("READ_PARQUET"))
	if err != nil {
		t.Fatal(err)
	}
	if source == nil || calls != 1 {
		t.Fatalf("source=%v calls=%d", source, calls)
	}
}

// TestRuntimeScanSourceFactoryClonesPushdownSlices 验证 concrete callback 不能修改 PhysicalPlan-owned slice backing array。
func TestRuntimeScanSourceFactoryClonesPushdownSlices(t *testing.T) {
	catalog := function.NewBuiltinCatalog()
	factory, err := NewRuntimeScanSourceFactory(catalog, []ScanOpenAdapter{{
		FunctionID: function.ReadCSVID,
		Open: func(_ context.Context, spec planner.ScanSpec) (pipeline.Source, error) {
			spec.Pushdown.Projection[0] = 99
			return runtimeTestSource{}, nil
		},
	}})
	if err != nil {
		t.Fatal(err)
	}
	spec := runtimeScanSpec("read_csv")
	if _, err := factory.Open(context.Background(), spec); err != nil {
		t.Fatal(err)
	}
	if spec.Pushdown.Projection[0] != 0 {
		t.Fatalf("caller projection mutated: %#v", spec.Pushdown.Projection)
	}
}

// TestRuntimeScanSourceFactoryRejectsNonTableAdapter 验证 aggregate/scalar function 不能注册为 runtime scan adapter。
func TestRuntimeScanSourceFactoryRejectsNonTableAdapter(t *testing.T) {
	if _, err := NewRuntimeScanSourceFactory(function.NewBuiltinCatalog(), []ScanOpenAdapter{{
		FunctionID: function.CountID,
		Open:       func(context.Context, planner.ScanSpec) (pipeline.Source, error) { return runtimeTestSource{}, nil },
	}}); err == nil {
		t.Fatal("expected non-table adapter rejection")
	}
}

// TestRuntimeScanSourceFactoryFailsClosed 验证未注册函数、空 identity/source/fingerprint 与 nil source 均不会降级。
func TestRuntimeScanSourceFactoryFailsClosed(t *testing.T) {
	catalog := function.NewBuiltinCatalog()
	factory, err := NewRuntimeScanSourceFactory(catalog, []ScanOpenAdapter{{
		FunctionID: function.ReadJSONID,
		Open:       func(context.Context, planner.ScanSpec) (pipeline.Source, error) { return nil, nil },
	}})
	if err != nil {
		t.Fatal(err)
	}
	cases := []planner.ScanSpec{
		runtimeScanSpec("read_parquet"),
		runtimeScanSpec("unknown_reader"),
	}
	emptyRelation := runtimeScanSpec("read_json")
	emptyRelation.RelationID = ""
	cases = append(cases, emptyRelation)
	emptySource := runtimeScanSpec("read_json")
	emptySource.Source = ""
	cases = append(cases, emptySource)
	emptyFingerprint := runtimeScanSpec("read_json")
	emptyFingerprint.Fingerprint = ""
	cases = append(cases, emptyFingerprint)
	cases = append(cases, runtimeScanSpec("read_json"))
	for i, spec := range cases {
		if source, err := factory.Open(context.Background(), spec); err == nil || source != nil {
			t.Fatalf("case=%d source=%v err=%v", i, source, err)
		}
	}
}

// TestRuntimeScanSourceFactoryPropagatesAdapterError 验证具体格式 open error 保留 errors.Is 链。
func TestRuntimeScanSourceFactoryPropagatesAdapterError(t *testing.T) {
	want := errors.New("reader open failed")
	factory, err := NewRuntimeScanSourceFactory(function.NewBuiltinCatalog(), []ScanOpenAdapter{{
		FunctionID: function.ReadNDJSONID,
		Open:       func(context.Context, planner.ScanSpec) (pipeline.Source, error) { return nil, want },
	}})
	if err != nil {
		t.Fatal(err)
	}
	if _, err := factory.Open(context.Background(), runtimeScanSpec("read_ndjson")); !errors.Is(err, want) {
		t.Fatalf("err=%v", err)
	}
}
