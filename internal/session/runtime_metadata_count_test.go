package session

import (
	"context"
	"testing"

	"github.com/steamwo/fssql/internal/core/types"
	"github.com/steamwo/fssql/internal/execution/pipeline"
	"github.com/steamwo/fssql/internal/function"
	"github.com/steamwo/fssql/internal/planner"
	"github.com/steamwo/fssql/internal/sql/binder"
)

func TestRuntimeMetadataCountAcceptsOnlyExactCountStar(t *testing.T) {
	calls := 0
	open := func(context.Context, planner.ScanSpec) (pipeline.Source, error) { return runtimeTestSource{}, nil }
	factory, err := NewRuntimeScanSourceFactory(function.NewBuiltinCatalog(), []ScanOpenAdapter{{FunctionID: function.ReadParquetID, Open: open, OpenCount: func(ctx context.Context, spec planner.ScanSpec) (pipeline.Source, error) {
		calls++
		return open(ctx, spec)
	}}})
	if err != nil {
		t.Fatal(err)
	}
	// Internal adapters must preserve optional aggregate dispatch.
	factory, err = factory.WithInternalAdapters(nil)
	if err != nil {
		t.Fatal(err)
	}
	count := planner.AggregateSpec{FunctionID: uint16(function.CountID), Output: planner.Column{Type: types.LogicalType{ID: types.BigInt}}}
	for _, tc := range []struct {
		name      string
		spec      planner.ScanSpec
		aggregate planner.AggregateSpec
		accepted  bool
	}{
		{"count-star", runtimeScanSpec("read_parquet"), count, true},
		{"count-column", runtimeScanSpec("read_parquet"), func() planner.AggregateSpec {
			a := count
			a.Arguments = []*binder.BoundExpression{{Kind: binder.BoundColumn}}
			return a
		}(), false},
		{"distinct", runtimeScanSpec("read_parquet"), func() planner.AggregateSpec { a := count; a.Distinct = true; return a }(), false},
		{"filter", func() planner.ScanSpec {
			s := runtimeScanSpec("read_parquet")
			s.Pushdown.Filters = []*binder.BoundExpression{{Kind: binder.BoundLiteral}}
			return s
		}(), count, false},
		{"csv", runtimeScanSpec("read_csv"), count, false},
	} {
		t.Run(tc.name, func(t *testing.T) {
			_, accepted, err := factory.OpenAggregateScan(context.Background(), tc.spec, []planner.AggregateSpec{tc.aggregate})
			if err != nil || accepted != tc.accepted {
				t.Fatalf("accepted=%v err=%v", accepted, err)
			}
		})
	}
	if calls != 1 {
		t.Fatalf("metadata calls=%d", calls)
	}
}

func TestRuntimeNDJSONCountUsesOptionalSourceOnlyWithoutFilters(t *testing.T) {
	calls := 0
	factory, err := NewRuntimeScanSourceFactory(function.NewBuiltinCatalog(), []ScanOpenAdapter{{
		FunctionID: function.ReadNDJSONID,
		Open: func(context.Context, planner.ScanSpec) (pipeline.Source, error) {
			return runtimeTestSource{}, nil
		},
		OpenCount: func(context.Context, planner.ScanSpec) (pipeline.Source, error) {
			calls++
			return runtimeTestSource{}, nil
		},
	}})
	if err != nil {
		t.Fatal(err)
	}
	count := planner.AggregateSpec{FunctionID: uint16(function.CountID), Output: planner.Column{Type: types.LogicalType{ID: types.BigInt}}}
	spec := runtimeScanSpec("read_ndjson")
	if _, accepted, err := factory.OpenAggregateScan(context.Background(), spec, []planner.AggregateSpec{count}); err != nil || !accepted {
		t.Fatalf("count accepted=%v err=%v", accepted, err)
	}
	spec.Pushdown.Filters = []*binder.BoundExpression{{Kind: binder.BoundLiteral}}
	if _, accepted, err := factory.OpenAggregateScan(context.Background(), spec, []planner.AggregateSpec{count}); err != nil || accepted {
		t.Fatalf("filtered count accepted=%v err=%v", accepted, err)
	}
	if calls != 1 {
		t.Fatalf("count source calls=%d", calls)
	}
}
