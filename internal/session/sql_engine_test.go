package session

import (
	"context"
	"fmt"
	"testing"

	"github.com/steamwo/fssql/internal/catalog"
	"github.com/steamwo/fssql/internal/core/memory"
	"github.com/steamwo/fssql/internal/core/stats"
	"github.com/steamwo/fssql/internal/core/types"
	"github.com/steamwo/fssql/internal/core/vector"
	"github.com/steamwo/fssql/internal/execution/pipeline"
	"github.com/steamwo/fssql/internal/function"
	"github.com/steamwo/fssql/internal/planner"
	"github.com/steamwo/fssql/internal/query"
	"github.com/steamwo/fssql/internal/sql/binder"
)

type sqlEngineTestSource struct {
	scope   *memory.Scope
	buffers *memory.BufferManager
	done    bool
}

func (s *sqlEngineTestSource) Next(ctx context.Context) (*vector.DataChunk, bool, error) {
	if err := ctx.Err(); err != nil {
		return nil, false, err
	}
	if s.done {
		return nil, false, nil
	}
	s.done = true
	column, err := vector.NewFlat(s.scope, s.buffers, types.LogicalType{ID: types.Integer}, 4)
	if err != nil {
		return nil, false, err
	}
	for i, value := range []int32{1, 2, 3, 4} {
		column.SetInt32(i, value)
	}
	chunk, err := vector.NewDataChunk(column)
	if err != nil {
		column.Release()
		return nil, false, err
	}
	return chunk, true, nil
}

type sqlEngineCollectSink struct{ values []int32 }

func (s *sqlEngineCollectSink) Consume(_ context.Context, chunk *vector.DataChunk) error {
	if chunk == nil || len(chunk.Columns) != 1 {
		return fmt.Errorf("unexpected result chunk")
	}
	for row := 0; row < chunk.Cardinality; row++ {
		value, ok := chunk.Columns[0].Int32(row)
		if !ok {
			return fmt.Errorf("unexpected NULL result")
		}
		s.values = append(s.values, value)
	}
	return nil
}
func (*sqlEngineCollectSink) Finalize(context.Context) error { return nil }

func newSQLTestEngine(t *testing.T, buffers *memory.BufferManager) *StableSQLEngine {
	t.Helper()
	cat, err := catalog.NewMemoryCatalog(16)
	if err != nil {
		t.Fatal(err)
	}
	functions := function.NewBuiltinCatalog()
	sources := QuerySourceFactoryFunc(func(q *query.Context) ([]TableBindAdapter, *RuntimeScanSourceFactory, error) {
		binding := TableBindAdapter{
			FunctionID: function.ReadCSVID,
			Bind: func(context.Context, string) (binder.ResolvedRelation, error) {
				return binder.ResolvedRelation{
					ID: "tablefunc:read_csv:test-fingerprint",
					Columns: []binder.RelationColumn{{Name: "id", Type: types.LogicalType{ID: types.Integer}, Nullable: false}},
					Fingerprint: stats.DatasetFingerprint("test-fingerprint"),
				}, nil
			},
		}
		scans, err := NewRuntimeScanSourceFactory(functions, []ScanOpenAdapter{{
			FunctionID: function.ReadCSVID,
			Open: func(_ context.Context, spec planner.ScanSpec) (pipeline.Source, error) {
				if spec.Fingerprint != stats.DatasetFingerprint("test-fingerprint") {
					return nil, fmt.Errorf("unexpected fingerprint %q", spec.Fingerprint)
				}
				return &sqlEngineTestSource{scope: q.Memory, buffers: buffers}, nil
			},
		}})
		return []TableBindAdapter{binding}, scans, err
	})
	engine, err := NewStableSQLEngine(StableSQLEngineConfig{
		Catalog: cat, Functions: functions, Sources: sources, Buffers: buffers, Workers: 1,
		Distinct: ExternalDistinctRuntimeConfig{MemoryBudget: 4096, RadixBits: 3, SpillBlockBytes: 1024},
	})
	if err != nil {
		t.Fatal(err)
	}
	return engine
}

// TestStableSQLEngineEndToEndScanFilterLimit 验证 SQL 文本通过 Parser/Binder/Planner/Optimizer/Push Pipeline 输出 managed DataChunk。
func TestStableSQLEngineEndToEndScanFilterLimit(t *testing.T) {
	manager := memory.NewManager(1 << 20)
	buffers := memory.NewBufferManager(nil)
	q := query.New(context.Background(), manager)
	engine := newSQLTestEngine(t, buffers)
	prepared, err := engine.Prepare(context.Background(), q, "SELECT id FROM read_csv('mem://rows.csv') WHERE id >= 2 LIMIT 2")
	if err != nil {
		t.Fatal(err)
	}
	if _, ok := q.SourceSnapshot(); !ok {
		t.Fatal("expected statement SourceSnapshot")
	}
	sink := &sqlEngineCollectSink{}
	if err := prepared.Run(q, sink); err != nil {
		t.Fatal(err)
	}
	if len(sink.values) != 2 || sink.values[0] != 2 || sink.values[1] != 3 {
		t.Fatalf("values=%v want=[2 3]", sink.values)
	}
	if err := q.Close(); err != nil {
		t.Fatal(err)
	}
	if manager.Used() != 0 {
		t.Fatalf("managed memory leak=%d", manager.Used())
	}
}

// TestStableSQLEngineStatefulPlanRequiresTemp 验证 DISTINCT 不会在缺少 TemporaryStorageManager 时静默退化成无界内存路径。
func TestStableSQLEngineStatefulPlanRequiresTemp(t *testing.T) {
	manager := memory.NewManager(1 << 20)
	buffers := memory.NewBufferManager(nil)
	q := query.New(context.Background(), manager)
	engine := newSQLTestEngine(t, buffers)
	prepared, err := engine.Prepare(context.Background(), q, "SELECT DISTINCT id FROM read_csv('mem://rows.csv')")
	if err != nil {
		t.Fatal(err)
	}
	if err := prepared.Run(q, &sqlEngineCollectSink{}); err == nil {
		t.Fatal("expected DISTINCT without query temp scope to fail")
	}
	if err := q.Close(); err != nil {
		t.Fatal(err)
	}
	if manager.Used() != 0 {
		t.Fatalf("managed memory leak=%d", manager.Used())
	}
}
