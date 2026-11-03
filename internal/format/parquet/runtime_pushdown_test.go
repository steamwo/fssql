package parquet

import (
	"context"
	"io"
	"testing"

	"github.com/steamwo/fssql/internal/core/memory"
	"github.com/steamwo/fssql/internal/core/types"
	"github.com/steamwo/fssql/internal/core/vector"
	formatscan "github.com/steamwo/fssql/internal/format/scan"
	"github.com/steamwo/fssql/internal/storage/fileset"
)

func TestResolveRuntimePushdownMapsProjectionAndFilter(t *testing.T) {
	schema := ResolvedSchema{Fields: []types.StructField{
		{Name: "id", Type: types.LogicalType{ID: types.BigInt}},
		{Name: "grp", Type: types.LogicalType{ID: types.Integer}},
		{Name: "payload", Type: types.LogicalType{ID: types.Varchar}},
	}}
	indexes, names, filters, err := resolveRuntimePushdown(schema, formatscan.Pushdown{
		Projection: []int{2, 0, 0},
		Filters: []formatscan.IndexedFilter{{
			ColumnIndex: 1,
			Op:          formatscan.GreaterEqual,
			Value:       formatscan.Literal{Type: types.LogicalType{ID: types.Integer}, Int64: 7},
		}},
	})
	if err != nil {
		t.Fatal(err)
	}
	if len(indexes) != 3 || indexes[0] != 0 || indexes[1] != 1 || indexes[2] != 2 {
		t.Fatalf("indexes=%v", indexes)
	}
	if len(names) != 3 || names[0] != "id" || names[1] != "grp" || names[2] != "payload" {
		t.Fatalf("names=%v", names)
	}
	if len(filters) != 1 || filters[0].Column != "grp" || filters[0].Op != formatscan.GreaterEqual || filters[0].Value.Int64 != 7 {
		t.Fatalf("filters=%+v", filters)
	}
}

func TestResolveRuntimePushdownKeepsNestedProjectionFullWidth(t *testing.T) {
	child := types.LogicalType{ID: types.BigInt}
	schema := ResolvedSchema{Fields: []types.StructField{
		{Name: "id", Type: types.LogicalType{ID: types.BigInt}},
		{Name: "items", Type: types.LogicalType{ID: types.List, Child: &child}},
	}}
	indexes, names, _, err := resolveRuntimePushdown(schema, formatscan.Pushdown{Projection: []int{1}})
	if err != nil {
		t.Fatal(err)
	}
	if indexes != nil || names != nil {
		t.Fatalf("nested projection must stay full width: indexes=%v names=%v", indexes, names)
	}
}

type runtimeProjectedFakeScanner struct {
	scope   *memory.Scope
	buffers *memory.BufferManager
	emitted bool
	closed  *int
}

func (s *runtimeProjectedFakeScanner) Next(context.Context) (*vector.DataChunk, error) {
	if s.emitted {
		return nil, io.EOF
	}
	s.emitted = true
	left, err := vector.NewFlat(s.scope, s.buffers, types.LogicalType{ID: types.BigInt}, 1)
	if err != nil {
		return nil, err
	}
	right, err := vector.NewFlat(s.scope, s.buffers, types.LogicalType{ID: types.BigInt}, 1)
	if err != nil {
		left.Release()
		return nil, err
	}
	left.SetInt64(0, 10)
	right.SetInt64(0, 30)
	chunk, err := vector.NewDataChunk(left, right)
	if err != nil {
		left.Release()
		right.Release()
		return nil, err
	}
	return chunk, nil
}

func (s *runtimeProjectedFakeScanner) Close() {
	if s.closed != nil {
		(*s.closed)++
	}
}

func TestRuntimeSourceProjectedChunkPreservesBoundColumnIndexes(t *testing.T) {
	manager := memory.NewManager(1 << 20)
	scope := manager.NewScope()
	buffers := memory.NewBufferManager(nil)
	closed := 0
	prepare := func(context.Context, fileset.FileSnapshot) (runtimePreparedFile, error) {
		return runtimePreparedFile{
			rowGroups: []RowGroup{{NumRows: 1}},
			newScanner: func(RowGroup) (runtimeRowGroupScanner, error) {
				return &runtimeProjectedFakeScanner{scope: scope, buffers: buffers, closed: &closed}, nil
			},
		}, nil
	}
	source, err := newRuntimeSource([]fileset.FileSnapshot{runtimeTestFile(t, "projection.parquet")}, prepare)
	if err != nil {
		t.Fatal(err)
	}
	source.outputTypes = []types.LogicalType{{ID: types.BigInt}, {ID: types.Varchar}, {ID: types.BigInt}}
	source.projectionIndexes = []int{0, 2}
	source.scope = scope
	source.buffers = buffers

	chunk, ok, err := source.Next(context.Background())
	if err != nil || !ok || chunk == nil {
		t.Fatalf("chunk=%v ok=%v err=%v", chunk, ok, err)
	}
	if len(chunk.Columns) != 3 {
		t.Fatalf("columns=%d", len(chunk.Columns))
	}
	left, leftOK := chunk.Columns[0].Int64(0)
	right, rightOK := chunk.Columns[2].Int64(0)
	if !leftOK || left != 10 || !rightOK || right != 30 {
		t.Fatalf("values left=%d/%v right=%d/%v", left, leftOK, right, rightOK)
	}
	if chunk.Columns[1].IsValid(0) {
		t.Fatal("unread placeholder column must be NULL")
	}
	chunk.Release()
	if err := source.Close(); err != nil {
		t.Fatal(err)
	}
	if closed != 1 {
		t.Fatalf("closed=%d", closed)
	}
	if manager.Used() != 0 {
		t.Fatalf("managed leak=%d", manager.Used())
	}
}

func TestRuntimeSourceLimitStopsBeforeOpeningNextRowGroup(t *testing.T) {
	manager := memory.NewManager(1 << 20)
	scope := manager.NewScope()
	buffers := memory.NewBufferManager(nil)
	created := 0
	closed := 0
	prepare := func(context.Context, fileset.FileSnapshot) (runtimePreparedFile, error) {
		return runtimePreparedFile{
			rowGroups: []RowGroup{{NumRows: 1}, {NumRows: 1}},
			newScanner: func(rg RowGroup) (runtimeRowGroupScanner, error) {
				created++
				return &runtimeFakeScanner{scope: scope, buffers: buffers, value: rg.NumRows, closed: &closed}, nil
			},
		}, nil
	}
	source, err := newRuntimeSource([]fileset.FileSnapshot{runtimeTestFile(t, "limit.parquet")}, prepare)
	if err != nil {
		t.Fatal(err)
	}
	source.limit = 1
	chunk, ok, err := source.Next(context.Background())
	if err != nil || !ok || chunk == nil {
		t.Fatalf("chunk=%v ok=%v err=%v", chunk, ok, err)
	}
	chunk.Release()
	if chunk, ok, err := source.Next(context.Background()); err != nil || ok || chunk != nil {
		t.Fatalf("limit next chunk=%v ok=%v err=%v", chunk, ok, err)
	}
	if created != 1 || closed != 1 {
		t.Fatalf("created=%d closed=%d", created, closed)
	}
	if err := source.Close(); err != nil {
		t.Fatal(err)
	}
	if manager.Used() != 0 {
		t.Fatalf("managed leak=%d", manager.Used())
	}
}

func BenchmarkRuntimeProjectedChunkExpansion(b *testing.B) {
	manager := memory.NewManager(64 << 20)
	scope := manager.NewScope()
	buffers := memory.NewBufferManager(nil)
	source := &RuntimeSource{
		outputTypes:       []types.LogicalType{{ID: types.BigInt}, {ID: types.Varchar}, {ID: types.BigInt}},
		projectionIndexes: []int{0, 2},
		scope:             scope,
		buffers:           buffers,
	}
	b.ReportAllocs()
	for i := 0; i < b.N; i++ {
		left, _ := vector.NewFlat(scope, buffers, types.LogicalType{ID: types.BigInt}, vector.DefaultVectorSize)
		right, _ := vector.NewFlat(scope, buffers, types.LogicalType{ID: types.BigInt}, vector.DefaultVectorSize)
		chunk, _ := vector.NewDataChunk(left, right)
		expanded, err := source.expandProjectedChunk(chunk)
		if err != nil {
			b.Fatal(err)
		}
		expanded.Release()
	}
	if scope.Used() != 0 {
		b.Fatalf("managed leak=%d", scope.Used())
	}
}
