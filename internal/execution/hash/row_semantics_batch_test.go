package hash

import (
	"math"
	"testing"

	"github.com/steamwo/fssql/internal/core/memory"
	"github.com/steamwo/fssql/internal/core/types"
	"github.com/steamwo/fssql/internal/core/vector"
)

func TestChunkKeyComparerMatchesEncodedRows(t *testing.T) {
	mm := memory.NewManager(1 << 20)
	scope := mm.NewScope()
	bm := memory.NewBufferManager(nil)
	typ := types.LogicalType{ID: types.Varchar}
	layout, err := NewRelocatableRowLayout([]types.LogicalType{typ})
	if err != nil {
		t.Fatal(err)
	}
	build := mustVarcharChunk(t, scope, bm, []string{"abc", "abd", ""})
	build.Columns[0].SetNull(2)
	rows, err := NewTupleDataCollectionForLayout(scope, bm, layout, 3)
	if err != nil {
		t.Fatal(err)
	}
	for row := 0; row < 3; row++ {
		if err := rows.AppendChunkRow(build, row); err != nil {
			t.Fatal(err)
		}
	}
	probe := mustVarcharChunk(t, scope, bm, []string{"abc", "abd", ""})
	probe.Columns[0].SetNull(2)
	comparer, err := NewChunkKeyComparer(probe, []int{0})
	if err != nil {
		t.Fatal(err)
	}
	if ok, err := comparer.Equal(0, layout, rows.Row(0), []int{0}, NullsUnequal); err != nil || !ok {
		t.Fatalf("abc vs abc ok=%v err=%v", ok, err)
	}
	if ok, err := comparer.Equal(0, layout, rows.Row(1), []int{0}, NullsUnequal); err != nil || ok {
		t.Fatalf("abc vs abd ok=%v err=%v", ok, err)
	}
	if ok, err := comparer.Equal(2, layout, rows.Row(2), []int{0}, NullsUnequal); err != nil || ok {
		t.Fatalf("null vs null unequal policy ok=%v err=%v", ok, err)
	}
	if ok, err := comparer.Equal(2, layout, rows.Row(2), []int{0}, NullsEqual); err != nil || !ok {
		t.Fatalf("null vs null equal policy ok=%v err=%v", ok, err)
	}
	rows.Release()
	probe.Release()
	build.Release()
	if err := scope.Close(); err != nil {
		t.Fatal(err)
	}
	if mm.Used() != 0 {
		t.Fatalf("managed leak=%d", mm.Used())
	}
}

func TestChunkKeyComparerFloatNaNAndSignedZero(t *testing.T) {
	mm := memory.NewManager(1 << 20)
	scope := mm.NewScope()
	bm := memory.NewBufferManager(nil)
	typ := types.LogicalType{ID: types.Double}
	layout, err := NewRelocatableRowLayout([]types.LogicalType{typ})
	if err != nil {
		t.Fatal(err)
	}
	build, err := vector.NewFlat(scope, bm, typ, 2)
	if err != nil {
		t.Fatal(err)
	}
	build.SetFloat64(0, math.NaN())
	build.SetFloat64(1, 0)
	buildChunk, err := vector.NewDataChunk(build)
	if err != nil {
		t.Fatal(err)
	}
	rows, err := NewTupleDataCollectionForLayout(scope, bm, layout, 2)
	if err != nil {
		t.Fatal(err)
	}
	for row := 0; row < 2; row++ {
		if err := rows.AppendChunkRow(buildChunk, row); err != nil {
			t.Fatal(err)
		}
	}
	probe, err := vector.NewFlat(scope, bm, typ, 2)
	if err != nil {
		t.Fatal(err)
	}
	probe.SetFloat64(0, math.NaN())
	probe.SetFloat64(1, math.Copysign(0, -1))
	chunk, err := vector.NewDataChunk(probe)
	if err != nil {
		t.Fatal(err)
	}
	comparer, err := NewChunkKeyComparer(chunk, []int{0})
	if err != nil {
		t.Fatal(err)
	}
	if ok, err := comparer.Equal(0, layout, rows.Row(0), []int{0}, NullsUnequal); err != nil || !ok {
		t.Fatalf("NaN vs NaN ok=%v err=%v", ok, err)
	}
	if ok, err := comparer.Equal(1, layout, rows.Row(1), []int{0}, NullsUnequal); err != nil || !ok {
		t.Fatalf("-0 vs +0 ok=%v err=%v", ok, err)
	}
	chunk.Release()
	rows.Release()
	buildChunk.Release()
	if err := scope.Close(); err != nil {
		t.Fatal(err)
	}
	if mm.Used() != 0 {
		t.Fatalf("managed leak=%d", mm.Used())
	}
}

func mustVarcharChunk(t *testing.T, scope *memory.Scope, bm *memory.BufferManager, values []string) *vector.DataChunk {
	t.Helper()
	v, err := vector.NewFlat(scope, bm, types.LogicalType{ID: types.Varchar}, len(values))
	if err != nil {
		t.Fatal(err)
	}
	for i, value := range values {
		if err := v.SetString(i, value); err != nil {
			t.Fatal(err)
		}
	}
	chunk, err := vector.NewDataChunk(v)
	if err != nil {
		v.Release()
		t.Fatal(err)
	}
	return chunk
}
