package hash

import (
	"math"
	"reflect"
	"testing"

	"github.com/steamwo/fssql/internal/core/memory"
	"github.com/steamwo/fssql/internal/core/types"
	"github.com/steamwo/fssql/internal/core/vector"
)

func TestSelectEqualMatchesScalarSemanticReference(t *testing.T) {
	manager := memory.NewManager(1 << 20)
	scope := manager.NewScope()
	bm := memory.NewBufferManager(nil)
	ts := []types.LogicalType{{ID: types.Double}, {ID: types.Varchar}}
	f, err := vector.NewFlat(scope, bm, ts[0], 4)
	if err != nil {
		t.Fatal(err)
	}
	f.SetFloat64(0, math.NaN())
	f.SetFloat64(1, math.Copysign(0, -1))
	f.SetFloat64(2, 12)
	f.SetNull(3)
	s, err := vector.NewConstant(scope, bm, ts[1], 4)
	if err != nil {
		t.Fatal(err)
	}
	if err := s.SetString(0, "constant"); err != nil {
		t.Fatal(err)
	}
	chunk, _ := vector.NewDataChunk(f, s)
	layout, _ := NewRelocatableRowLayout(ts)
	rows, err := NewTupleDataCollectionForLayout(scope, bm, layout, 4)
	if err != nil {
		t.Fatal(err)
	}
	for i := 0; i < 4; i++ {
		if err := rows.AppendChunkRow(chunk, i); err != nil {
			t.Fatal(err)
		}
	}
	comparer, err := NewChunkKeyComparer(chunk, []int{0, 1})
	if err != nil {
		t.Fatal(err)
	}
	probes := []int{0, 0, 1, 2, 3, 3}
	encoded := [][]byte{rows.Row(0), rows.Row(1), rows.Row(1), rows.Row(2), rows.Row(3), rows.Row(2)}
	for _, nulls := range []NullEquality{NullsUnequal, NullsEqual} {
		var want []int
		for i, probe := range probes {
			equal, err := comparer.Equal(probe, layout, encoded[i], []int{0, 1}, nulls)
			if err != nil {
				t.Fatal(err)
			}
			if equal {
				want = append(want, i)
			}
		}
		selection := make([]int, len(probes))
		n, err := comparer.SelectEqual(probes, layout, encoded, []int{0, 1}, nulls, selection)
		if err != nil {
			t.Fatal(err)
		}
		if !reflect.DeepEqual(selection[:n], want) {
			t.Fatalf("selected=%v want=%v", selection[:n], want)
		}
	}
	out, err := vector.NewFlat(scope, bm, ts[0], 2)
	if err != nil {
		t.Fatal(err)
	}
	if err := layout.MaterializeSelectedRows([][]byte{make([]byte, 0)}, []*vector.Vector{out, nil}, []bool{true, false}); err == nil {
		t.Fatal("short nonnil row accepted")
	}
	out.Release()
	rows.Release()
	chunk.Release()
	if err := scope.Close(); err != nil {
		t.Fatal(err)
	}
	if manager.Used() != 0 {
		t.Fatal("memory leak")
	}
}
