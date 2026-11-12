package hash

import (
	"fmt"
	"math"
	"reflect"
	"testing"

	"github.com/steamwo/fssql/internal/core/memory"
	"github.com/steamwo/fssql/internal/core/types"
	"github.com/steamwo/fssql/internal/core/vector"
)

func TestSelectEqualFlatAndEncodedKeys(t *testing.T) {
	for _, kind := range []string{"flat", "constant", "dictionary"} {
		for _, nullable := range []bool{false, true} {
			t.Run(fmt.Sprintf("%s/nullable%v", kind, nullable), func(t *testing.T) {
				manager := memory.NewManager(1 << 20)
				scope := manager.NewScope()
				defer func() {
					if err := scope.Close(); err != nil {
						t.Fatal(err)
					}
					if manager.Used() != 0 {
						t.Fatalf("managed leak=%d", manager.Used())
					}
				}()
				bm := memory.NewBufferManager(nil)
				const count = 129
				typ := types.LogicalType{ID: types.Double}
				base, err := vector.NewFlat(scope, bm, typ, count)
				if kind == "constant" {
					if err != nil {
						t.Fatal(err)
					}
					base.Release()
					base, err = vector.NewConstant(scope, bm, typ, count)
				}
				if err != nil {
					t.Fatal(err)
				}
				values := []float64{math.NaN(), 0, math.Copysign(0, -1), math.Inf(1), math.Inf(-1), 42}
				for i := 0; i < count; i++ {
					base.SetFloat64(i, values[i%len(values)])
					if nullable && i%17 == 0 {
						base.SetNull(i)
					}
				}
				if kind == "constant" && nullable {
					base.SetNull(0)
				}
				key := base
				if kind == "dictionary" {
					indices := make([]uint32, count)
					for i := range indices {
						indices[i] = uint32(count - i - 1)
					}
					key, err = vector.NewDictionary(base, vector.SelectionFrom(indices))
					if err != nil {
						t.Fatal(err)
					}
					defer base.Release()
				}
				chunk, err := vector.NewDataChunk(key)
				if err != nil {
					t.Fatal(err)
				}
				defer chunk.Release()
				layout, _ := NewRelocatableRowLayout([]types.LogicalType{typ})
				encoded := make([][]byte, count)
				probes := make([]int, count)
				for i := range encoded {
					encoded[i] = make([]byte, layout.FixedWidth)
					if _, err := layout.WriteRow(chunk, i, encoded[i]); err != nil {
						t.Fatal(err)
					}
					probes[i] = (i * 7) % count
				}
				comparer, err := NewChunkKeyComparer(chunk, []int{0})
				if err != nil {
					t.Fatal(err)
				}
				for _, nulls := range []NullEquality{NullsEqual, NullsUnequal} {
					want := make([]int, 0, count)
					for i, probe := range probes {
						equal, err := comparer.Equal(probe, layout, encoded[i], []int{0}, nulls)
						if err != nil {
							t.Fatal(err)
						}
						if equal {
							want = append(want, i)
						}
					}
					selection := make([]int, count)
					n, err := comparer.SelectEqual(probes, layout, encoded, []int{0}, nulls, selection)
					if err != nil || !reflect.DeepEqual(selection[:n], want) {
						t.Fatalf("selected=%v want=%v err=%v", selection[:n], want, err)
					}
				}
			})
		}
	}
}

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
