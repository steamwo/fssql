package sort

import (
	"context"
	"math"
	stdsort "sort"
	"testing"

	"github.com/steamwo/fssql/internal/core/types"
	"github.com/steamwo/fssql/internal/core/vector"
	exechash "github.com/steamwo/fssql/internal/execution/hash"
)

func TestTopNCachedThresholdMatchesFullSort(t *testing.T) {
	h, cleanup := newSortHarness(t, 8<<20)
	defer cleanup()
	inputTypes := []types.LogicalType{{ID: types.Double}, {ID: types.BigInt}}
	layout, err := exechash.NewRelocatableRowLayout(inputTypes)
	if err != nil {
		t.Fatal(err)
	}
	for _, desc := range []bool{false, true} {
		for _, nullsFirst := range []bool{false, true} {
			for _, kind := range []vector.VectorType{vector.Flat, vector.Constant, vector.Dictionary} {
				keys := []Key{{Column: 0, Desc: desc, NullsFirst: nullsFirst}, {Column: 1, Desc: true}}
				first, err := vector.NewFlat(h.scope, h.bm, inputTypes[0], 16)
				if err != nil {
					t.Fatal(err)
				}
				if kind == vector.Constant {
					first.Release()
					first, err = vector.NewConstant(h.scope, h.bm, inputTypes[0], 16)
					if err != nil {
						t.Fatal(err)
					}
				}
				for row := 0; row < 16; row++ {
					first.SetFloat64(row, []float64{math.NaN(), 3, -2, 3, math.Copysign(0, -1), 0, math.Inf(1), math.Inf(-1)}[row%8])
				}
				first.SetNull(5)
				base := first
				if kind == vector.Dictionary {
					first, err = vector.NewDictionary(base, vector.SelectionFrom([]uint32{15, 2, 0, 5, 8, 11, 4, 1, 9, 14, 7, 12, 6, 3, 10, 13}))
					if err != nil {
						t.Fatal(err)
					}
				}
				second, err := vector.NewFlat(h.scope, h.bm, inputTypes[1], 16)
				if err != nil {
					t.Fatal(err)
				}
				for row := 0; row < 16; row++ {
					second.SetInt64(row, int64(row))
				}
				chunk, err := vector.NewDataChunk(first, second)
				if err != nil {
					t.Fatal(err)
				}
				encoded := make([][]byte, 16)
				order := make([]int, 16)
				for row := 0; row < 16; row++ {
					size, err := layout.RowSize(chunk, row)
					if err != nil {
						t.Fatal(err)
					}
					encoded[row] = make([]byte, size)
					if _, err := layout.WriteRow(chunk, row, encoded[row]); err != nil {
						t.Fatal(err)
					}
					order[row] = row
				}
				stdsort.Slice(order, func(i, j int) bool {
					cmp, err := compareRowsWith(layout, keys, encoded[order[i]], encoded[order[j]])
					if err != nil {
						t.Fatal(err)
					}
					return cmp < 0
				})
				top, err := NewTopN(TopNConfig{InputTypes: inputTypes, Keys: keys, N: 5, MemoryBudget: 1 << 20, Scope: h.scope, Buffers: h.bm, PreallocateFixed: true})
				if err != nil {
					t.Fatal(err)
				}
				if err := top.Consume(context.Background(), chunk); err != nil {
					t.Fatal(err)
				}
				chunk.Release()
				if base != first {
					base.Release()
				}
				gotRows := 0
				if err := top.Emit(context.Background(), func(out *vector.DataChunk) error {
					defer out.Release()
					for row := 0; row < out.Cardinality; row++ {
						got, valid := out.Columns[1].Int64(row)
						if !valid || gotRows >= 5 || got != int64(order[gotRows]) {
							t.Fatalf("desc=%v nullsFirst=%v kind=%v row=%d got=%d expected=%v", desc, nullsFirst, kind, gotRows, got, order[:5])
						}
						gotRows++
					}
					return nil
				}); err != nil {
					t.Fatal(err)
				}
				if gotRows != 5 {
					t.Fatalf("rows=%d", gotRows)
				}
				top.Release()
			}
		}
	}
}
