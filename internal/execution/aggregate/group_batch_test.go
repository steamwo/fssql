package aggregate

import (
	"context"
	"testing"

	"github.com/steamwo/fssql/internal/core/types"
	"github.com/steamwo/fssql/internal/core/vector"
)

func TestHashAggregateGroupedVarcharFloatAcrossChunks(t *testing.T) {
	mm, scope, bm := aggEnv(8 << 20)
	a, err := NewHashAggregate(scope, bm, []types.LogicalType{{ID: types.Varchar}, {ID: types.Double}}, []int{0}, []Spec{{Kind: CountStar}, {Kind: Sum, Input: 1}, {Kind: Avg, Input: 1}}, 4)
	if err != nil {
		t.Fatal(err)
	}
	chunk := func(keys []string, nullKey bool, vals []float64, nullVal map[int]bool) *vector.DataChunk {
		k, err := vector.NewFlat(scope, bm, types.LogicalType{ID: types.Varchar}, len(keys))
		if err != nil {
			t.Fatal(err)
		}
		v, err := vector.NewFlat(scope, bm, types.LogicalType{ID: types.Double}, len(vals))
		if err != nil {
			k.Release()
			t.Fatal(err)
		}
		for i, key := range keys {
			if err := k.SetString(i, key); err != nil {
				t.Fatal(err)
			}
		}
		if nullKey {
			k.SetNull(len(keys) - 1)
		}
		for i, value := range vals {
			if nullVal != nil && nullVal[i] {
				v.SetNull(i)
			} else {
				v.SetFloat64(i, value)
			}
		}
		c, err := vector.NewDataChunk(k, v)
		if err != nil {
			k.Release()
			v.Release()
			t.Fatal(err)
		}
		return c
	}
	for _, c := range []*vector.DataChunk{
		chunk([]string{"a", "b", "a"}, false, []float64{1.5, 0, 2.5}, map[int]bool{1: true}),
		chunk([]string{"b", "c", "a"}, true, []float64{3, 4, 0}, map[int]bool{2: true}),
	} {
		if err := a.Consume(context.Background(), c); err != nil {
			t.Fatal(err)
		}
		c.Release()
	}
	out, err := a.Finalize(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	type result struct {
		count              int64
		sum, avg           float64
		sumValid, avgValid bool
	}
	got := map[string]result{}
	for i := 0; i < out.Cardinality; i++ {
		key, _ := out.Columns[0].String(i)
		if !out.Columns[0].IsValid(i) {
			key = "<null>"
		}
		count, _ := out.Columns[1].Int64(i)
		sum, sumValid := out.Columns[2].Float64(i)
		avg, avgValid := out.Columns[3].Float64(i)
		got[key] = result{count: count, sum: sum, avg: avg, sumValid: sumValid, avgValid: avgValid}
	}
	want := map[string]result{
		"a":      {count: 2, sum: 4, avg: 2, sumValid: true, avgValid: true},
		"b":      {count: 2, sum: 3, avg: 3, sumValid: true, avgValid: true},
		"c":      {count: 1, sum: 4, avg: 4, sumValid: true, avgValid: true},
		"<null>": {count: 1},
	}
	if len(got) != len(want) {
		t.Fatalf("groups=%v", got)
	}
	for key, expected := range want {
		if got[key] != expected {
			t.Fatalf("group %q=%+v want %+v", key, got[key], expected)
		}
	}
	out.Release()
	a.Release()
	if err := scope.Close(); err != nil {
		t.Fatal(err)
	}
	if mm.Used() != 0 {
		t.Fatalf("managed leak=%d", mm.Used())
	}
}
