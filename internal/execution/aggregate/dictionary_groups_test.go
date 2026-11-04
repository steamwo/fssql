package aggregate

import (
	"context"
	"fmt"
	"math"
	"testing"

	"github.com/steamwo/fssql/internal/core/memory"
	"github.com/steamwo/fssql/internal/core/types"
	"github.com/steamwo/fssql/internal/core/vector"
	"github.com/steamwo/fssql/internal/execution/blocking"
	"github.com/steamwo/fssql/internal/execution/expression"
	"github.com/steamwo/fssql/internal/execution/pipeline"
	"github.com/steamwo/fssql/internal/execution/sqltransform"
	"github.com/steamwo/fssql/internal/sql/binder"
)

func dictionaryGroupChunk(t testing.TB, scope *memory.Scope, bm *memory.BufferManager, labels []string, nullEntry, rows, offset int, blob bool) *vector.DataChunk {
	t.Helper()
	keyType := types.LogicalType{ID: types.Varchar, Binary: blob}
	child, err := vector.NewFlat(scope, bm, keyType, len(labels))
	if err != nil {
		t.Fatal(err)
	}
	for i, label := range labels {
		if i == nullEntry {
			child.SetNull(i)
		} else if err := child.SetStringBytes(i, []byte(label)); err != nil {
			child.Release()
			t.Fatal(err)
		}
	}
	indices := make([]int, rows)
	values, err := vector.NewFlat(scope, bm, types.LogicalType{ID: types.BigInt}, rows)
	if err != nil {
		child.Release()
		t.Fatal(err)
	}
	for row := range indices {
		indices[row] = row % len(labels)
		values.SetInt64(row, int64(offset+row+1))
	}
	keys, err := vector.NewOwnedDictionary(scope, child, vector.SelectionFromRows(indices))
	if err != nil {
		child.Release()
		values.Release()
		t.Fatal(err)
	}
	chunk, err := vector.NewDataChunk(keys, values)
	if err != nil {
		keys.Release()
		values.Release()
		t.Fatal(err)
	}
	return chunk
}

type dictionaryGroupResult struct {
	count int64
	sum   types.Int128
}

func readDictionaryGroups(t testing.TB, emit func(func(*vector.DataChunk) error) error) map[string]dictionaryGroupResult {
	t.Helper()
	got := make(map[string]dictionaryGroupResult)
	err := emit(func(chunk *vector.DataChunk) error {
		defer chunk.Release()
		for row := 0; row < chunk.Cardinality; row++ {
			key, valid := chunk.Columns[0].String(row)
			if !valid {
				key = "<NULL>"
			}
			if _, exists := got[key]; exists {
				t.Errorf("duplicate group %q", key)
			}
			count, _ := chunk.Columns[1].Int64(row)
			sum, _ := chunk.Columns[2].Int128(row)
			got[key] = dictionaryGroupResult{count, sum}
		}
		return nil
	})
	if err != nil {
		t.Fatal(err)
	}
	return got
}

func TestDictionaryGroupsOnlyReferencedEntriesThroughOwnedProjectAndFilter(t *testing.T) {
	for _, blob := range []bool{false, true} {
		t.Run(fmt.Sprintf("blob%v", blob), func(t *testing.T) {
			mm, scope, bm := aggEnv(4 << 20)
			keyType := types.LogicalType{ID: types.Varchar, Binary: blob}
			a, err := NewHashAggregate(scope, bm, []types.LogicalType{keyType, {ID: types.BigInt}}, []int{0}, []Spec{{Kind: CountStar}, {Kind: Sum, Input: 1}}, 8)
			if err != nil {
				t.Fatal(err)
			}
			defer a.Release()
			executor := expression.NewExecutor(scope, bm)
			project, err := sqltransform.NewProject(executor, []*binder.BoundExpression{{Kind: binder.BoundColumn, ColumnIndex: 0, Type: keyType}, {Kind: binder.BoundColumn, ColumnIndex: 1, Type: types.LogicalType{ID: types.BigInt}}})
			if err != nil {
				t.Fatal(err)
			}
			// The filter excludes every "unreferenced" entry, while preserving
			// a nested owned dictionary over a 1024-row child.
			filter, err := sqltransform.NewFilter(executor, &binder.BoundExpression{Kind: binder.BoundBinary, Op: "!=", Type: types.LogicalType{ID: types.Boolean}, Children: []*binder.BoundExpression{{Kind: binder.BoundColumn, ColumnIndex: 0, Type: keyType}, {Kind: binder.BoundLiteral, Literal: "unreferenced", Type: keyType}}})
			if err != nil {
				t.Fatal(err)
			}
			want := make(map[string]dictionaryGroupResult)
			for batch, labels := range [][]string{{"a\x00", "b", "a\x00", "unreferenced", "c"}, {"b", "a\x00", "unreferenced", "c", "a\x00"}} {
				input := dictionaryGroupChunk(t, scope, bm, labels, -1, 1024, batch*10000, blob)
				out, err := project.TransformOwned(context.Background(), input)
				input.Release()
				if err != nil {
					t.Fatal(err)
				}
				if out.Columns[0].Kind != vector.Dictionary {
					t.Fatal("pre-aggregate project flattened dictionary")
				}
				selected, err := filter.TransformOwned(context.Background(), out)
				out.Release()
				if err != nil {
					t.Fatal(err)
				}
				for row := 0; row < selected.Cardinality; row++ {
					key, _ := selected.Columns[0].String(row)
					v, _ := selected.Columns[1].Int64(row)
					result := want[key]
					result.count++
					result.sum, _ = types.AddInt128(result.sum, types.Int128FromInt64(v))
					want[key] = result
				}
				var ids [vector.DefaultVectorSize]int
				handled, err := a.findDictionaryGroups(context.Background(), a.projected(selected), ids[:selected.Cardinality])
				if err != nil || !handled {
					selected.Release()
					t.Fatalf("dictionary fast path handled=%v err=%v", handled, err)
				}
				if err := a.Consume(context.Background(), selected); err != nil {
					selected.Release()
					t.Fatal(err)
				}
				selected.Release()
			}
			got := readDictionaryGroups(t, func(emit func(*vector.DataChunk) error) error { return a.Emit(context.Background(), emit) })
			if len(got) != len(want) {
				t.Fatalf("groups=%v want=%v", got, want)
			}
			for key, expected := range want {
				if got[key] != expected {
					t.Fatalf("group %q=%+v want=%+v", key, got[key], expected)
				}
			}
			a.Release()
			if err := scope.Close(); err != nil {
				t.Fatal(err)
			}
			if mm.Used() != 0 {
				t.Fatalf("managed leak=%d", mm.Used())
			}
		})
	}
}

func TestDictionaryGroupsNullDuplicatesAndGeneralFallback(t *testing.T) {
	for _, size := range []int{5, 1000} {
		t.Run(fmt.Sprint(size), func(t *testing.T) {
			mm, scope, bm := aggEnv(8 << 20)
			labels := make([]string, size)
			for i := range labels {
				labels[i] = fmt.Sprint(i)
			}
			labels[1] = labels[0]
			a, err := NewHashAggregate(scope, bm, []types.LogicalType{{ID: types.Varchar}, {ID: types.BigInt}}, []int{0}, []Spec{{Kind: CountStar}, {Kind: Sum, Input: 1}}, 8)
			if err != nil {
				t.Fatal(err)
			}
			defer a.Release()
			chunk := dictionaryGroupChunk(t, scope, bm, labels, 2, 2048, 0, false)
			var ids [vector.DefaultVectorSize]int
			handled, err := a.findDictionaryGroups(context.Background(), a.projected(chunk), ids[:chunk.Cardinality])
			if err != nil || handled != (size <= maxGroupDictionaryEntries) {
				t.Fatalf("size=%d handled=%v err=%v", size, handled, err)
			}
			want := make(map[string]dictionaryGroupResult)
			for row := 0; row < chunk.Cardinality; row++ {
				key, valid := chunk.Columns[0].String(row)
				if !valid {
					key = "<NULL>"
				}
				result := want[key]
				result.count++
				result.sum, _ = types.AddInt128(result.sum, types.Int128FromInt64(int64(row+1)))
				want[key] = result
			}
			if err := a.Consume(context.Background(), chunk); err != nil {
				t.Fatal(err)
			}
			chunk.Release()
			got := readDictionaryGroups(t, func(emit func(*vector.DataChunk) error) error { return a.Emit(context.Background(), emit) })
			if len(got) != len(want) {
				t.Fatalf("groups=%d want=%d", len(got), len(want))
			}
			for key, expected := range want {
				if got[key] != expected {
					t.Fatalf("group %q=%+v want=%+v", key, got[key], expected)
				}
			}
			a.Release()
			if err := scope.Close(); err != nil {
				t.Fatal(err)
			}
			if mm.Used() != 0 {
				t.Fatalf("managed leak=%d", mm.Used())
			}
		})
	}
}

func TestExternalDictionaryGroupsConcurrentSpill(t *testing.T) {
	h, cleanup := newExtAggHarness(t, 4<<20, 8<<20)
	defer cleanup()
	e, err := NewExternalAggregate(ExternalConfig{InputTypes: []types.LogicalType{{ID: types.Varchar}, {ID: types.BigInt}}, GroupColumns: []int{0}, Specs: []Spec{{Kind: CountStar}, {Kind: Sum, Input: 1}}, EstimatedGroups: 8, MemoryBudget: 1024, RadixBits: 2, MaxDepth: 8, SpillBlockBytes: 512, Scope: h.scope, Buffers: h.bm, Temp: h.temp})
	if err != nil {
		t.Fatal(err)
	}
	bridge, err := blocking.New(e, 1)
	if err != nil {
		e.Release()
		t.Fatal(err)
	}
	defer bridge.Close()
	var chunks []*vector.DataChunk
	want := make(map[string]dictionaryGroupResult)
	for batch := 0; batch < 16; batch++ {
		labels := []string{"a", "b", "a", "c", "NULL"}
		if batch%2 != 0 {
			labels = []string{"b", "a", "c", "a", "NULL"}
		}
		chunk := dictionaryGroupChunk(t, h.scope, h.bm, labels, 4, 128, batch*128, false)
		for row := 0; row < chunk.Cardinality; row++ {
			key, valid := chunk.Columns[0].String(row)
			if !valid {
				key = "<NULL>"
			}
			v, _ := chunk.Columns[1].Int64(row)
			result := want[key]
			result.count++
			result.sum, _ = types.AddInt128(result.sum, types.Int128FromInt64(v))
			want[key] = result
		}
		chunks = append(chunks, chunk)
	}
	root := &pipeline.Pipeline{Source: &aggregateChunkSource{chunks: chunks}, Sink: bridge}
	if err := pipeline.RunDAG(context.Background(), root, 4, nil); err != nil {
		t.Fatal(err)
	}
	got := readDictionaryGroups(t, func(emit func(*vector.DataChunk) error) error { return e.Emit(context.Background(), emit) })
	if e.Stats().SpillPasses == 0 {
		t.Fatal("expected concurrent dictionary states to spill")
	}
	if len(got) != len(want) {
		t.Fatalf("groups=%d want=%d", len(got), len(want))
	}
	for key, expected := range want {
		if got[key] != expected {
			t.Fatalf("group %q=%+v want=%+v", key, got[key], expected)
		}
	}
}

func TestDictionaryGroupsFloatSemanticEquality(t *testing.T) {
	mm, scope, bm := aggEnv(1 << 20)
	child, err := vector.NewFlat(scope, bm, types.LogicalType{ID: types.Double}, 6)
	if err != nil {
		t.Fatal(err)
	}
	values := []float64{math.NaN(), math.Float64frombits(0x7ff0000000000001), 0, math.Copysign(0, -1), 42, 99}
	for i, value := range values {
		child.SetFloat64(i, value)
	}
	indices := make([]int, 512)
	want := map[string]int64{}
	for row := range indices {
		indices[row] = row % 5 // 99 is never referenced
		key := fmt.Sprint(values[indices[row]])
		if math.IsNaN(values[indices[row]]) {
			key = "NaN"
		} else if values[indices[row]] == 0 {
			key = "0"
		}
		want[key]++
	}
	keys, err := vector.NewOwnedDictionary(scope, child, vector.SelectionFromRows(indices))
	if err != nil {
		child.Release()
		t.Fatal(err)
	}
	chunk, err := vector.NewDataChunk(keys)
	if err != nil {
		keys.Release()
		t.Fatal(err)
	}
	a, err := NewHashAggregate(scope, bm, []types.LogicalType{{ID: types.Double}}, []int{0}, []Spec{{Kind: CountStar}}, 8)
	if err != nil {
		chunk.Release()
		t.Fatal(err)
	}
	if err := a.Consume(context.Background(), chunk); err != nil {
		chunk.Release()
		a.Release()
		t.Fatal(err)
	}
	chunk.Release()
	got := map[string]int64{}
	if err := a.Emit(context.Background(), func(out *vector.DataChunk) error {
		defer out.Release()
		for row := 0; row < out.Cardinality; row++ {
			value, _ := out.Columns[0].Float64(row)
			key := fmt.Sprint(value)
			if math.IsNaN(value) {
				key = "NaN"
			} else if value == 0 {
				key = "0"
			}
			if _, exists := got[key]; exists {
				t.Errorf("duplicate semantic group %q", key)
			}
			got[key], _ = out.Columns[1].Int64(row)
		}
		return nil
	}); err != nil {
		a.Release()
		t.Fatal(err)
	}
	a.Release()
	if len(got) != len(want) {
		t.Fatalf("groups=%v want=%v", got, want)
	}
	for key, count := range want {
		if got[key] != count {
			t.Fatalf("group %s count=%d want=%d", key, got[key], count)
		}
	}
	if err := scope.Close(); err != nil {
		t.Fatal(err)
	}
	if mm.Used() != 0 {
		t.Fatalf("managed leak=%d", mm.Used())
	}
}

func BenchmarkHashAggregateDictionaryGroups(b *testing.B) {
	for _, groups := range []int{5, 256, 1000} {
		for _, dictionary := range []bool{false, true} {
			b.Run(fmt.Sprintf("groups%d/dictionary%v", groups, dictionary), func(b *testing.B) {
				mm, scope, bm := aggEnv(16 << 20)
				labels := make([]string, groups)
				for i := range labels {
					labels[i] = fmt.Sprintf("group-%04d", i)
				}
				chunk := dictionaryGroupChunk(b, scope, bm, labels, -1, vector.DefaultVectorSize, 0, false)
				if !dictionary {
					rows := make([]int, chunk.Cardinality)
					for i := range rows {
						rows[i] = i
					}
					flat, err := vector.CopySelection(scope, bm, chunk.Columns[0], rows)
					if err != nil {
						b.Fatal(err)
					}
					chunk.Columns[0].Release()
					chunk.Columns[0] = flat
				}
				defer func() {
					chunk.Release()
					if err := scope.Close(); err != nil {
						b.Fatal(err)
					}
					if mm.Used() != 0 {
						b.Fatal("managed leak")
					}
				}()
				a, err := NewHashAggregate(scope, bm, []types.LogicalType{{ID: types.Varchar}, {ID: types.BigInt}}, []int{0}, []Spec{{Kind: CountStar}, {Kind: Sum, Input: 1}}, groups)
				if err != nil {
					b.Fatal(err)
				}
				defer a.Release()
				// Prewarm group insertion to isolate repeated batch lookup/update.
				if err := a.Consume(context.Background(), chunk); err != nil {
					b.Fatal(err)
				}
				b.ReportAllocs()
				b.SetBytes(vector.DefaultVectorSize * int64(10+8))
				b.ResetTimer()
				for i := 0; i < b.N; i++ {
					if err := a.Consume(context.Background(), chunk); err != nil {
						b.Fatal(err)
					}
				}
				b.StopTimer()
			})
		}
	}
}
