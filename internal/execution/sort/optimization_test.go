package sort

import (
	"context"
	"errors"
	"fmt"
	"math/rand"
	stdsort "sort"
	"testing"

	"github.com/steamwo/fssql/internal/core/types"
	"github.com/steamwo/fssql/internal/core/vector"
)

func TestManagedIndicesMatchesReferenceAndPropagatesErrors(t *testing.T) {
	h, clean := newSortHarness(t, 32<<20)
	defer clean()
	for _, n := range []int{0, 1, 15, 16, 17, 257, 4096} {
		values := make([]int, n)
		rng := rand.New(rand.NewSource(17))
		for i := range values {
			values[i] = rng.Intn(31)
		}
		idx, err := newManagedIndices(h.scope, h.bm, n)
		if err != nil {
			t.Fatal(err)
		}
		if err := sortManagedIndices(idx, func(a, b int) (int, error) { return values[a] - values[b], nil }); err != nil {
			t.Fatal(err)
		}
		want := append([]int(nil), values...)
		stdsort.Ints(want)
		for i, x := range want {
			if got := values[idx.get(i)]; got != x {
				t.Fatalf("n=%d row=%d value=%d want=%d", n, i, got, x)
			}
		}
		idx.release()
	}
	idx, err := newManagedIndices(h.scope, h.bm, 20)
	if err != nil {
		t.Fatal(err)
	}
	defer idx.release()
	want := errors.New("comparison failed")
	if err := sortManagedIndices(idx, func(int, int) (int, error) { return 0, want }); !errors.Is(err, want) {
		t.Fatalf("err=%v", err)
	}
}

func TestStringSortOptimizationsPreserveFullKeysAcrossSpillAndBatches(t *testing.T) {
	type row struct {
		a, b  string
		valid bool
	}
	var expected []row
	for i := 0; i < 4097; i++ {
		a := fmt.Sprintf("long-common-prefix-%03d", (i*17)%31)
		if i%19 == 0 {
			a = ""
		}
		expected = append(expected, row{a, fmt.Sprintf("second-%05d", 4096-i), i%19 != 0})
	}
	less := func(a, b row) bool {
		if a.valid != b.valid {
			return !a.valid
		}
		if a.valid && a.a != b.a {
			return a.a < b.a
		}
		return a.b > b.b
	}
	stdsort.Slice(expected, func(i, j int) bool { return less(expected[i], expected[j]) })
	for _, budget := range []int64{4 << 10, 64 << 20} {
		h, clean := newSortHarness(t, 64<<20)
		s, err := New(Config{InputTypes: []types.LogicalType{{ID: types.Varchar}, {ID: types.Varchar}}, Keys: []Key{{Column: 0, NullsFirst: true}, {Column: 1, Desc: true}}, MemoryBudget: budget, SpillBlockBytes: 1024, Scope: h.scope, Buffers: h.bm, Temp: h.temp})
		if err != nil {
			t.Fatal(err)
		}
		for base := 0; base < 4097; base += 256 {
			n := min(256, 4097-base)
			a, err := vector.NewFlat(h.scope, h.bm, types.LogicalType{ID: types.Varchar}, n)
			if err != nil {
				t.Fatal(err)
			}
			b, err := vector.NewFlat(h.scope, h.bm, types.LogicalType{ID: types.Varchar}, n)
			if err != nil {
				t.Fatal(err)
			}
			for j := 0; j < n; j++ {
				i := base + j
				if i%19 == 0 {
					a.SetNull(j)
				} else if err := a.SetString(j, fmt.Sprintf("long-common-prefix-%03d", (i*17)%31)); err != nil {
					t.Fatal(err)
				}
				if err := b.SetString(j, fmt.Sprintf("second-%05d", 4096-i)); err != nil {
					t.Fatal(err)
				}
			}
			chunk, err := vector.NewDataChunk(a, b)
			if err != nil {
				t.Fatal(err)
			}
			if err := s.Consume(context.Background(), chunk); err != nil {
				t.Fatal(err)
			}
			chunk.Release()
		}
		seen := 0
		if err := s.Emit(context.Background(), func(c *vector.DataChunk) error {
			defer c.Release()
			for i := 0; i < c.Cardinality; i++ {
				a, valid := c.Columns[0].String(i)
				b, _ := c.Columns[1].String(i)
				got := row{a, b, valid}
				if got != expected[seen] {
					t.Fatalf("budget=%d row=%d got=%+v want=%+v", budget, seen, got, expected[seen])
				}
				seen++
			}
			return nil
		}); err != nil {
			t.Fatal(err)
		}
		if seen != len(expected) {
			t.Fatalf("rows=%d", seen)
		}
		s.Release()
		clean()
	}
}
