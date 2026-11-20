package sort

import (
	"context"
	"errors"
	"sort"
	"testing"

	"github.com/steamwo/fssql/internal/core/memory"
	"github.com/steamwo/fssql/internal/core/types"
	"github.com/steamwo/fssql/internal/core/vector"
)

type packedRow struct {
	id    int64
	value float64
}

func collectPackedTopN(t testing.TB, top *PackedTopN) []packedRow {
	t.Helper()
	var out []packedRow
	if err := top.Emit(context.Background(), func(chunk *vector.DataChunk) error {
		for i := 0; i < chunk.Cardinality; i++ {
			id, _ := chunk.Columns[0].Int64(i)
			value, _ := chunk.Columns[1].Float64(i)
			out = append(out, packedRow{id: id, value: value})
		}
		chunk.Release()
		return nil
	}); err != nil {
		t.Fatal(err)
	}
	return out
}

// TestPackedTopNEqualsFullSortLimit 验证紧凑行区 TopN 与完整排序前 N 行一致。
func TestPackedTopNEqualsFullSortLimit(t *testing.T) {
	h, clean := newSortHarness(t, 1<<20)
	defer clean()
	top, err := NewPackedTopN(PackedTopNConfig{
		InputTypes:   []types.LogicalType{{ID: types.BigInt}, {ID: types.Double}},
		Keys:         []Key{{Column: 1}, {Column: 0}},
		N:            37,
		MemoryBudget: 64 << 10,
		Scope:        h.scope,
		Buffers:      h.bm,
	})
	if err != nil {
		t.Fatal(err)
	}
	for base := 0; base < 1000; base += 125 {
		chunk := makeFixedTopNChunk(t, h, base, 125)
		if err := top.Consume(context.Background(), chunk); err != nil {
			chunk.Release()
			t.Fatal(err)
		}
		chunk.Release()
	}
	got := collectPackedTopN(t, top)
	want := make([]packedRow, 0, 1000)
	for i := 0; i < 1000; i++ {
		want = append(want, packedRow{id: int64(i), value: float64(i % 97)})
	}
	sort.Slice(want, func(i, j int) bool {
		if want[i].value != want[j].value {
			return want[i].value < want[j].value
		}
		return want[i].id < want[j].id
	})
	want = want[:37]
	if len(got) != len(want) {
		t.Fatalf("len=%d want=%d", len(got), len(want))
	}
	for i := range want {
		if got[i] != want[i] {
			t.Fatalf("row %d=%+v want %+v", i, got[i], want[i])
		}
	}
	top.Release()
	if h.scope.Used() != 0 {
		t.Fatalf("managed leak=%d", h.scope.Used())
	}
}

func TestPackedTopNMonotoneBatchKeepsBestRows(t *testing.T) {
	for _, tc := range []struct {
		name    string
		desc    bool
		outlier bool
		want    []int64
	}{
		{name: "descending sort", desc: true, want: []int64{59, 58, 57, 56, 55}},
		{name: "ascending sort", want: []int64{-59, -58, -57, -56, -55}},
		{name: "out of order first row", desc: true, outlier: true, want: []int64{1000, 59, 58, 57, 56}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			h, clean := newSortHarness(t, 1<<20)
			defer clean()
			top, err := NewPackedTopN(PackedTopNConfig{
				InputTypes: []types.LogicalType{{ID: types.BigInt}, {ID: types.Double}},
				Keys:       []Key{{Column: 0, Desc: tc.desc}, {Column: 1}},
				N:          len(tc.want), MemoryBudget: 64 << 10, Scope: h.scope, Buffers: h.bm,
			})
			if err != nil {
				t.Fatal(err)
			}
			defer top.Release()
			for base := 0; base < 160; base += 80 {
				chunk := makeFixedTopNChunk(t, h, base, 80)
				for row := 0; row < chunk.Cardinality; row++ {
					value := int64(base + row - 100)
					if !tc.desc {
						value = -value
					}
					if tc.outlier && base == 0 && row == 0 {
						value = 1000
					}
					chunk.Columns[0].SetInt64(row, value)
				}
				err := top.Consume(context.Background(), chunk)
				chunk.Release()
				if err != nil {
					t.Fatal(err)
				}
			}
			got := collectPackedTopN(t, top)
			if len(got) != len(tc.want) {
				t.Fatalf("rows=%d want=%d", len(got), len(tc.want))
			}
			for i, want := range tc.want {
				if got[i].id != want {
					t.Fatalf("row %d id=%d want=%d", i, got[i].id, want)
				}
			}
		})
	}
}

// TestPackedTopNPreallocatesAndRejectsSmallBudget 验证构造阶段预留全部状态，预算不足时无泄漏地失败。
func TestPackedTopNPreallocatesAndRejectsSmallBudget(t *testing.T) {
	h, clean := newSortHarness(t, 1<<20)
	defer clean()
	top, err := NewPackedTopN(PackedTopNConfig{
		InputTypes:   []types.LogicalType{{ID: types.BigInt}, {ID: types.Double}},
		Keys:         []Key{{Column: 0}},
		N:            100,
		MemoryBudget: 16 << 10,
		Scope:        h.scope,
		Buffers:      h.bm,
	})
	if err != nil {
		t.Fatal(err)
	}
	used := h.scope.Used()
	for base := 0; base < 1000; base += 125 {
		chunk := makeFixedTopNChunk(t, h, base, 125)
		if err := top.Consume(context.Background(), chunk); err != nil {
			chunk.Release()
			t.Fatal(err)
		}
		chunk.Release()
	}
	if h.scope.Used() != used {
		t.Fatalf("consume changed footprint: before=%d after=%d", used, h.scope.Used())
	}
	top.Release()

	mm := memory.NewManager(1 << 20)
	scope := mm.NewScope()
	buffers := memory.NewBufferManager(nil)
	_, err = NewPackedTopN(PackedTopNConfig{
		InputTypes:   []types.LogicalType{{ID: types.BigInt}, {ID: types.Double}},
		Keys:         []Key{{Column: 0}},
		N:            1000,
		MemoryBudget: 1024,
		Scope:        scope,
		Buffers:      buffers,
	})
	if !errors.Is(err, memory.ErrLimitExceeded) {
		t.Fatalf("err=%v", err)
	}
	if scope.Used() != 0 {
		t.Fatalf("managed leak=%d", scope.Used())
	}
	if err := scope.Close(); err != nil {
		t.Fatal(err)
	}
}

// TestPackedTopNZeroAndMoreThanRows 验证 N=0 与 N 大于输入行数。
func TestPackedTopNZeroAndMoreThanRows(t *testing.T) {
	h, clean := newSortHarness(t, 1<<20)
	defer clean()
	makeTop := func(n int) *PackedTopN {
		top, err := NewPackedTopN(PackedTopNConfig{
			InputTypes:   []types.LogicalType{{ID: types.BigInt}, {ID: types.Double}},
			Keys:         []Key{{Column: 0}},
			N:            n,
			MemoryBudget: 64 << 10,
			Scope:        h.scope,
			Buffers:      h.bm,
		})
		if err != nil {
			t.Fatal(err)
		}
		return top
	}
	zero := makeTop(0)
	chunk := makeFixedTopNChunk(t, h, 0, 10)
	if err := zero.Consume(context.Background(), chunk); err != nil {
		chunk.Release()
		t.Fatal(err)
	}
	chunk.Release()
	if got := collectPackedTopN(t, zero); len(got) != 0 {
		t.Fatalf("zero TopN len=%d", len(got))
	}
	zero.Release()

	more := makeTop(20)
	chunk = makeFixedTopNChunk(t, h, 0, 10)
	if err := more.Consume(context.Background(), chunk); err != nil {
		chunk.Release()
		t.Fatal(err)
	}
	chunk.Release()
	got := collectPackedTopN(t, more)
	if len(got) != 10 {
		t.Fatalf("len=%d", len(got))
	}
	for i := 1; i < len(got); i++ {
		if got[i-1].id > got[i].id {
			t.Fatalf("not sorted at %d", i)
		}
	}
	more.Release()
	if h.scope.Used() != 0 {
		t.Fatalf("managed leak=%d", h.scope.Used())
	}
}
