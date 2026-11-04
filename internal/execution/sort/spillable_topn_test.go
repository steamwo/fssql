package sort

import (
	"context"
	"errors"
	"fmt"
	"reflect"
	stdsort "sort"
	"strings"
	"testing"

	"github.com/steamwo/fssql/internal/core/memory"
	"github.com/steamwo/fssql/internal/core/types"
	"github.com/steamwo/fssql/internal/core/vector"
	storagetemp "github.com/steamwo/fssql/internal/storage/temp"
)

type varcharTopRow struct {
	key     string
	id      int64
	payload string
	null    bool
}

func varcharTopTypes() []types.LogicalType {
	return []types.LogicalType{{ID: types.Varchar}, {ID: types.BigInt}, {ID: types.Varchar}}
}

func varcharTopChunk(t testing.TB, h *sortHarness, rows []varcharTopRow) *vector.DataChunk {
	t.Helper()
	cols := make([]*vector.Vector, 3)
	for i, typ := range varcharTopTypes() {
		v, err := vector.NewFlat(h.scope, h.bm, typ, len(rows))
		if err != nil {
			t.Fatal(err)
		}
		cols[i] = v
	}
	for i, row := range rows {
		if row.null {
			cols[0].SetNull(i)
		} else if err := cols[0].SetString(i, row.key); err != nil {
			t.Fatal(err)
		}
		cols[1].SetInt64(i, row.id)
		if err := cols[2].SetString(i, row.payload); err != nil {
			t.Fatal(err)
		}
	}
	chunk, err := vector.NewDataChunk(cols...)
	if err != nil {
		t.Fatal(err)
	}
	return chunk
}

func collectVarcharTop(t testing.TB, operator interface {
	Emit(context.Context, func(*vector.DataChunk) error) error
}) []varcharTopRow {
	t.Helper()
	var got []varcharTopRow
	err := operator.Emit(context.Background(), func(chunk *vector.DataChunk) error {
		defer chunk.Release()
		for i := 0; i < chunk.Cardinality; i++ {
			key, valid := chunk.Columns[0].String(i)
			id, _ := chunk.Columns[1].Int64(i)
			payload, _ := chunk.Columns[2].String(i)
			got = append(got, varcharTopRow{key: key, id: id, payload: payload, null: !valid})
		}
		return nil
	})
	if err != nil {
		t.Fatal(err)
	}
	return got
}

func varcharTopReference(rows []varcharTopRow, keys []Key, n int) []varcharTopRow {
	want := append([]varcharTopRow(nil), rows...)
	for i := range want {
		if want[i].null {
			want[i].key = ""
		}
	}
	stdsort.Slice(want, func(i, j int) bool {
		a, b := want[i], want[j]
		if a.null != b.null {
			return a.null == keys[0].NullsFirst
		}
		if !a.null && a.key != b.key {
			if keys[0].Desc {
				return a.key > b.key
			}
			return a.key < b.key
		}
		return a.id > b.id
	})
	if n < len(want) {
		want = want[:n]
	}
	return want
}

func TestSpillableTopNVarcharMatchesReference(t *testing.T) {
	var rows []varcharTopRow
	for i := 0; i < 1703; i++ {
		rows = append(rows, varcharTopRow{key: fmt.Sprintf("分组-%02d", i%31), id: int64(i), payload: strings.Repeat("x", 20+i%7*33), null: i%13 == 0})
	}
	for _, desc := range []bool{false, true} {
		for _, nullsFirst := range []bool{false, true} {
			for _, budget := range []int64{2048, 64 << 10} {
				t.Run(fmt.Sprintf("desc%v/nulls%v/budget%d", desc, nullsFirst, budget), func(t *testing.T) {
					h, cleanup := newSortHarness(t, 64<<20)
					defer cleanup()
					keys := []Key{{Column: 0, Desc: desc, NullsFirst: nullsFirst}, {Column: 1, Desc: true}}
					s, err := NewSpillableTopN(Config{InputTypes: varcharTopTypes(), Keys: keys, MemoryBudget: budget, SpillBlockBytes: 512, Scope: h.scope, Buffers: h.bm, Temp: h.temp}, 37)
					if err != nil {
						t.Fatal(err)
					}
					defer s.Release()
					for start := 0; start < len(rows); start += 113 {
						end := start + 113
						if end > len(rows) {
							end = len(rows)
						}
						c := varcharTopChunk(t, h, rows[start:end])
						err := s.Consume(context.Background(), c)
						c.Release()
						if err != nil {
							t.Fatal(err)
						}
					}
					if (s.external != nil) != (budget == 2048) {
						t.Fatalf("unexpected fallback=%v", s.external != nil)
					}
					got := collectVarcharTop(t, s)
					want := varcharTopReference(rows, keys, 37)
					if !reflect.DeepEqual(got, want) {
						t.Fatalf("TopN differs: got %d rows want %d", len(got), len(want))
					}
				})
			}
		}
	}
}

func TestSpillableTopNReplacementFailureKeepsFailedRowOnce(t *testing.T) {
	h, cleanup := newSortHarness(t, 64<<20)
	defer cleanup()
	keys := []Key{{Column: 0}, {Column: 1, Desc: true}}
	s, err := NewSpillableTopN(Config{InputTypes: varcharTopTypes(), Keys: keys, MemoryBudget: 512, SpillBlockBytes: 512, Scope: h.scope, Buffers: h.bm, Temp: h.temp}, 3)
	if err != nil {
		t.Fatal(err)
	}
	defer s.Release()
	rows := []varcharTopRow{{key: "z3", id: 3}, {key: "z2", id: 2}, {key: "z1", id: 1}, {key: "zz-rejected", id: 4}, {key: "a", id: 5, payload: strings.Repeat("long", 400)}, {key: "b", id: 6}, {key: "a", id: 7}}
	c := varcharTopChunk(t, h, rows)
	err = s.Consume(context.Background(), c)
	c.Release()
	if err != nil {
		t.Fatal(err)
	}
	if s.external == nil {
		t.Fatal("expected mid-chunk fallback")
	}
	if got, want := collectVarcharTop(t, s), varcharTopReference(rows, keys, 3); !reflect.DeepEqual(got, want) {
		t.Fatalf("got=%+v want=%+v", got, want)
	}
}

func TestSpillableTopNInitialFallbackAndErrors(t *testing.T) {
	for _, mode := range []string{"initial", "quota", "cancel", "consumer"} {
		t.Run(mode, func(t *testing.T) {
			quota := int64(64 << 20)
			if mode == "quota" {
				quota = 1
			}
			h, cleanup := newSortHarness(t, quota)
			defer cleanup()
			n := 5
			if mode == "initial" {
				n = 1000
			}
			s, err := NewSpillableTopN(Config{InputTypes: varcharTopTypes(), Keys: []Key{{Column: 0}, {Column: 1, Desc: true}}, MemoryBudget: 512, SpillBlockBytes: 512, Scope: h.scope, Buffers: h.bm, Temp: h.temp}, n)
			if err != nil {
				t.Fatal(err)
			}
			defer s.Release()
			if mode == "initial" && s.external == nil {
				t.Fatal("expected constructor fallback")
			}
			rows := []varcharTopRow{{key: "c", id: 1, payload: strings.Repeat("x", 100)}, {key: "a", id: 2, payload: strings.Repeat("y", 100)}, {key: "b", id: 3, payload: strings.Repeat("z", 100)}}
			if mode == "consumer" {
				rows = rows[:1]
				rows[0].payload = ""
			}
			c := varcharTopChunk(t, h, rows)
			err = s.Consume(context.Background(), c)
			c.Release()
			if mode == "quota" {
				if !errors.Is(err, storagetemp.ErrQuotaExceeded) {
					t.Fatalf("err=%v", err)
				}
				return
			}
			if err != nil {
				t.Fatal(err)
			}
			if mode == "consumer" {
				calls := 0
				err = s.Emit(context.Background(), func(*vector.DataChunk) error { calls++; return memory.ErrLimitExceeded })
				if !errors.Is(err, memory.ErrLimitExceeded) || calls != 1 || s.external != nil {
					t.Fatalf("err=%v calls=%d fallback=%v", err, calls, s.external != nil)
				}
				return
			}
			if mode == "cancel" {
				ctx, cancel := context.WithCancel(context.Background())
				cancel()
				err = s.Emit(ctx, func(*vector.DataChunk) error { return nil })
				if !errors.Is(err, context.Canceled) {
					t.Fatalf("err=%v", err)
				}
				return
			}
			if got := collectVarcharTop(t, s); len(got) != len(rows) {
				t.Fatalf("rows=%d want=%d", len(got), len(rows))
			}
		})
	}
}

func TestSpillableTopNManagedHardLimitFallback(t *testing.T) {
	for _, tc := range []struct {
		name                    string
		limit                   int64
		n, rows, batch, payload int
		emitFallback            bool
	}{
		{name: "consume", limit: 128 << 10, n: 2048, rows: 4096, batch: 128},
		{name: "emit", limit: 90 << 10, n: 64, rows: 128, batch: 8, payload: 700, emitFallback: true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			h, cleanup := newSortHarness(t, 64<<20)
			defer cleanup()
			if err := h.scope.Close(); err != nil {
				t.Fatal(err)
			}
			h.mm = memory.NewManager(tc.limit)
			h.scope = h.mm.NewScope()
			keys := []Key{{Column: 0}, {Column: 1, Desc: true}}
			s, err := NewSpillableTopN(Config{InputTypes: varcharTopTypes(), Keys: keys, MemoryBudget: 1 << 20, SpillBlockBytes: 512, Scope: h.scope, Buffers: h.bm, Temp: h.temp}, tc.n)
			if err != nil {
				t.Fatal(err)
			}
			defer func() {
				s.Release()
				if err := h.scope.Close(); err != nil {
					t.Fatal(err)
				}
				if h.mm.Used() != 0 {
					t.Fatalf("managed leak=%d", h.mm.Used())
				}
			}()
			var rows []varcharTopRow
			for i := 0; i < tc.rows; i++ {
				rows = append(rows, varcharTopRow{key: fmt.Sprintf("%04d", tc.rows-i), id: int64(i), payload: strings.Repeat("x", tc.payload)})
			}
			// Small batches keep incoming vectors bounded while the retained heap grows.
			for start := 0; start < len(rows); start += tc.batch {
				c := varcharTopChunk(t, h, rows[start:start+tc.batch])
				err = s.Consume(context.Background(), c)
				c.Release()
				if err != nil {
					t.Fatalf("batch %d: %v (managed=%d builder=%d rows=%d)", start, err, h.mm.Used(), s.external.builderBytes(), s.external.builder.Count())
				}
			}
			if (s.external == nil) != tc.emitFallback {
				t.Fatalf("unexpected pre-emit fallback=%v", s.external != nil)
			}
			if !tc.emitFallback && s.external.Stats().RunsCreated < 2 {
				t.Fatal("expected retained-prefix run and hard-cap suffix spill")
			}
			got := collectVarcharTop(t, s)
			want := varcharTopReference(rows, keys, tc.n)
			if !reflect.DeepEqual(got, want) {
				t.Fatalf("hard-cap TopN mismatch: got=%d", len(got))
			}
			if s.external == nil {
				t.Fatal("expected hard-cap fallback")
			}
		})
	}
}
