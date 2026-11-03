package sort

import (
	"context"
	"strings"
	"testing"

	"github.com/steamwo/fssql/internal/core/types"
	"github.com/steamwo/fssql/internal/core/vector"
)

func TestTopNRejectsLargePayloadBeforeSerialization(t *testing.T) {
	h, clean := newSortHarness(t, 8<<20)
	defer clean()
	top, err := NewTopN(TopNConfig{InputTypes: []types.LogicalType{{ID: types.BigInt}, {ID: types.Varchar}}, Keys: []Key{{Column: 0}}, N: 1, MemoryBudget: 1024, Scope: h.scope, Buffers: h.bm})
	if err != nil {
		t.Fatal(err)
	}
	for _, tc := range []struct {
		key     int64
		payload string
	}{{0, "keep"}, {1, strings.Repeat("large", 10000)}} {
		k, err := vector.NewFlat(h.scope, h.bm, types.LogicalType{ID: types.BigInt}, 1)
		if err != nil {
			t.Fatal(err)
		}
		k.SetInt64(0, tc.key)
		p, err := vector.NewFlat(h.scope, h.bm, types.LogicalType{ID: types.Varchar}, 1)
		if err != nil {
			t.Fatal(err)
		}
		if err := p.SetString(0, tc.payload); err != nil {
			t.Fatal(err)
		}
		chunk, err := vector.NewDataChunk(k, p)
		if err != nil {
			t.Fatal(err)
		}
		if err := top.Consume(context.Background(), chunk); err != nil {
			t.Fatal(err)
		}
		chunk.Release()
	}
	if top.scratch.Len() > 1024 {
		t.Fatal("rejected payload grew sort scratch")
	}
	if err := top.Emit(context.Background(), func(c *vector.DataChunk) error {
		defer c.Release()
		if value, _ := c.Columns[1].String(0); value != "keep" {
			t.Fatalf("payload=%q", value)
		}
		return nil
	}); err != nil {
		t.Fatal(err)
	}
	top.Release()
}

func TestPrefixComparatorMatchesFullKeysIncludingNullAndNUL(t *testing.T) {
	h, clean := newSortHarness(t, 8<<20)
	defer clean()
	for _, desc := range []bool{false, true} {
		for _, nullsFirst := range []bool{false, true} {
			s, err := New(Config{InputTypes: []types.LogicalType{{ID: types.Varchar}, {ID: types.BigInt}}, Keys: []Key{{Column: 0, Desc: desc, NullsFirst: nullsFirst}, {Column: 1, Desc: true}}, MemoryBudget: 1 << 20, Scope: h.scope, Buffers: h.bm, Temp: h.temp})
			if err != nil {
				t.Fatal(err)
			}
			values := []string{"", "a", "a\x00", "a\x00b", "abcdefgh-A", "abcdefgh-B", "z", ""}
			a, err := vector.NewFlat(h.scope, h.bm, types.LogicalType{ID: types.Varchar}, len(values))
			if err != nil {
				t.Fatal(err)
			}
			b, err := vector.NewFlat(h.scope, h.bm, types.LogicalType{ID: types.BigInt}, len(values))
			if err != nil {
				t.Fatal(err)
			}
			for i, value := range values {
				if err := a.SetString(i, value); err != nil {
					t.Fatal(err)
				}
				b.SetInt64(i, int64(i))
			}
			a.SetNull(len(values) - 1)
			chunk, err := vector.NewDataChunk(a, b)
			if err != nil {
				t.Fatal(err)
			}
			if err := s.Consume(context.Background(), chunk); err != nil {
				t.Fatal(err)
			}
			chunk.Release()
			row := s.builder.BorrowRowReader()
			prefix, err := s.newStringPrefixes(row, len(values))
			if err != nil || prefix == nil {
				t.Fatalf("prefix=%v err=%v", prefix, err)
			}
			compare := s.prefixComparator(prefix.Bytes(), row)
			for i := range values {
				for j := range values {
					got, e := compare(i, j)
					want, we := s.compareRows(row(i), row(j))
					if e != nil || we != nil || (got < 0) != (want < 0) || (got > 0) != (want > 0) {
						t.Fatalf("desc=%v nullsFirst=%v pair=%d,%d got=%d want=%d", desc, nullsFirst, i, j, got, want)
					}
				}
			}
			prefix.Release()
			s.cfg.MemoryBudget = s.builderBytes() + int64(len(values))*4
			if fallback, err := s.newStringPrefixes(row, len(values)); err != nil || fallback != nil {
				if fallback != nil {
					fallback.Release()
				}
				t.Fatalf("prefix exceeded sort budget: err=%v", err)
			}
			s.Release()
		}
	}
}
