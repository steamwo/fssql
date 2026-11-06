package sort

import (
	"context"
	"math"
	stdsort "sort"
	"testing"

	"github.com/steamwo/fssql/internal/core/types"
	"github.com/steamwo/fssql/internal/core/vector"
)

// TestNormalizedKeysMatchSQLComparison 对照完整 SQL 比较，覆盖极值、NULL、多键及双向排序。
func TestNormalizedKeysMatchSQLComparison(t *testing.T) {
	decimal64, _ := types.NewDecimal(18, 2)
	decimal128, _ := types.NewDecimal(38, 6)
	for _, typ := range []types.LogicalType{{ID: types.Boolean}, {ID: types.TinyInt}, {ID: types.SmallInt}, {ID: types.Integer}, {ID: types.BigInt}, types.UBigIntType(), {ID: types.HugeInt}, decimal64, decimal128, {ID: types.DateType}, {ID: types.TimestampType}} {
		for _, desc := range []bool{false, true} {
			for _, nullsFirst := range []bool{false, true} {
				h, clean := newSortHarness(t, 8<<20)
				s, err := New(Config{InputTypes: []types.LogicalType{typ, {ID: types.BigInt}}, Keys: []Key{{Column: 0, Desc: desc, NullsFirst: nullsFirst}, {Column: 1, Desc: !desc}}, MemoryBudget: 1 << 20, Scope: h.scope, Buffers: h.bm, Temp: h.temp})
				if err != nil {
					t.Fatal(err)
				}
				const count = 259
				key, err := vector.NewFlat(h.scope, h.bm, typ, count)
				if err != nil {
					t.Fatal(err)
				}
				id, err := vector.NewFlat(h.scope, h.bm, types.LogicalType{ID: types.BigInt}, count)
				if err != nil {
					t.Fatal(err)
				}
				for i := 0; i < count; i++ {
					x := []int64{math.MinInt64, -1, 0, 1, math.MaxInt64}[i%5]
					if typ.ID == types.Boolean {
						key.SetBool(i, x != 0)
					} else if typ.ID == types.TinyInt {
						key.SetInt8(i, int8(x))
					} else if typ.ID == types.SmallInt {
						key.SetInt16(i, int16(x))
					} else if typ.ID == types.Integer || typ.ID == types.DateType {
						key.SetInt32(i, int32(x))
					} else if typ.Unsigned {
						key.SetUint64(i, uint64(x))
					} else if typ.ID == types.HugeInt || typ.ID == types.Decimal && typ.Decimal.Width > 18 {
						key.SetInt128(i, types.Int128{Lo: uint64(i % 7), Hi: x})
					} else {
						key.SetInt64(i, x)
					}
					if i%13 == 0 {
						key.SetNull(i)
					}
					id.SetInt64(i, int64(i))
				}
				chunk, err := vector.NewDataChunk(key, id)
				if err != nil {
					t.Fatal(err)
				}
				if err := s.Consume(context.Background(), chunk); err != nil {
					t.Fatal(err)
				}
				chunk.Release()
				row := s.builder.BorrowRowReader()
				want := make([]int, count)
				for i := range want {
					want[i] = i
				}
				stdsort.Slice(want, func(i, j int) bool {
					cmp, err := compareRowsWith(s.layout, s.cfg.Keys, row(want[i]), row(want[j]))
					if err != nil {
						t.Fatal(err)
					}
					return cmp < 0
				})
				idx, err := newManagedIndices(h.scope, h.bm, count)
				if err != nil {
					t.Fatal(err)
				}
				if handled, err := s.sortNormalizedIndices(idx, row); !handled || err != nil {
					t.Fatalf("type=%v handled=%v err=%v", typ, handled, err)
				}
				for i, expected := range want {
					if idx.get(i) != expected {
						t.Fatalf("type=%v desc=%v nullsFirst=%v row %d=%d want %d", typ, desc, nullsFirst, i, idx.get(i), expected)
					}
				}
				before := h.scope.Used()
				s.cfg.MemoryBudget = 1
				if handled, err := s.sortNormalizedIndices(idx, row); handled || err != nil || h.scope.Used() != before {
					t.Fatalf("budget fallback handled=%v err=%v memory=%d/%d", handled, err, h.scope.Used(), before)
				}
				idx.release()
				s.Release()
				clean()
			}
		}
	}
}
