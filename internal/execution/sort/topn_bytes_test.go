package sort

import (
	"context"
	"fmt"
	"reflect"
	"strings"
	"testing"

	"github.com/steamwo/fssql/internal/core/vector"
)

func TestTopNBorrowedStringThreshold(t *testing.T) {
	for _, kind := range []string{"flat", "constant", "dictionary"} {
		for _, desc := range []bool{false, true} {
			for _, nullsFirst := range []bool{false, true} {
				t.Run(fmt.Sprintf("%s/desc%v/nulls%v", kind, desc, nullsFirst), func(t *testing.T) {
					h, cleanup := newSortHarness(t, 8<<20)
					defer cleanup()
					keys := []Key{{Column: 0, Desc: desc, NullsFirst: nullsFirst}, {Column: 1, Desc: true}}
					top, err := NewTopN(TopNConfig{InputTypes: varcharTopTypes(), Keys: keys, N: 37, MemoryBudget: 64 << 10, Scope: h.scope, Buffers: h.bm})
					if err != nil {
						t.Fatal(err)
					}
					defer top.Release()
					var all []varcharTopRow
					for batch := 0; batch < 3; batch++ {
						rows := make([]varcharTopRow, 97)
						for i := range rows {
							rows[i] = varcharTopRow{key: []string{"", "a", "中文", strings.Repeat("long", 20)}[(i+batch)%4], id: int64(batch*97 + i), payload: "payload", null: i%11 == 0}
						}
						chunk := varcharTopChunk(t, h, rows)
						base := chunk.Columns[0]
						var dictionary *vector.Vector
						if kind == "constant" {
							constant, err := vector.NewConstant(h.scope, h.bm, base.Type, len(rows))
							if err != nil {
								t.Fatal(err)
							}
							if err := constant.SetString(0, strings.Repeat("constant", 4)); err != nil {
								t.Fatal(err)
							}
							if batch == 1 {
								constant.SetNull(0)
							}
							chunk.Columns[0] = constant
						} else if kind == "dictionary" {
							indices := make([]uint32, len(rows))
							for i := range indices {
								indices[i] = uint32((i * 13) % len(rows))
							}
							dictionary, err = vector.NewDictionary(base, vector.SelectionFrom(indices))
							if err != nil {
								t.Fatal(err)
							}
							chunk.Columns[0], err = vector.NewDictionary(dictionary, vector.SelectionFrom(indices))
							if err != nil {
								t.Fatal(err)
							}
						}
						for i := range rows {
							key, valid := chunk.Columns[0].String(i)
							rows[i].key, rows[i].null = key, !valid
						}
						all = append(all, rows...)
						err = top.Consume(context.Background(), chunk)
						chunk.Release()
						if kind != "flat" {
							base.Release()
							if dictionary != nil {
								dictionary.Release()
							}
						}
						if err != nil {
							t.Fatal(err)
						}
					}
					got := collectVarcharTop(t, top)
					want := varcharTopReference(all, keys, 37)
					if !reflect.DeepEqual(got, want) {
						t.Fatalf("got %v want %v", got, want)
					}
				})
			}
		}
	}
}
