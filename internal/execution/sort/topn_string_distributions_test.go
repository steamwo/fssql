package sort

import (
	"context"
	"fmt"
	"reflect"
	"strings"
	"testing"

	"github.com/steamwo/fssql/internal/core/vector"
)

func TestVarcharTopNStringDistributions(t *testing.T) {
	const count = 8192
	for _, distribution := range []string{"high_cardinality", "long_common_prefix", "ascending", "descending", "many_ties"} {
		for _, n := range []int{1, 100, 257} {
			t.Run(fmt.Sprintf("%s/k%d", distribution, n), func(t *testing.T) {
				h, cleanup := newSortHarness(t, 16<<20)
				defer cleanup()
				keys := []Key{{Column: 0}, {Column: 1}}
				top, err := NewTopN(TopNConfig{InputTypes: varcharTopTypes(), Keys: keys, N: n, MemoryBudget: 2 << 20, Scope: h.scope, Buffers: h.bm})
				if err != nil {
					t.Fatal(err)
				}
				defer top.Release()
				var all []varcharTopRow
				for start := 0; start < count; start += vector.DefaultVectorSize {
					rows := make([]varcharTopRow, vector.DefaultVectorSize)
					for i := range rows {
						id := start + i
						value := id * 7919 % count
						switch distribution {
						case "ascending":
							value = id
						case "descending":
							value = count - id
						case "many_ties":
							value %= 19
						}
						key := fmt.Sprintf("key-%06d", value)
						if distribution == "long_common_prefix" {
							key = strings.Repeat("shared-prefix-", 32) + key
						}
						rows[i] = varcharTopRow{key: key, id: int64(id), payload: "payload", null: id%97 == 0}
					}
					all = append(all, rows...)
					chunk := varcharTopChunk(t, h, rows)
					err = top.Consume(context.Background(), chunk)
					chunk.Release()
					if err != nil {
						t.Fatal(err)
					}
				}
				if got, want := collectVarcharTop(t, top), varcharTopReference(all, keys, n); !reflect.DeepEqual(got, want) {
					t.Fatalf("TopN differs from full reference sort")
				}
			})
		}
	}
}
