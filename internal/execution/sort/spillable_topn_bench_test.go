package sort

import (
	"context"
	"fmt"
	"testing"

	"github.com/steamwo/fssql/internal/core/vector"
)

// Both implementations consume the same prebuilt VARCHAR batches. This
// isolates sort/TopN execution and excludes fixture formatting/allocation.
func BenchmarkVarcharTopNAgainstExternalSort(b *testing.B) {
	const rows, n = 32768, 100
	for _, order := range []string{"ascending", "permuted"} {
		for _, implementation := range []string{"topn", "full_sort"} {
			b.Run(order+"/"+implementation, func(b *testing.B) {
				h, cleanup := newSortHarness(b, 64<<20)
				defer cleanup()
				var chunks []*vector.DataChunk
				for start := 0; start < rows; start += vector.DefaultVectorSize {
					batch := make([]varcharTopRow, vector.DefaultVectorSize)
					for i := range batch {
						key := start + i
						if order == "permuted" {
							key = key * 7919 % rows
						}
						batch[i] = varcharTopRow{key: fmt.Sprintf("key-%06d", key), id: int64(key), payload: "payload"}
					}
					chunks = append(chunks, varcharTopChunk(b, h, batch))
				}
				defer func() {
					for _, chunk := range chunks {
						chunk.Release()
					}
				}()
				cfg := Config{InputTypes: varcharTopTypes(), Keys: []Key{{Column: 0}, {Column: 1, Desc: true}}, MemoryBudget: 16 << 20, SpillBlockBytes: 4096, Scope: h.scope, Buffers: h.bm, Temp: h.temp}
				b.ReportAllocs()
				b.SetBytes(rows * int64(10+8+7))
				b.ResetTimer()
				for i := 0; i < b.N; i++ {
					var op interface {
						Consume(context.Context, *vector.DataChunk) error
						Emit(context.Context, func(*vector.DataChunk) error) error
						Release()
					}
					var err error
					if implementation == "topn" {
						op, err = NewSpillableTopN(cfg, n)
					} else {
						var full *ExternalSort
						full, err = New(cfg)
						if err == nil {
							full.emitLimit = n
						}
						op = full
					}
					if err != nil {
						b.Fatal(err)
					}
					for _, chunk := range chunks {
						if err := op.Consume(context.Background(), chunk); err != nil {
							op.Release()
							b.Fatal(err)
						}
					}
					count := 0
					err = op.Emit(context.Background(), func(chunk *vector.DataChunk) error { count += chunk.Cardinality; chunk.Release(); return nil })
					op.Release()
					if err != nil {
						b.Fatal(err)
					}
					if count != n {
						b.Fatalf("rows=%d want=%d", count, n)
					}
				}
				b.StopTimer()
			})
		}
	}
}
