package export

import (
	"context"
	"fmt"
	"os"
	"path/filepath"
	"testing"

	"github.com/steamwo/fssql/internal/core/memory"
	"github.com/steamwo/fssql/internal/core/types"
	"github.com/steamwo/fssql/internal/core/vector"
	"github.com/steamwo/fssql/internal/storage/local"
	"github.com/steamwo/fssql/internal/storage/vfs"
)

// BenchmarkParquetLayout isolates native writer/VFS costs from text decoding.
// Input vectors have their own budget, excluded from writer-managed-peak-B.
func BenchmarkParquetLayout(b *testing.B) {
	for _, columns := range []int{4, 64} {
		b.Run(fmt.Sprint(columns), func(b *testing.B) {
			const rows = 100_000
			schema, chunks := parquetLayoutFixture(b, columns, rows)
			for _, layout := range [][2]int{{2048, 512}, {8192, 512}, {2048, 2048}, {8192, 2048}, {32768, 2048}} {
				b.Run(fmt.Sprintf("r%d_p%d", layout[0], layout[1]), func(b *testing.B) {
					ctx := context.Background()
					root := b.TempDir()
					driver, err := local.NewDriver(root)
					if err != nil {
						b.Fatal(err)
					}
					registry, err := vfs.NewRegistry(driver)
					if err != nil {
						b.Fatal(err)
					}
					bm := memory.NewBufferManager(nil)
					var peak, fileBytes int64
					b.ReportAllocs()
					b.ResetTimer()
					b.StopTimer()
					for i := 0; i < b.N; i++ {
						path := filepath.Join(root, fmt.Sprintf("%d.parquet", i))
						uri, err := local.URIFromPath(path)
						if err != nil {
							b.Fatal(err)
						}
						mm := memory.NewManager(256 << 20)
						scope := mm.NewScope()
						b.StartTimer()
						sink, err := NewAtomicVFSByteSink(ctx, registry, uri, vfs.WriteOptions{}, scope, bm)
						if err != nil {
							b.Fatal(err)
						}
						writer, err := NewParquetWriter(schema, ParquetWriteOptions{RowGroupRows: layout[0], PageRows: layout[1]}, scope, bm, sink)
						if err != nil {
							b.Fatal(err)
						}
						for _, chunk := range chunks {
							if err := writer.WriteChunk(ctx, chunk); err != nil {
								b.Fatal(err)
							}
						}
						result, err := writer.Close(ctx)
						b.StopTimer()
						if err != nil || writer.totalRows != rows || result.Bytes <= 0 || scope.Used() != 0 {
							b.Fatalf("rows=%d bytes=%d memory=%d err=%v", writer.totalRows, result.Bytes, scope.Used(), err)
						}
						fileBytes = result.Bytes
						if mm.Peak() > peak {
							peak = mm.Peak()
						}
						if err := scope.Close(); err != nil {
							b.Fatal(err)
						}
						if err := os.Remove(path); err != nil {
							b.Fatal(err)
						}
					}
					b.ReportMetric(float64(peak), "writer-managed-peak-B")
					b.ReportMetric(float64(fileBytes), "parquet-B/op")
					b.ReportMetric(float64(rows*b.N)/b.Elapsed().Seconds(), "rows/s")
				})
			}
		})
	}
}

func parquetLayoutFixture(tb testing.TB, columns, rows int) ([]Column, []*vector.DataChunk) {
	tb.Helper()
	scope := memory.NewManager(256 << 20).NewScope()
	bm := memory.NewBufferManager(nil)
	schema := make([]Column, columns)
	for col := range schema {
		typ := []types.LogicalType{{ID: types.BigInt}, {ID: types.Double}, {ID: types.Varchar}, {ID: types.Boolean}}[col%4]
		schema[col] = Column{Name: fmt.Sprintf("c%02d", col), Type: typ}
	}
	var chunks []*vector.DataChunk
	tb.Cleanup(func() {
		for _, chunk := range chunks {
			chunk.Release()
		}
		if scope.Used() != 0 {
			tb.Errorf("fixture managed leak=%d", scope.Used())
		}
		if err := scope.Close(); err != nil {
			tb.Error(err)
		}
	})
	for start := 0; start < rows; start += vector.DefaultVectorSize {
		count := rows - start
		if count > vector.DefaultVectorSize {
			count = vector.DefaultVectorSize
		}
		vectors := make([]*vector.Vector, columns)
		for col := range vectors {
			v := newVector(tb, scope, bm, schema[col].Type, count)
			vectors[col] = v
			for i := 0; i < count; i++ {
				row := start + i
				if col != 0 && (row+col)%10 == 0 {
					v.SetNull(i)
					continue
				}
				switch col % 4 {
				case 0:
					v.SetInt64(i, int64(row))
				case 1:
					v.SetFloat64(i, float64((row*17+col)%4000)/4-500)
				case 2:
					value := fmt.Sprintf("区域-%02d, \"x\"", (row+col)%32)
					if col%8 == 2 {
						value = fmt.Sprintf("acct-%02d-%08x", col, uint32(row)*2654435761)
					}
					if err := v.SetString(i, value); err != nil {
						tb.Fatal(err)
					}
				case 3:
					v.SetBool(i, (row+col)%3 == 0)
				}
			}
		}
		chunk, err := vector.NewDataChunk(vectors...)
		if err != nil {
			tb.Fatal(err)
		}
		chunks = append(chunks, chunk)
	}
	return schema, chunks
}
