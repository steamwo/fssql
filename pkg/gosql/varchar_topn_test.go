package gosql

import (
	"context"
	"fmt"
	"io"
	"sort"
	"testing"
)

func TestVarcharTopNOrderOffsetAcrossChunks(t *testing.T) {
	const rows, offset, count = 8193, 123, 5000
	type item struct {
		name string
		id   int64
	}
	want := make([]item, rows)
	for i := range want {
		want[i] = item{name: fmt.Sprintf("分类-%03d", i%97), id: int64(i)}
	}
	sort.Slice(want, func(i, j int) bool {
		if want[i].name != want[j].name {
			return want[i].name > want[j].name
		}
		return want[i].id < want[j].id
	})
	want = want[offset : offset+count]
	fn := TableFunction{Name: "varchar_topn_rows", Columns: []Column{{Name: "name", Type: Type{ID: "VARCHAR"}}, {Name: "id", Type: Type{ID: "BIGINT"}}}, Open: func(context.Context) (BatchReader, error) {
		next := 0
		return &batchReaderFunc{next: func(_ context.Context, w BatchWriter) (int, error) {
			if next == rows {
				return 0, io.EOF
			}
			n := 113
			if rows-next < n {
				n = rows - next
			}
			for row := 0; row < n; row++ {
				i := next + row
				if err := w.SetString(row, 0, fmt.Sprintf("分类-%03d", i%97)); err != nil {
					return 0, err
				}
				if err := w.SetInt64(row, 1, int64(i)); err != nil {
					return 0, err
				}
			}
			next += n
			return n, nil
		}}, nil
	}}
	for _, budget := range []int64{4 << 10, 2 << 20} {
		t.Run(fmt.Sprintf("budget%d", budget), func(t *testing.T) {
			db, err := Open(Config{MemoryLimitBytes: 16 << 20, TempLimitBytes: 32 << 20, SpillMemoryBudgetBytes: budget, SpillBlockBytes: 1024, Workers: 4, TempDir: t.TempDir(), ManifestDir: t.TempDir(), TableFunctions: []TableFunction{fn}})
			if err != nil {
				t.Fatal(err)
			}
			defer db.Close()
			next, chunks := 0, 0
			_, err = db.QueryChunks(context.Background(), "SELECT name,id FROM varchar_topn_rows() ORDER BY name DESC,id ASC LIMIT 5000 OFFSET 123", func(c *Chunk) error {
				chunks++
				for row := 0; row < c.RowCount(); row++ {
					name, valid, e := c.String(row, 0)
					if e != nil {
						return e
					}
					id, idValid, e := c.Int64(row, 1)
					if e != nil {
						return e
					}
					if next >= len(want) || !valid || !idValid || name != want[next].name || id != want[next].id {
						return fmt.Errorf("row %d=(%q,%d) differs from reference", next, name, id)
					}
					next++
				}
				return nil
			})
			if err != nil {
				t.Fatal(err)
			}
			if next != count || chunks < 2 {
				t.Fatalf("rows=%d chunks=%d", next, chunks)
			}
			if db.MemoryUsed() != 0 {
				t.Fatalf("managed leak=%d", db.MemoryUsed())
			}
		})
	}
}
