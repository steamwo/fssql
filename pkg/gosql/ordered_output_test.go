package gosql

import (
	"context"
	"fmt"
	"io"
	"sync/atomic"
	"testing"
	"time"
)

func TestOrderByStreamsOrderedCallbacksWithFourWorkers(t *testing.T) {
	const total = 7000
	fn := TableFunction{Name: "ordered_rows", Columns: []Column{{Name: "id", Type: Type{ID: "BIGINT"}}}, Open: func(context.Context) (BatchReader, error) {
		next := 0
		return &batchReaderFunc{next: func(_ context.Context, w BatchWriter) (int, error) {
			if next == total {
				return 0, io.EOF
			}
			n := 256
			if total-next < n {
				n = total - next
			}
			for row := 0; row < n; row++ {
				if err := w.SetInt64(row, 0, int64(total-next-row-1)); err != nil {
					return 0, err
				}
			}
			next += n
			return n, nil
		}}, nil
	}}
	db, err := Open(Config{MemoryLimitBytes: 8 << 20, TempLimitBytes: 8 << 20, SpillMemoryBudgetBytes: 1 << 20, Workers: 4, TempDir: t.TempDir(), ManifestDir: t.TempDir(), TableFunctions: []TableFunction{fn}})
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	var active atomic.Int32
	next := int64(123)
	chunks := 0
	_, err = db.QueryChunks(context.Background(), "SELECT id FROM ordered_rows() ORDER BY id LIMIT 5000 OFFSET 123", func(c *Chunk) error {
		if active.Add(1) != 1 {
			active.Add(-1)
			return fmt.Errorf("ordered callbacks overlapped")
		}
		defer active.Add(-1)
		// Expose completion-order races while the final stage consumes several chunks.
		if chunks == 0 {
			time.Sleep(10 * time.Millisecond)
		}
		chunks++
		for row := 0; row < c.RowCount(); row++ {
			v, ok, e := c.Int64(row, 0)
			if e != nil {
				return e
			}
			if !ok || v != next {
				return fmt.Errorf("row=%d want=%d", v, next)
			}
			next++
		}
		return nil
	})
	if err != nil {
		t.Fatal(err)
	}
	if next != 5123 || chunks < 2 {
		t.Fatalf("next=%d chunks=%d", next, chunks)
	}
	if db.MemoryUsed() != 0 {
		t.Fatalf("managed leak=%d", db.MemoryUsed())
	}
}
