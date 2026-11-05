package gosql

import (
	"context"
	"errors"
	"fmt"
	"io"
	"strings"
	"sync/atomic"
	"testing"
	"time"
)

func TestMaterializedCrossStatementSnapshot(t *testing.T) {
	for _, budget := range []int64{1, 1 << 20} {
		t.Run(fmt.Sprint(budget), func(t *testing.T) {
			var opens atomic.Int32
			fn := migrationSource()
			open := fn.Open
			fn.Open = func(ctx context.Context) (BatchReader, error) {
				if opens.Add(1) != 1 {
					return nil, errors.New("upstream reopened")
				}
				return open(ctx)
			}
			db, err := Open(Config{TableFunctions: []TableFunction{fn}, SpillMemoryBudgetBytes: budget, TempDir: t.TempDir(), ManifestDir: t.TempDir()})
			if err != nil {
				t.Fatal(err)
			}
			defer db.Close()
			m, err := db.Materialize(context.Background(), "SELECT * FROM migration_rows()")
			if err != nil {
				t.Fatal(err)
			}
			for i := 0; i < 2; i++ {
				count := 0
				_, err = db.QueryChunks(context.Background(), m.SQL(), func(c *Chunk) error {
					count += c.RowCount()
					if c.RowCount() != 3 {
						return fmt.Errorf("rows=%d", c.RowCount())
					}
					b, ok, e := c.Bytes(0, 2)
					if e != nil || !ok || len(b) != 7 {
						return fmt.Errorf("blob %v %v %v", b, ok, e)
					}
					typ, e := c.Type(3)
					if e != nil || typ.ID != "TIMESTAMPTZ" {
						return fmt.Errorf("type=%v err=%v", typ, e)
					}
					return nil
				})
				if err != nil || count != 3 {
					t.Fatalf("scan=%d rows=%d err=%v", i, count, err)
				}
			}
			_, err = db.QueryChunks(context.Background(), "SELECT count(*) FROM ("+m.SQL()+") a JOIN ("+m.SQL()+") b ON a.u IS NOT DISTINCT FROM b.u", func(c *Chunk) error {
				n, _, e := c.Int64(0, 0)
				if n != 3 {
					return fmt.Errorf("join count=%d", n)
				}
				return e
			})
			if err != nil {
				t.Fatal(err)
			}
			if opens.Load() != 1 {
				t.Fatal(opens.Load())
			}
			if err = m.Close(); err != nil {
				t.Fatal(err)
			}
			if err = m.Close(); err != nil {
				t.Fatal(err)
			}
			if _, err = db.QueryChunks(context.Background(), m.SQL(), func(*Chunk) error { return nil }); err == nil {
				t.Fatal("closed snapshot accepted")
			}
			if db.MemoryUsed() != 0 {
				t.Fatalf("leaked %d bytes", db.MemoryUsed())
			}
		})
	}
}

func TestMaterializedCloseRetainsBoundReader(t *testing.T) {
	db, err := Open(Config{TableFunctions: []TableFunction{migrationSource()}, TempDir: t.TempDir(), ManifestDir: t.TempDir(), SpillMemoryBudgetBytes: 1})
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	m, err := db.Materialize(context.Background(), "SELECT * FROM migration_rows()")
	if err != nil {
		t.Fatal(err)
	}
	q, p, _, err := db.prepare(context.Background(), m.SQL())
	if err != nil {
		t.Fatal(err)
	}
	defer q.Close()
	if err = m.Close(); err != nil {
		t.Fatal(err)
	}
	rows := 0
	err = p.Run(q, &borrowedChunkSink{callback: func(c *Chunk) error { rows += c.RowCount(); return nil }})
	if err != nil || rows != 3 {
		t.Fatalf("bound reader rows=%d err=%v", rows, err)
	}
	if err = q.Close(); err != nil {
		t.Fatal(err)
	}
	if db.MemoryUsed() != 0 {
		t.Fatalf("retained lease leaked %d", db.MemoryUsed())
	}
}

func TestMaterializedCancelAndQuota(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	started := make(chan struct{})
	fn := migrationSource()
	fn.Open = func(context.Context) (BatchReader, error) {
		return &batchReaderFunc{next: func(ctx context.Context, _ BatchWriter) (int, error) {
			close(started)
			<-ctx.Done()
			return 0, ctx.Err()
		}}, nil
	}
	db, err := Open(Config{TableFunctions: []TableFunction{fn}, TempDir: t.TempDir(), ManifestDir: t.TempDir()})
	if err != nil {
		t.Fatal(err)
	}
	done := make(chan error, 1)
	go func() { _, err := db.Materialize(ctx, "SELECT * FROM migration_rows()"); done <- err }()
	select {
	case <-started:
	case <-ctx.Done():
		t.Fatal(ctx.Err())
	}
	if err = db.Close(); err != nil {
		t.Fatal(err)
	}
	select {
	case err = <-done:
		if err == nil {
			t.Fatal("closed DB published a snapshot")
		}
	case <-ctx.Done():
		t.Fatal("materialization did not cancel")
	}
	if db.MemoryUsed() != 0 || len(db.materialized.items) != 0 {
		t.Fatal("cancelled snapshot leaked")
	}
	db, err = Open(Config{TableFunctions: []TableFunction{migrationSource()}, TempDir: t.TempDir(), ManifestDir: t.TempDir(), TempLimitBytes: 1, SpillMemoryBudgetBytes: 1})
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	if _, err = db.Materialize(context.Background(), "SELECT * FROM migration_rows()"); err == nil {
		t.Fatal("temporary quota ignored")
	}
	if db.MemoryUsed() != 0 || len(db.materialized.items) != 0 {
		t.Fatal("quota failure leaked")
	}
}

func TestMaterializedLargeSpill(t *testing.T) {
	fn := TableFunction{Name: "many", Columns: []Column{{Name: "id", Type: Type{ID: "BIGINT"}}}, Open: func(context.Context) (BatchReader, error) {
		offset := 0
		return &batchReaderFunc{next: func(_ context.Context, w BatchWriter) (int, error) {
			n := min(w.Capacity(), 8193-offset)
			if n == 0 {
				return 0, io.EOF
			}
			for i := 0; i < n; i++ {
				if err := w.SetInt64(i, 0, int64(offset+i)); err != nil {
					return 0, err
				}
			}
			offset += n
			return n, nil
		}}, nil
	}}
	db, err := Open(Config{TableFunctions: []TableFunction{fn}, TempDir: t.TempDir(), ManifestDir: t.TempDir(), SpillMemoryBudgetBytes: 1024})
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	m, err := db.Materialize(context.Background(), "SELECT * FROM many()")
	if err != nil {
		t.Fatal(err)
	}
	if m.state.store == nil {
		t.Fatal("did not spill")
	}
	for i := 0; i < 2; i++ {
		rows := 0
		_, err = db.QueryChunks(context.Background(), m.SQL(), func(c *Chunk) error {
			for j := 0; j < c.RowCount(); j++ {
				v, _, e := c.Int64(j, 0)
				if e != nil || v != int64(rows) {
					return fmt.Errorf("row=%d value=%d err=%v", rows, v, e)
				}
				rows++
			}
			return nil
		})
		if err != nil || rows != 8193 {
			t.Fatalf("rows=%d err=%v", rows, err)
		}
	}
	if err = m.Close(); err != nil {
		t.Fatal(err)
	}
	if db.MemoryUsed() != 0 {
		t.Fatal("large spill leaked")
	}
}

func TestMaterializedFailureAndDBClose(t *testing.T) {
	fn := migrationSource()
	fn.Open = func(context.Context) (BatchReader, error) {
		return &batchReaderFunc{next: func(context.Context, BatchWriter) (int, error) { return 0, errors.New("source failure") }}, nil
	}
	db, err := Open(Config{TableFunctions: []TableFunction{fn}, TempDir: t.TempDir(), ManifestDir: t.TempDir()})
	if err != nil {
		t.Fatal(err)
	}
	if _, err = db.Materialize(context.Background(), "SELECT * FROM migration_rows()"); err == nil || !strings.Contains(err.Error(), "source failure") {
		t.Fatal(err)
	}
	if len(db.materialized.items) != 0 || db.MemoryUsed() != 0 {
		t.Fatal("failed snapshot leaked")
	}
	db.Close()
	db, err = Open(Config{TableFunctions: []TableFunction{migrationSource()}, TempDir: t.TempDir(), ManifestDir: t.TempDir()})
	if err != nil {
		t.Fatal(err)
	}
	m, err := db.Materialize(context.Background(), "SELECT * FROM migration_rows()")
	if err != nil {
		t.Fatal(err)
	}
	if err = db.Close(); err != nil {
		t.Fatal(err)
	}
	if db.MemoryUsed() != 0 {
		t.Fatal("DB close leaked snapshot")
	}
	if err = m.Close(); err != nil {
		t.Fatal(err)
	}
	_, err = db.Materialize(context.Background(), "SELECT 1")
	if err == nil {
		t.Fatal("closed DB accepted materialization")
	}
}
