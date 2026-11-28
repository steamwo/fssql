package gosql

import (
	"context"
	"encoding/binary"
	"os"
	"path/filepath"
	"runtime"
	"sync/atomic"
	"testing"
	"time"
)

func TestProductionReviewRowsCloseCancelsBlockedReader(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	blocked := make(chan struct{})
	var readerCloses atomic.Int32
	db, err := OpenSQLDB(Config{TempDir: t.TempDir(), ManifestDir: t.TempDir(), TableFunctions: []TableFunction{{
		Name:    "review_blocked_reader",
		Columns: []Column{{Name: "id", Type: Type{ID: "BIGINT"}}},
		Open: func(context.Context) (BatchReader, error) {
			first := true
			return &batchReaderFunc{close: func() error { readerCloses.Add(1); return nil }, next: func(ctx context.Context, w BatchWriter) (int, error) {
				if first {
					first = false
					return 1, w.SetInt64(0, 0, 1)
				}
				close(blocked)
				<-ctx.Done()
				return 0, ctx.Err()
			}}, nil
		},
	}}})
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	db.SetMaxOpenConns(1)
	rows, err := db.QueryContext(ctx, "SELECT id FROM review_blocked_reader()")
	if err != nil {
		t.Fatal(err)
	}
	if !rows.Next() {
		t.Fatalf("first row: %v", rows.Err())
	}
	select {
	case <-blocked:
	case <-time.After(3 * time.Second):
		cancel()
		rows.Close()
		t.Fatal("upstream did not block")
	}
	done := make(chan error, 1)
	go func() { done <- rows.Close() }()
	select {
	case err := <-done:
		if err != nil {
			t.Fatal(err)
		}
	case <-time.After(time.Second):
		cancel()
		select {
		case <-done:
		case <-time.After(3 * time.Second):
			t.Fatal("cleanup remained blocked")
		}
		t.Fatal("Rows.Close did not cancel the blocked upstream; only canceling the original query context released it")
	}
	if err := ctx.Err(); err != nil {
		t.Fatalf("Rows.Close canceled the caller's context: %v", err)
	}
	if readerCloses.Load() != 1 {
		t.Fatalf("reader close calls=%d, want 1", readerCloses.Load())
	}
	if err := rows.Close(); err != nil {
		t.Fatal(err)
	}
	reuseCtx, reuseCancel := context.WithTimeout(ctx, 3*time.Second)
	defer reuseCancel()
	var answer int64
	if err := db.QueryRowContext(reuseCtx, "SELECT 42").Scan(&answer); err != nil || answer != 42 {
		t.Fatalf("reuse connection after Rows.Close: answer=%d error=%v", answer, err)
	}
}

func TestProductionReviewMalformedParquetAllocation(t *testing.T) {
	root := t.TempDir()
	// Schema field claims 65,536 structs, but the file has no list contents.
	meta := []byte{0x29, 0xfc, 0x80, 0x80, 0x04}
	data := append([]byte("PAR1"), meta...)
	trailer := make([]byte, 8)
	binary.LittleEndian.PutUint32(trailer, uint32(len(meta)))
	copy(trailer[4:], "PAR1")
	data = append(data, trailer...)
	path := filepath.Join(root, "invalid.parquet")
	if err := os.WriteFile(path, data, 0600); err != nil {
		t.Fatal(err)
	}
	uri, err := FileURI(path)
	if err != nil {
		t.Fatal(err)
	}
	db, err := Open(Config{LocalFiles: &LocalFileConfig{RootDir: root}, MemoryLimitBytes: 64 << 10, TempDir: t.TempDir(), ManifestDir: t.TempDir()})
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	runtime.GC()
	var before, after runtime.MemStats
	runtime.ReadMemStats(&before)
	_, err = db.QueryChunks(context.Background(), "SELECT * FROM read_parquet('"+uri+"')", func(*Chunk) error { return nil })
	runtime.ReadMemStats(&after)
	if err == nil {
		t.Fatal("invalid file accepted")
	}
	allocated := after.TotalAlloc - before.TotalAlloc
	t.Logf("file=%d bytes, limit=%d, allocated=%d bytes, managed_after=%d, error=%v", len(data), 64<<10, allocated, db.MemoryUsed(), err)
	if allocated > 1<<20 {
		t.Fatalf("tiny malformed footer amplified heap allocation to %d bytes", allocated)
	}
	if used := db.MemoryUsed(); used != 0 {
		t.Fatalf("failed query retained %d managed bytes", used)
	}
}
