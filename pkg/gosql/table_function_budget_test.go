package gosql

import (
	"context"
	"errors"
	"sync/atomic"
	"testing"

	"github.com/steamwo/fssql/internal/core/memory"
)

func TestTypedTableSourceManagedBudgetExhaustionCleansUp(t *testing.T) {
	var closes atomic.Int32
	payload := make([]byte, 1<<20)
	db, err := Open(Config{
		MemoryLimitBytes:       128 << 10,
		TempLimitBytes:         1 << 20,
		SpillMemoryBudgetBytes: 64 << 10,
		TempDir:                t.TempDir(),
		ManifestDir:            t.TempDir(),
		Workers:                1,
		TableFunctions: []TableFunction{{
			Name:    "typed_budget",
			Columns: []Column{{Name: "payload", Type: Type{ID: "BLOB"}, Nullable: false}},
			Open: func(context.Context) (BatchReader, error) {
				return &batchReaderFunc{
					next: func(_ context.Context, w BatchWriter) (int, error) {
						if err := w.SetBytes(0, 0, payload); err != nil {
							return 0, err
						}
						return 1, nil
					},
					close: func() error { closes.Add(1); return nil },
				}, nil
			},
		}},
	})
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	_, err = db.QueryChunks(context.Background(), "SELECT payload FROM typed_budget()", func(*Chunk) error { return nil })
	if !errors.Is(err, memory.ErrLimitExceeded) {
		t.Fatalf("error = %v, want memory.ErrLimitExceeded", err)
	}
	if closes.Load() != 1 {
		t.Fatalf("close count = %d, want 1", closes.Load())
	}
	if used := db.MemoryUsed(); used != 0 {
		t.Fatalf("managed memory leaked: %d", used)
	}
}
