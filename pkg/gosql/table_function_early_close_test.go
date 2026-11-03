package gosql

import (
	"context"
	"errors"
	"sync/atomic"
	"testing"
)

func TestTypedTableSourceConsumerEarlyEndReleasesOwnership(t *testing.T) {
	stop := errors.New("consumer stop")
	var closes atomic.Int32
	var next atomic.Int32
	db := testTypedDB(t, 1<<20, TableFunction{
		Name:    "typed_early_stop",
		Columns: []Column{{Name: "id", Type: Type{ID: "BIGINT"}, Nullable: false}},
		Open: func(context.Context) (BatchReader, error) {
			return &batchReaderFunc{
				next: func(_ context.Context, w BatchWriter) (int, error) {
					value := int64(next.Add(1))
					if err := w.SetInt64(0, 0, value); err != nil {
						return 0, err
					}
					return 1, nil
				},
				close: func() error { closes.Add(1); return nil },
			}, nil
		},
	})
	_, err := db.QueryChunks(context.Background(), "SELECT id FROM typed_early_stop()", func(*Chunk) error {
		return stop
	})
	if !errors.Is(err, stop) {
		t.Fatalf("error = %v, want consumer stop", err)
	}
	if closes.Load() != 1 {
		t.Fatalf("close count = %d, want 1", closes.Load())
	}
	if used := db.MemoryUsed(); used != 0 {
		t.Fatalf("managed memory leaked: %d", used)
	}
}
