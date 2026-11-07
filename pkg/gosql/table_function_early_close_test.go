package gosql

import (
	"context"
	"errors"
	"sync/atomic"
	"testing"

	"github.com/steamwo/fssql/internal/core/memory"
	querypkg "github.com/steamwo/fssql/internal/query"
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

func TestTypedSourceCloseRetriesFailedReaderClose(t *testing.T) {
	q := querypkg.New(context.Background(), memory.NewManager(1024))
	failure := errors.New("reader close failed")
	closes := 0
	state := &typedSourceState{
		q:            q,
		readerOpened: true,
		reader: &batchReaderFunc{close: func() error {
			closes++
			if closes == 1 {
				return failure
			}
			return nil
		}},
	}
	if err := q.AddCleanup(state.Close); err != nil {
		t.Fatal(err)
	}
	if err := q.Close(); !errors.Is(err, failure) {
		t.Fatalf("first close error = %v, want reader failure", err)
	}
	if err := q.Close(); err != nil {
		t.Fatalf("reader close retry failed: %v", err)
	}
	if closes != 2 {
		t.Fatalf("reader close calls = %d, want 2", closes)
	}
	if err := q.Close(); err != nil {
		t.Fatal(err)
	}
	if closes != 2 {
		t.Fatalf("successful reader close repeated: %d", closes)
	}
}
