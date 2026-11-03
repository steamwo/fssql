package gosql

import (
	"context"
	"errors"
	"io"
	"reflect"
	"sort"
	"sync/atomic"
	"testing"
)

type batchReaderFunc struct {
	next  func(context.Context, BatchWriter) (int, error)
	close func() error
}

func (r *batchReaderFunc) Next(ctx context.Context, w BatchWriter) (int, error) {
	return r.next(ctx, w)
}

func (r *batchReaderFunc) Close() error {
	if r.close != nil {
		return r.close()
	}
	return nil
}

func testTypedDB(t *testing.T, spill int64, fn TableFunction) *DB {
	t.Helper()
	db, err := Open(Config{
		MemoryLimitBytes:       8 << 20,
		TempLimitBytes:         8 << 20,
		SpillMemoryBudgetBytes: spill,
		TempDir:                t.TempDir(),
		ManifestDir:            t.TempDir(),
		TableFunctions:         []TableFunction{fn},
		Workers:                1,
	})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = db.Close() })
	return db
}

func TestTypedTableSourceCopiesReusedBlobBuffer(t *testing.T) {
	var opens, closes atomic.Int32
	shared := []byte{'a', 0, 'b'}
	fn := TableFunction{
		Name:    "typed_blob",
		Columns: []Column{{Name: "payload", Type: Type{ID: "BLOB"}, Nullable: false}},
		Open: func(context.Context) (BatchReader, error) {
			opens.Add(1)
			emitted := false
			return &batchReaderFunc{
				next: func(_ context.Context, w BatchWriter) (int, error) {
					if emitted {
						return 0, io.EOF
					}
					emitted = true
					if err := w.SetBytes(0, 0, shared); err != nil {
						return 0, err
					}
					shared[0] = 'z'
					return 1, io.EOF
				},
				close: func() error { closes.Add(1); return nil },
			}, nil
		},
	}
	db := testTypedDB(t, 1<<20, fn)
	var got []byte
	columns, err := db.QueryChunks(context.Background(), "SELECT payload FROM typed_blob()", func(chunk *Chunk) error {
		value, valid, err := chunk.Bytes(0, 0)
		if err != nil {
			return err
		}
		if !valid {
			return errors.New("payload unexpectedly NULL")
		}
		got = value
		return nil
	})
	if err != nil {
		t.Fatal(err)
	}
	if len(columns) != 1 || columns[0].Type.ID != "BLOB" {
		t.Fatalf("schema = %+v", columns)
	}
	if !reflect.DeepEqual(got, []byte{'a', 0, 'b'}) {
		t.Fatalf("payload = %v, want copied original", got)
	}
	if opens.Load() != 1 || closes.Load() != 1 {
		t.Fatalf("open/close = %d/%d, want 1/1", opens.Load(), closes.Load())
	}
	if used := db.MemoryUsed(); used != 0 {
		t.Fatalf("managed memory leaked: %d", used)
	}
}

func TestTypedTableSourceMultiReferenceConsumesUpstreamOnce(t *testing.T) {
	for _, tc := range []struct {
		name  string
		spill int64
	}{
		{name: "memory", spill: 1 << 20},
		{name: "forced_spill", spill: 1},
	} {
		t.Run(tc.name, func(t *testing.T) {
			var opens, closes atomic.Int32
			fn := TableFunction{
				Name:    "typed_rows",
				Columns: []Column{{Name: "id", Type: Type{ID: "BIGINT"}, Nullable: false}},
				Open: func(context.Context) (BatchReader, error) {
					opens.Add(1)
					emitted := false
					return &batchReaderFunc{
						next: func(_ context.Context, w BatchWriter) (int, error) {
							if emitted {
								return 0, io.EOF
							}
							emitted = true
							if err := w.SetInt64(0, 0, 1); err != nil {
								return 0, err
							}
							if err := w.SetInt64(1, 0, 2); err != nil {
								return 0, err
							}
							return 2, io.EOF
						},
						close: func() error { closes.Add(1); return nil },
					}, nil
				},
			}
			db := testTypedDB(t, tc.spill, fn)
			var got [][2]int64
			_, err := db.QueryChunks(context.Background(), "SELECT a.id, b.id FROM typed_rows() AS a CROSS JOIN typed_rows() AS b", func(chunk *Chunk) error {
				for row := 0; row < chunk.RowCount(); row++ {
					left, lok, err := chunk.Int64(row, 0)
					if err != nil {
						return err
					}
					right, rok, err := chunk.Int64(row, 1)
					if err != nil {
						return err
					}
					if !lok || !rok {
						return errors.New("join value unexpectedly NULL")
					}
					got = append(got, [2]int64{left, right})
				}
				return nil
			})
			if err != nil {
				t.Fatal(err)
			}
			sort.Slice(got, func(i, j int) bool {
				if got[i][0] != got[j][0] {
					return got[i][0] < got[j][0]
				}
				return got[i][1] < got[j][1]
			})
			want := [][2]int64{{1, 1}, {1, 2}, {2, 1}, {2, 2}}
			if !reflect.DeepEqual(got, want) {
				t.Fatalf("rows = %v, want %v", got, want)
			}
			if opens.Load() != 1 || closes.Load() != 1 {
				t.Fatalf("open/close = %d/%d, want 1/1", opens.Load(), closes.Load())
			}
			if used := db.MemoryUsed(); used != 0 {
				t.Fatalf("managed memory leaked: %d", used)
			}
		})
	}
}

func TestTypedTableSourceEmptyPreservesSchema(t *testing.T) {
	var opens, closes atomic.Int32
	fn := TableFunction{
		Name:    "typed_empty",
		Columns: []Column{{Name: "id", Type: Type{ID: "UBIGINT"}, Nullable: true}},
		Open: func(context.Context) (BatchReader, error) {
			opens.Add(1)
			return &batchReaderFunc{
				next:  func(context.Context, BatchWriter) (int, error) { return 0, io.EOF },
				close: func() error { closes.Add(1); return nil },
			}, nil
		},
	}
	db := testTypedDB(t, 1<<20, fn)
	callbacks := 0
	columns, err := db.QueryChunks(context.Background(), "SELECT id FROM typed_empty()", func(*Chunk) error {
		callbacks++
		return nil
	})
	if err != nil {
		t.Fatal(err)
	}
	if callbacks != 0 {
		t.Fatalf("callbacks = %d, want 0", callbacks)
	}
	if len(columns) != 1 || columns[0].Name != "id" || columns[0].Type.ID != "UBIGINT" || !columns[0].Nullable {
		t.Fatalf("schema = %+v", columns)
	}
	if opens.Load() != 1 || closes.Load() != 1 {
		t.Fatalf("open/close = %d/%d, want 1/1", opens.Load(), closes.Load())
	}
}

func TestTypedTableSourceErrorAndCancellationReleaseOwnership(t *testing.T) {
	t.Run("source_error", func(t *testing.T) {
		boom := errors.New("boom")
		var closes atomic.Int32
		db := testTypedDB(t, 1<<20, TableFunction{
			Name:    "typed_error",
			Columns: []Column{{Name: "id", Type: Type{ID: "BIGINT"}, Nullable: true}},
			Open: func(context.Context) (BatchReader, error) {
				return &batchReaderFunc{
					next:  func(context.Context, BatchWriter) (int, error) { return 0, boom },
					close: func() error { closes.Add(1); return nil },
				}, nil
			},
		})
		_, err := db.QueryChunks(context.Background(), "SELECT id FROM typed_error()", func(*Chunk) error { return nil })
		if !errors.Is(err, boom) {
			t.Fatalf("error = %v, want boom", err)
		}
		if closes.Load() != 1 {
			t.Fatalf("close count = %d, want 1", closes.Load())
		}
		if used := db.MemoryUsed(); used != 0 {
			t.Fatalf("managed memory leaked: %d", used)
		}
	})

	t.Run("cancel", func(t *testing.T) {
		ctx, cancel := context.WithCancel(context.Background())
		var closes atomic.Int32
		db := testTypedDB(t, 1<<20, TableFunction{
			Name:    "typed_cancel",
			Columns: []Column{{Name: "id", Type: Type{ID: "BIGINT"}, Nullable: true}},
			Open: func(context.Context) (BatchReader, error) {
				return &batchReaderFunc{
					next: func(ctx context.Context, _ BatchWriter) (int, error) {
					cancel()
					return 0, ctx.Err()
				},
					close: func() error { closes.Add(1); return nil },
				}, nil
			},
		})
		_, err := db.QueryChunks(ctx, "SELECT id FROM typed_cancel()", func(*Chunk) error { return nil })
		if !errors.Is(err, context.Canceled) {
			t.Fatalf("error = %v, want context.Canceled", err)
		}
		if closes.Load() != 1 {
			t.Fatalf("close count = %d, want 1", closes.Load())
		}
		if used := db.MemoryUsed(); used != 0 {
			t.Fatalf("managed memory leaked: %d", used)
		}
	})
}
