package gosql

import (
	"context"
	"fmt"
	"io"
	"reflect"
	"sync/atomic"
	"testing"
)

func TestDomainCorrelatedSubqueries(t *testing.T) {
	for _, budget := range []int64{1 << 20, 128} {
		t.Run(fmt.Sprint(budget), func(t *testing.T) {
			db, err := OpenSQLDB(Config{TableFunctions: []TableFunction{commonSQLRows()}, TempDir: t.TempDir(), ManifestDir: t.TempDir(), SpillMemoryBudgetBytes: budget, Workers: 4})
			if err != nil {
				t.Fatal(err)
			}
			defer db.Close()
			cases := []struct {
				sql  string
				want [][]any
			}{
				{"SELECT o.g,(SELECT o.g),(SELECT o.id+1 WHERE FALSE) FROM common_rows() o WHERE o.id=3 ORDER BY o.g NULLS LAST", [][]any{{int64(2), int64(2), nil}, {nil, nil, nil}}},
				{"SELECT o.id,EXISTS(SELECT CAST('bad' AS INTEGER) FROM common_rows() i WHERE i.id<o.id) FROM common_rows() o WHERE o.g=2 ORDER BY o.id", [][]any{{int64(2), true}, {int64(3), true}}},
				{"SELECT o.g,(SELECT sum(i.id)+1 FROM common_rows() i WHERE i.g=o.g),(SELECT count(*)+1 FROM common_rows() i WHERE i.g=o.g) FROM common_rows() o WHERE o.id=3 ORDER BY o.g NULLS LAST", [][]any{{int64(2), "6", int64(3)}, {nil, nil, int64(1)}}},
				{"SELECT (SELECT count(DISTINCT i.id) FROM common_rows() i WHERE i.id<o.id),(SELECT sum(i.id+o.id) FROM common_rows() i WHERE i.id<o.id) FROM common_rows() o WHERE o.id IS NULL", [][]any{{int64(0), nil}}},
				{"SELECT o.g,(SELECT sum(i.id+o.id) FROM common_rows() i WHERE i.g=o.g) FROM common_rows() o WHERE o.id=3 ORDER BY o.g NULLS LAST", [][]any{{int64(2), "11"}, {nil, nil}}},
				{"SELECT o.id,(SELECT count(*) FROM common_rows() i WHERE i.id<o.id GROUP BY i.g HAVING count(*)>1) FROM common_rows() o WHERE o.g=2 ORDER BY o.id", [][]any{{int64(2), int64(2)}, {int64(3), int64(3)}}},
				{"SELECT o.g FROM common_rows() o GROUP BY o.g HAVING EXISTS(SELECT 1 FROM common_rows() i WHERE i.g<o.g) ORDER BY o.g", [][]any{{int64(2)}}},
				{"SELECT o.id,EXISTS(SELECT 1 FROM common_rows() i WHERE i.id<o.id) FROM common_rows() o ORDER BY o.id NULLS LAST", [][]any{{int64(1), false}, {int64(1), false}, {int64(2), true}, {int64(2), true}, {int64(3), true}, {int64(3), true}, {nil, false}}},
				{"SELECT o.id,(SELECT count(*) FROM common_rows() i WHERE i.id<o.id),(SELECT sum(i.id)+1 FROM common_rows() i WHERE i.id<o.id) FROM common_rows() o WHERE o.g=2 ORDER BY o.id", [][]any{{int64(2), int64(2), "3"}, {int64(3), int64(4), "7"}}},
				{"SELECT (SELECT count(*)+count(1) FROM common_rows() i WHERE i.id<o.id),(SELECT sum(1) FROM common_rows() i WHERE i.id<o.id) FROM common_rows() o WHERE o.id IS NULL", [][]any{{int64(0), nil}}},
				{"SELECT o.g,(SELECT i.id FROM common_rows() i WHERE i.g=o.g ORDER BY i.id DESC NULLS LAST LIMIT 1 OFFSET 1) FROM common_rows() o WHERE o.id=3 ORDER BY o.g NULLS LAST", [][]any{{int64(2), int64(2)}, {nil, nil}}},
				{"SELECT o.g,EXISTS(SELECT 1 FROM common_rows() i WHERE i.g=o.g LIMIT 0),EXISTS(SELECT 1 FROM common_rows() i WHERE i.g=o.g OFFSET 2) FROM common_rows() o WHERE o.id=3 ORDER BY o.g NULLS LAST", [][]any{{int64(2), false, false}, {nil, false, false}}},
				{"SELECT o.g,(SELECT count(*) FROM common_rows() i WHERE i.g=o.g HAVING count(*)=0) FROM common_rows() o WHERE o.id=3 ORDER BY o.g NULLS LAST", [][]any{{int64(2), nil}, {nil, int64(0)}}},
				{"SELECT o.id,o.id IN (SELECT i.id FROM common_rows() i WHERE i.id<o.id OR i.id IS NULL),o.id NOT IN (SELECT i.id FROM common_rows() i WHERE i.id<o.id) FROM common_rows() o WHERE o.g=2 ORDER BY o.id", [][]any{{int64(2), nil, true}, {int64(3), nil, true}}},
				{"SELECT o.g,o.id IN (SELECT i.id FROM common_rows() i WHERE i.g=o.g ORDER BY i.id DESC LIMIT 1) FROM common_rows() o WHERE o.id=3 ORDER BY o.g NULLS LAST", [][]any{{int64(2), true}, {nil, false}}},
				{"SELECT (SELECT o.id+1 FROM common_rows() i WHERE i.g=2 AND i.id=3) FROM common_rows() o WHERE o.id=1", [][]any{{int64(2)}, {int64(2)}}},
				{"SELECT o.g,(SELECT count(*) FROM common_rows() i WHERE EXISTS(SELECT 1 FROM common_rows() j WHERE j.g=o.g AND j.id=i.id)) FROM common_rows() o WHERE o.id=3 ORDER BY o.g NULLS LAST", [][]any{{int64(2), int64(4)}, {nil, int64(0)}}},
				{"SELECT o.g,count(*),(SELECT count(*) FROM common_rows() i WHERE i.g=o.g) FROM common_rows() o GROUP BY o.g ORDER BY o.g NULLS LAST", [][]any{{int64(1), int64(4), int64(4)}, {int64(2), int64(2), int64(2)}, {nil, int64(1), int64(0)}}},
				{"SELECT DISTINCT o.g,(SELECT DISTINCT i.id FROM common_rows() i WHERE i.g=o.g ORDER BY i.id LIMIT 1 OFFSET 1) FROM common_rows() o ORDER BY o.g NULLS LAST", [][]any{{int64(1), int64(2)}, {int64(2), int64(3)}, {nil, nil}}},
			}
			for _, tc := range cases {
				t.Run(tc.sql, func(t *testing.T) {
					rows, err := db.QueryContext(context.Background(), tc.sql)
					if err != nil {
						t.Fatal(err)
					}
					defer rows.Close()
					cols, err := rows.Columns()
					if err != nil {
						t.Fatal(err)
					}
					var got [][]any
					for rows.Next() {
						row := make([]any, len(cols))
						dest := make([]any, len(cols))
						for i := range row {
							dest[i] = &row[i]
						}
						if err := rows.Scan(dest...); err != nil {
							t.Fatal(err)
						}
						got = append(got, row)
					}
					if err := rows.Err(); err != nil {
						t.Fatal(err)
					}
					if !reflect.DeepEqual(got, tc.want) {
						t.Fatalf("got %#v want %#v", got, tc.want)
					}
				})
			}
		})
	}
}

func TestDomainMembershipSharesVolatileValues(t *testing.T) {
	var calls atomic.Int64
	fn := ScalarFunction{Name: "domain_touch", Arguments: []Type{{ID: "BIGINT"}}, Result: Type{ID: "BIGINT"}, NullHandling: ScalarNullPropagate, Eval: func(args []ScalarValue) (ScalarValue, error) { calls.Add(1); return args[0], nil }}
	db, err := OpenSQLDB(Config{TableFunctions: []TableFunction{commonSQLRows()}, ScalarFunctions: []ScalarFunction{fn}, TempDir: t.TempDir(), ManifestDir: t.TempDir(), SpillMemoryBudgetBytes: 128, Workers: 4})
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	rows, err := db.QueryContext(context.Background(), "SELECT o.id IN (SELECT domain_touch(i.id) FROM common_rows() i WHERE i.g<o.g) FROM common_rows() o WHERE o.g=2 ORDER BY o.id")
	if err != nil {
		t.Fatal(err)
	}
	var got []any
	for rows.Next() {
		var value any
		if err := rows.Scan(&value); err != nil {
			t.Fatal(err)
		}
		got = append(got, value)
	}
	if err := rows.Err(); err != nil {
		t.Fatal(err)
	}
	if err := rows.Close(); err != nil {
		t.Fatal(err)
	}
	if !reflect.DeepEqual(got, []any{true, nil}) {
		t.Fatalf("values=%v", got)
	}
	if calls.Load() != 3 {
		t.Fatalf("inner calls=%d want=3", calls.Load())
	}
}

func TestDomainSubqueryAcrossBatchesAndCleanup(t *testing.T) {
	fn := TableFunction{Name: "domain_many", Columns: []Column{{Name: "id", Type: Type{ID: "BIGINT"}}}, Open: func(context.Context) (BatchReader, error) {
		next := 0
		return &batchReaderFunc{next: func(_ context.Context, w BatchWriter) (int, error) {
			if next == 5000 {
				return 0, io.EOF
			}
			n := min(w.Capacity(), 5000-next)
			for i := 0; i < n; i++ {
				if err := w.SetInt64(i, 0, int64(next+i)); err != nil {
					return 0, err
				}
			}
			next += n
			return n, nil
		}}, nil
	}}
	db := testTypedDB(t, 4096, fn)
	for _, sql := range []string{
		"SELECT (SELECT i.id FROM domain_many() i WHERE i.id<=o.id ORDER BY i.id DESC LIMIT 1 OFFSET 2048) FROM domain_many() o WHERE o.id=4999",
		"SELECT (SELECT count(*) FROM domain_many() i WHERE i.id<o.id) FROM domain_many() o WHERE o.id=2951",
	} {
		var got int64
		_, err := db.QueryChunks(context.Background(), sql, func(c *Chunk) error {
			v, ok, err := c.Int64(0, 0)
			if err != nil {
				return err
			}
			if !ok {
				return fmt.Errorf("unexpected NULL")
			}
			got = v
			return nil
		})
		if err != nil || got != 2951 {
			t.Fatalf("%s: got=%d err=%v", sql, got, err)
		}
		if db.MemoryUsed() != 0 {
			t.Fatalf("managed memory leak=%d", db.MemoryUsed())
		}
	}
	for _, sql := range []string{
		"SELECT (SELECT i.id FROM domain_many() i WHERE i.id<o.id) FROM domain_many() o WHERE o.id=3",
		"SELECT (SELECT i.id FROM domain_many() i WHERE i.id<o.id LIMIT o.id) FROM domain_many() o WHERE o.id=3",
		"SELECT (SELECT i.id FROM domain_many() i WHERE i.id<o.id OFFSET o.id) FROM domain_many() o WHERE o.id=3",
	} {
		_, err := db.QueryChunks(context.Background(), sql, func(*Chunk) error { return nil })
		if err == nil {
			t.Fatalf("expected rejection: %s", sql)
		}
		if db.MemoryUsed() != 0 {
			t.Fatalf("error cleanup leaked %d bytes", db.MemoryUsed())
		}
	}
}
