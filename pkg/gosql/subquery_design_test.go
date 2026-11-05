package gosql

import (
	"context"
	"fmt"
	"io"
	"reflect"
	"strings"
	"sync/atomic"
	"testing"
)

func TestSubqueryJoinLoweringAndAggregateDistinct(t *testing.T) {
	for _, budget := range []int64{1 << 20, 128} {
		t.Run(fmt.Sprint(budget), func(t *testing.T) {
			db, err := OpenSQLDB(Config{TableFunctions: []TableFunction{commonSQLRows()}, TempDir: t.TempDir(), ManifestDir: t.TempDir(), SpillMemoryBudgetBytes: budget, Workers: 2})
			if err != nil {
				t.Fatal(err)
			}
			defer db.Close()
			cases := []struct {
				sql  string
				want [][]any
			}{
				{"SELECT 1 IN (SELECT 1 UNION ALL SELECT 1), 3 IN (SELECT 1 UNION ALL SELECT NULL), NULL IN (SELECT 1 WHERE FALSE), NULL NOT IN (SELECT 1 WHERE FALSE)", [][]any{{true, nil, false, true}}},
				{"SELECT NULL IN (SELECT NULL), 1 NOT IN (SELECT NULL), 1 IN (SELECT 1 UNION ALL SELECT NULL)", [][]any{{nil, nil, true}}},
				{"SELECT 1 IN (SELECT (SELECT 1))", [][]any{{true}}},
				{"SELECT 1 IN (SELECT sum(id) FROM common_rows() WHERE FALSE),0 IN (SELECT count(*) FROM common_rows() WHERE FALSE)", [][]any{{nil, true}}},
				{"SELECT id FROM common_rows() WHERE id IN (SELECT id FROM common_rows() WHERE g=2) ORDER BY id", [][]any{{int64(2)}, {int64(2)}, {int64(3)}, {int64(3)}}},
				{"SELECT id FROM common_rows() WHERE id NOT IN (SELECT id FROM common_rows() WHERE g=1)", nil},
				{"SELECT count(*) IN (SELECT 7), sum(id) IN (SELECT 12) FROM common_rows()", [][]any{{true, true}}},
				{"SELECT count(*) FROM common_rows() HAVING count(*) IN (SELECT 7)", [][]any{{int64(7)}}},
				{"SELECT sum(CASE WHEN id IN (SELECT id FROM common_rows() WHERE g=2) THEN id ELSE 0 END) FROM common_rows()", [][]any{{"10"}}},
				{"SELECT sum(CASE WHEN id IN (SELECT 1) THEN id ELSE 0 END),sum(CASE WHEN id IN (SELECT 2) THEN id ELSE 0 END) FROM common_rows()", [][]any{{"2", "4"}}},
				{"SELECT id FROM common_rows() WHERE id IN (SELECT id AS k FROM common_rows() ORDER BY k DESC NULLS LAST LIMIT 1) ORDER BY id", [][]any{{int64(3)}, {int64(3)}}},
				{"SELECT o.g,o.id,EXISTS(SELECT 1 FROM common_rows() i WHERE i.g=o.g AND i.id=3) FROM common_rows() o ORDER BY o.g NULLS LAST,o.id NULLS LAST", [][]any{{int64(1), int64(1), false}, {int64(1), int64(1), false}, {int64(1), int64(2), false}, {int64(1), nil, false}, {int64(2), int64(2), true}, {int64(2), int64(3), true}, {nil, int64(3), false}}},
				{"SELECT o.id FROM common_rows() o WHERE NOT EXISTS(SELECT 1 FROM common_rows() i WHERE i.g=o.g AND i.id=3) ORDER BY o.id NULLS LAST", [][]any{{int64(1)}, {int64(1)}, {int64(2)}, {int64(3)}, {nil}}},
				{"SELECT o.g,o.id,o.id IN (SELECT i.id FROM common_rows() i WHERE i.g=o.g) FROM common_rows() o ORDER BY o.g NULLS LAST,o.id NULLS LAST", [][]any{{int64(1), int64(1), true}, {int64(1), int64(1), true}, {int64(1), int64(2), true}, {int64(1), nil, nil}, {int64(2), int64(2), true}, {int64(2), int64(3), true}, {nil, int64(3), false}}},
				{"SELECT o.g,(SELECT sum(i.id) FROM common_rows() i WHERE i.g=o.g),(SELECT count(*) FROM common_rows() i WHERE i.g=o.g) FROM common_rows() o WHERE o.id=3 ORDER BY o.g NULLS LAST", [][]any{{int64(2), "5", int64(2)}, {nil, nil, int64(0)}}},
				{"SELECT o.id,(SELECT i.label FROM common_rows() i WHERE i.id=o.id AND i.g=2) FROM common_rows() o WHERE o.g=2 ORDER BY o.id", [][]any{{int64(2), "b"}, {int64(3), "ab"}}},
				{"SELECT EXISTS(SELECT count(*) FROM common_rows() i WHERE i.g=o.g) FROM common_rows() o WHERE o.g IS NULL", [][]any{{true}}},
				{"SELECT count(*) FROM common_rows() o WHERE EXISTS(SELECT 1 FROM common_rows() i WHERE i.g=o.g)", [][]any{{int64(6)}}},
				{"SELECT count(*) FROM common_rows() o WHERE EXISTS(SELECT 1 FROM common_rows() o WHERE o.id=3)", [][]any{{int64(7)}}},
				{"SELECT o.g,o.id,o.id IN (SELECT max(i.id) FROM common_rows() i WHERE i.g=o.g GROUP BY i.g) FROM common_rows() o WHERE o.id=3 ORDER BY o.g NULLS LAST", [][]any{{int64(2), int64(3), true}, {nil, int64(3), false}}},
				{"SELECT o.g,o.id IN (SELECT sum(i.id) FROM common_rows() i WHERE i.g=o.g),0 IN (SELECT count(*) FROM common_rows() i WHERE i.g=o.g) FROM common_rows() o WHERE o.id=3 ORDER BY o.g NULLS LAST", [][]any{{int64(2), false, false}, {nil, nil, true}}},
				{"SELECT o.id IN (SELECT max(i.id) FROM common_rows() i WHERE i.g=o.g GROUP BY i.g HAVING count(*)>9) FROM common_rows() o WHERE o.id IS NULL", [][]any{{false}}},
				{"SELECT o.id,(SELECT DISTINCT i.label FROM common_rows() i WHERE i.id=o.id) FROM common_rows() o WHERE o.id=1", [][]any{{int64(1), "a_x"}, {int64(1), "a_x"}}},
				{"SELECT o.g,(SELECT count(*) FROM common_rows() i WHERE i.g=o.g GROUP BY i.g) FROM common_rows() o WHERE o.id=3 ORDER BY o.g NULLS LAST", [][]any{{int64(2), int64(2)}, {nil, nil}}},
				{"SELECT o.g,(SELECT sum(i.id)+1 FROM common_rows() i WHERE i.g=o.g GROUP BY i.g HAVING count(*)>1) FROM common_rows() o WHERE o.id=3 ORDER BY o.g NULLS LAST", [][]any{{int64(2), "6"}, {nil, nil}}},
				{"SELECT DISTINCT o.g,EXISTS(SELECT i.id FROM common_rows() i WHERE i.g=o.g GROUP BY i.id HAVING count(*)>1) AS found FROM common_rows() o ORDER BY o.g NULLS LAST", [][]any{{int64(1), true}, {int64(2), false}, {nil, false}}},
				{"SELECT sum(DISTINCT id),avg(DISTINCT id),min(DISTINCT id),max(DISTINCT id),sum(id),count(DISTINCT id) FROM common_rows()", [][]any{{"6", float64(2), int64(1), int64(3), "12", int64(3)}}},
				{"SELECT g,sum(DISTINCT id),avg(DISTINCT id) FROM common_rows() GROUP BY g ORDER BY g NULLS FIRST", [][]any{{nil, "3", float64(3)}, {int64(1), "3", float64(1.5)}, {int64(2), "5", float64(2.5)}}},
				{"SELECT sum(DISTINCT id),avg(DISTINCT id),min(DISTINCT id),max(DISTINCT id) FROM common_rows() WHERE FALSE", [][]any{{nil, nil, nil, nil}}},
				{"SELECT 6 IN (SELECT sum(DISTINCT id) FROM common_rows())", [][]any{{true}}},
			}
			for _, tc := range cases {
				t.Run(tc.sql, func(t *testing.T) {
					rows, err := db.QueryContext(context.Background(), tc.sql)
					if err != nil {
						t.Fatal(err)
					}
					defer rows.Close()
					columns, err := rows.Columns()
					if err != nil {
						t.Fatal(err)
					}
					var got [][]any
					for rows.Next() {
						row := make([]any, len(columns))
						dest := make([]any, len(columns))
						for i := range dest {
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

func TestMembershipMaterializesVolatileInputOnce(t *testing.T) {
	var calls atomic.Int64
	fn := ScalarFunction{Name: "membership_once", Arguments: []Type{{ID: "BIGINT"}}, Result: Type{ID: "BIGINT"}, NullHandling: ScalarNullPropagate, Eval: func(args []ScalarValue) (ScalarValue, error) { calls.Add(1); return args[0], nil }}
	db, err := OpenSQLDB(Config{TableFunctions: []TableFunction{commonSQLRows()}, ScalarFunctions: []ScalarFunction{fn}, TempDir: t.TempDir(), ManifestDir: t.TempDir(), SpillMemoryBudgetBytes: 128})
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	var found bool
	if err := db.QueryRowContext(context.Background(), "SELECT 1 IN (SELECT membership_once(id) FROM common_rows())").Scan(&found); err != nil || !found {
		t.Fatalf("result=%v err=%v", found, err)
	}
	if calls.Load() != 6 {
		t.Fatalf("inner evaluator calls=%d want=6", calls.Load())
	}
	calls.Store(0)
	if err := db.QueryRowContext(context.Background(), "SELECT 12 IN (SELECT sum(membership_once(id)) FROM common_rows())").Scan(&found); err != nil || !found {
		t.Fatalf("aggregate inner result: %v %v", found, err)
	}
	if calls.Load() != 6 {
		t.Fatalf("aggregate inner evaluator calls=%d want=6", calls.Load())
	}
}

func TestMembershipSortedLimitAndSpillAcrossBatches(t *testing.T) {
	fn := TableFunction{Name: "membership_many", Columns: []Column{{Name: "id", Type: Type{ID: "BIGINT"}}}, Open: func(context.Context) (BatchReader, error) {
		next := 0
		return &batchReaderFunc{next: func(_ context.Context, w BatchWriter) (int, error) {
			if next >= 5000 {
				return 0, io.EOF
			}
			n := min(w.Capacity(), 5000-next)
			for row := 0; row < n; row++ {
				if err := w.SetInt64(row, 0, int64(next+row)); err != nil {
					return 0, err
				}
			}
			next += n
			return n, nil
		}}, nil
	}}
	db, err := OpenSQLDB(Config{TableFunctions: []TableFunction{fn}, MemoryLimitBytes: 8 << 20, TempLimitBytes: 64 << 20, SpillMemoryBudgetBytes: 4096, Workers: 4, TempDir: t.TempDir(), ManifestDir: t.TempDir()})
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	var found bool
	if err := db.QueryRowContext(context.Background(), "SELECT 2951 IN (SELECT id FROM membership_many() ORDER BY id DESC LIMIT 1 OFFSET 2048)").Scan(&found); err != nil || !found {
		t.Fatalf("sorted subquery: %v %v", found, err)
	}
	var count int64
	if err := db.QueryRowContext(context.Background(), "SELECT count(*) FROM membership_many() o WHERE o.id IN (SELECT id FROM membership_many())").Scan(&count); err != nil || count != 5000 {
		t.Fatalf("spilled IN count=%d err=%v", count, err)
	}
	if err := db.QueryRowContext(context.Background(), "SELECT count(*) FROM membership_many() o WHERE o.id NOT IN (SELECT CASE WHEN id=4999 THEN CAST(NULL AS BIGINT) ELSE id END FROM membership_many())").Scan(&count); err != nil || count != 0 {
		t.Fatalf("spilled NOT IN count=%d err=%v", count, err)
	}
}

func TestCorrelatedSubqueryCardinalityAndUnsupportedShapes(t *testing.T) {
	db := testTypedDB(t, 128, commonSQLRows())
	queries := []string{
		"SELECT (SELECT i.label FROM common_rows() i WHERE i.id=o.id) FROM common_rows() o WHERE o.id=1",
		"SELECT 1 IN (SELECT id,g FROM common_rows())",
		"SELECT EXISTS(SELECT sum(o.id) FROM common_rows() i WHERE i.g=o.g) FROM common_rows() o",
		"SELECT (SELECT count(*) FROM common_rows() i WHERE i.g=o.g GROUP BY i.id) FROM common_rows() o WHERE o.g=1",
	}
	for _, sql := range queries {
		_, err := db.QueryChunks(context.Background(), sql, func(*Chunk) error { return nil })
		if err == nil {
			t.Errorf("expected error: %s", sql)
		}
		if used := db.MemoryUsed(); used != 0 {
			t.Fatalf("managed memory leak: %d (%v)", used, err)
		}
	}
	sqlDB, err := OpenSQLDB(Config{TableFunctions: []TableFunction{commonSQLRows()}, TempDir: t.TempDir(), ManifestDir: t.TempDir()})
	if err != nil {
		t.Fatal(err)
	}
	defer sqlDB.Close()
	var value any
	err = sqlDB.QueryRowContext(context.Background(), queries[0]).Scan(&value)
	if err == nil || !strings.Contains(err.Error(), "more than one row") {
		t.Fatalf("scalar cardinality: %v", err)
	}
	stmt, err := sqlDB.PrepareContext(context.Background(), "SELECT ? IN (SELECT id FROM common_rows() WHERE g=?)")
	if err != nil {
		t.Fatal(err)
	}
	defer stmt.Close()
	var found bool
	if err := stmt.QueryRowContext(context.Background(), 3, 2).Scan(&found); err != nil || !found {
		t.Fatalf("prepared IN: %v %v", found, err)
	}
}
