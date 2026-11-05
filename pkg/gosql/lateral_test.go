package gosql

import (
	"context"
	"fmt"
	"io"
	"strings"
	"sync/atomic"
	"testing"
)

// Golden results are independently generated with DuckDB 2.0.0 and also
// retained as SQL.SUBQUERY.LATERAL cases in the compatibility corpus.
func TestLateralDerivedTables(t *testing.T) {
	cases := []struct{ sql, want string }{
		{"SELECT o.g,o.id,d.v FROM common_rows() o, LATERAL (SELECT o.id+1 AS v) d ORDER BY o.g NULLS LAST,o.id NULLS LAST", "1,1,2\n1,1,2\n1,2,3\n1,NULL,NULL\n2,2,3\n2,3,4\nNULL,3,4"},
		{"SELECT o.g,o.id,d.v FROM common_rows() o, (SELECT o.id+1 AS v) d ORDER BY o.g NULLS LAST,o.id NULLS LAST", "1,1,2\n1,1,2\n1,2,3\n1,NULL,NULL\n2,2,3\n2,3,4\nNULL,3,4"},
		{"SELECT o.id,d.v FROM common_rows() o CROSS JOIN LATERAL (SELECT o.id+1 AS v WHERE o.id<3) d ORDER BY o.id", "1,2\n1,2\n2,3\n2,3"},
		{"SELECT o.g,o.id,d.v FROM common_rows() o LEFT JOIN LATERAL (SELECT o.id+1 AS v WHERE o.id<3) d ON TRUE ORDER BY o.g NULLS LAST,o.id NULLS LAST", "1,1,2\n1,1,2\n1,2,3\n1,NULL,NULL\n2,2,3\n2,3,NULL\nNULL,3,NULL"},
		{"SELECT o.g,d.n,d.m FROM common_rows() o JOIN LATERAL (SELECT count(*) AS n,max(i.id) AS m FROM common_rows() i WHERE i.g=o.g) d ON TRUE WHERE o.id=3 ORDER BY o.g NULLS LAST", "2,2,3\nNULL,0,NULL"},
		{"SELECT o.g,d.v FROM common_rows() o CROSS JOIN LATERAL (SELECT sum(i.id+o.id) AS v FROM common_rows() i WHERE i.g=o.g) d WHERE o.id=3 ORDER BY o.g NULLS LAST", "2,11\nNULL,NULL"},
		{"SELECT o.g,o.id,d.v FROM common_rows() o LEFT JOIN LATERAL (SELECT DISTINCT i.id AS v FROM common_rows() i WHERE i.g=o.g ORDER BY i.id DESC NULLS LAST LIMIT 1 OFFSET 1) d ON TRUE ORDER BY o.g NULLS LAST,o.id NULLS LAST", "1,1,1\n1,1,1\n1,2,1\n1,NULL,1\n2,2,2\n2,3,2\nNULL,3,NULL"},
		{"SELECT o.g,o.id,d.v FROM common_rows() o LEFT JOIN LATERAL (SELECT o.id+1 AS v) d ON FALSE ORDER BY o.g NULLS LAST,o.id NULLS LAST", "1,1,NULL\n1,1,NULL\n1,2,NULL\n1,NULL,NULL\n2,2,NULL\n2,3,NULL\nNULL,3,NULL"},
		{"SELECT o.g,o.id,d.v FROM common_rows() o LEFT JOIN LATERAL (SELECT o.id+1 AS v) d ON o.g=2 ORDER BY o.g NULLS LAST,o.id NULLS LAST", "1,1,NULL\n1,1,NULL\n1,2,NULL\n1,NULL,NULL\n2,2,3\n2,3,4\nNULL,3,NULL"},
		{"SELECT o.g,o.id,d.v FROM common_rows() o, LATERAL (SELECT (SELECT o.id) AS v) d ORDER BY o.g NULLS LAST,o.id NULLS LAST", "1,1,1\n1,1,1\n1,2,2\n1,NULL,NULL\n2,2,2\n2,3,3\nNULL,3,3"},
		{"SELECT o.id,d.v,e.w FROM common_rows() o, LATERAL (SELECT o.id+1 AS v) d, LATERAL (SELECT d.v+o.id AS w) e ORDER BY o.id NULLS LAST", "1,2,3\n1,2,3\n2,3,5\n2,3,5\n3,4,7\n3,4,7\nNULL,NULL,NULL"},
		{"SELECT o.g,d.v FROM common_rows() o CROSS JOIN LATERAL (SELECT i.id AS v FROM common_rows() i CROSS JOIN LATERAL (SELECT o.g AS k) e WHERE i.g=e.k GROUP BY i.id ORDER BY i.id LIMIT 1) d WHERE o.id=3 ORDER BY o.g NULLS LAST", "2,2"},
		{"SELECT o.g,(SELECT sum(d.v) FROM common_rows() i CROSS JOIN LATERAL (SELECT i.id+o.id AS v) d WHERE i.g=o.g) FROM common_rows() o WHERE o.id=3 ORDER BY o.g NULLS LAST", "2,11\nNULL,NULL"},
		{"SELECT o.g,o.id,d.v FROM common_rows() o CROSS JOIN LATERAL (SELECT o.id AS v UNION ALL SELECT o.id+1) d WHERE o.g=2 ORDER BY o.id,d.v", "2,2,2\n2,2,3\n2,3,3\n2,3,4"},
		{"SELECT id,o.g FROM common_rows() o JOIN LATERAL (SELECT o.id AS id) d USING(id) ORDER BY o.g NULLS LAST,id", "1,1\n1,1\n2,1\n2,2\n3,2\n3,NULL"},
		{"SELECT o.g,o.id,d.v FROM common_rows() o RIGHT JOIN LATERAL (SELECT 7 AS v) d ON TRUE ORDER BY o.g NULLS LAST,o.id NULLS LAST", "1,1,7\n1,1,7\n1,2,7\n1,NULL,7\n2,2,7\n2,3,7\nNULL,3,7"},
		{"SELECT o.id,d.v FROM common_rows() o, LATERAL (SELECT o.id+1) d(v) WHERE o.g=2 ORDER BY o.id", "2,3\n3,4"},
		{"SELECT o.id,v FROM common_rows() o, LATERAL (SELECT o.id+1 AS v) WHERE o.g=2 ORDER BY o.id", "2,3\n3,4"},
		{"SELECT o.g,(SELECT sum(d.v) FROM common_rows() i CROSS JOIN (SELECT o.id AS v) d WHERE i.g=o.g) FROM common_rows() o WHERE o.id=3 ORDER BY o.g NULLS LAST", "2,6\nNULL,NULL"},
		{"SELECT o.g,(SELECT sum(i.id) FROM common_rows() i CROSS JOIN (SELECT sum(o.id) AS v) d LIMIT 1) FROM common_rows() o GROUP BY o.g ORDER BY o.g NULLS LAST", "1,12\n2,12\nNULL,12"},
		{"SELECT o.g,d.n FROM common_rows() o CROSS JOIN LATERAL (SELECT count(*) AS n FROM common_rows() i TABLESAMPLE 100 PERCENT (bernoulli) WHERE i.g=o.g) d WHERE o.id=3 ORDER BY o.g NULLS LAST", "2,2\nNULL,0"},
		{"SELECT count(*) FROM (SELECT 1 AS x WHERE FALSE) a, common_rows() b FULL JOIN common_rows() c ON b.id=c.id", "0"},
		{"SELECT a.id,b.id,c.id FROM common_rows() a, common_rows() b FULL JOIN (SELECT 3 AS id) c ON b.id=c.id WHERE a.g=2 ORDER BY a.id,b.id NULLS LAST,c.id NULLS LAST", "2,1,NULL\n2,1,NULL\n2,2,NULL\n2,2,NULL\n2,3,3\n2,3,3\n2,NULL,NULL\n3,1,NULL\n3,1,NULL\n3,2,NULL\n3,2,NULL\n3,3,3\n3,3,3\n3,NULL,NULL"},
		{"SELECT a.id,b.id,c.v FROM common_rows() a, common_rows() b JOIN LATERAL (SELECT a.id+b.id AS v) c ON TRUE WHERE a.g=2 AND b.g=2 ORDER BY a.id,b.id", "2,2,4\n2,3,5\n3,2,5\n3,3,6"},
	}
	for _, budget := range []int64{1 << 20, 128} {
		t.Run(fmt.Sprint(budget), func(t *testing.T) {
			db, err := OpenSQLDB(Config{TableFunctions: []TableFunction{commonSQLRows()}, TempDir: t.TempDir(), ManifestDir: t.TempDir(), SpillMemoryBudgetBytes: budget, Workers: 4})
			if err != nil {
				t.Fatal(err)
			}
			defer db.Close()
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
					var got []string
					for rows.Next() {
						values := make([]any, len(columns))
						dest := make([]any, len(values))
						for i := range dest {
							dest[i] = &values[i]
						}
						if err := rows.Scan(dest...); err != nil {
							t.Fatal(err)
						}
						row := make([]string, len(values))
						for i, v := range values {
							if v == nil {
								row[i] = "NULL"
							} else {
								row[i] = fmt.Sprint(v)
							}
						}
						got = append(got, strings.Join(row, ","))
					}
					if err := rows.Err(); err != nil {
						t.Fatal(err)
					}
					if value := strings.Join(got, "\n"); value != tc.want {
						t.Fatalf("got %s want %s", value, tc.want)
					}
				})
			}
		})
	}
}

// Additional LATERAL regression tests.
func TestLateralPreparedBindingsAndVolatileDomain(t *testing.T) {
	var calls atomic.Int64
	fn := ScalarFunction{Name: "lateral_touch", Arguments: []Type{{ID: "BIGINT"}}, Result: Type{ID: "BIGINT"}, NullHandling: ScalarNullPropagate, Eval: func(args []ScalarValue) (ScalarValue, error) { calls.Add(1); return args[0], nil }}
	db, err := OpenSQLDB(Config{TableFunctions: []TableFunction{commonSQLRows()}, ScalarFunctions: []ScalarFunction{fn}, TempDir: t.TempDir(), ManifestDir: t.TempDir(), SpillMemoryBudgetBytes: 128, Workers: 4})
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	var sum string
	if err := db.QueryRowContext(context.Background(), "SELECT sum(d.v) FROM common_rows() o CROSS JOIN LATERAL (SELECT lateral_touch(o.id) AS v) d").Scan(&sum); err != nil || sum != "12" {
		t.Fatalf("sum=%s err=%v", sum, err)
	}
	if calls.Load() != 3 {
		t.Fatalf("inner evaluations=%d want=3 distinct non-NULL parameters", calls.Load())
	}
	stmt, err := db.PrepareContext(context.Background(), "SELECT count(*),sum(d.v) FROM common_rows() o JOIN LATERAL (SELECT o.id+? AS v WHERE o.g=?) d ON TRUE WHERE o.id>?")
	if err != nil {
		t.Fatal(err)
	}
	defer stmt.Close()
	for _, tc := range []struct {
		args  []any
		count int64
		sum   string
	}{{[]any{10, 2, 1}, 2, "25"}, {[]any{0, 1, 0}, 3, "4"}} {
		var count int64
		var sum string
		if err := stmt.QueryRowContext(context.Background(), tc.args...).Scan(&count, &sum); err != nil || count != tc.count || sum != tc.sum {
			t.Fatalf("count=%d sum=%s err=%v", count, sum, err)
		}
	}
}

func TestLateralRestrictionsAndCleanup(t *testing.T) {
	db := testTypedDB(t, 128, commonSQLRows())
	cases := []struct{ sql, kind string }{
		{"SELECT o.id FROM common_rows() o RIGHT JOIN LATERAL (SELECT o.id AS v) d ON TRUE", "INNER or LEFT"},
		{"SELECT o.id FROM common_rows() o FULL JOIN LATERAL (SELECT o.id AS v) d ON TRUE", "INNER or LEFT"},
		{"SELECT o.id FROM common_rows() o SEMI JOIN LATERAL (SELECT o.id AS v) d ON TRUE", "INNER or LEFT"},
		{"SELECT o.id FROM common_rows() o ANTI JOIN LATERAL (SELECT o.id AS v) d ON TRUE", "INNER or LEFT"},
		{"SELECT d.v FROM common_rows() o, LATERAL (SELECT sum(o.id) AS v) d", "LATERAL join cannot contain aggregates"},
		{"SELECT d.v FROM common_rows() o, LATERAL (SELECT (SELECT sum(o.id)) AS v) d", "LATERAL join cannot contain aggregates"},
		{"SELECT o.id FROM common_rows() o CROSS JOIN LATERAL (SELECT i.id FROM common_rows() i WHERE i.g=o.g LIMIT o.id) d", "constant"},
		{"SELECT (SELECT count(*) FROM (SELECT i.id FROM common_rows() i WHERE i.g=o.g) d USING SAMPLE 1 ROWS) FROM common_rows() o", "sampling in correlated"},
		{"SELECT o.id FROM common_rows() o, LATERAL (SELECT i.id FROM common_rows() i WHERE i.g=o.g) d TABLESAMPLE reservoir(1 ROWS)", "sampling in correlated"},
		{"SELECT d.v FROM LATERAL (SELECT o.id AS v) d CROSS JOIN common_rows() o", "column"},
	}
	for _, tc := range cases {
		t.Run(tc.sql, func(t *testing.T) {
			_, err := db.QueryChunks(context.Background(), tc.sql, func(*Chunk) error { return nil })
			if err == nil || !strings.Contains(err.Error(), tc.kind) {
				t.Fatalf("got %v want %s", err, tc.kind)
			}
			if db.MemoryUsed() != 0 {
				t.Fatalf("managed memory leak=%d", db.MemoryUsed())
			}
		})
	}
}

func TestLateralJoinAcrossBatches(t *testing.T) {
	fn := TableFunction{Name: "lateral_many", Columns: []Column{{Name: "id", Type: Type{ID: "BIGINT"}}}, Open: func(context.Context) (BatchReader, error) {
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
	var count, nonnull int64
	sql := "SELECT count(*),count(d.v) FROM lateral_many() o LEFT JOIN LATERAL (SELECT o.id+1 AS v WHERE o.id<4999) d ON TRUE"
	_, err := db.QueryChunks(context.Background(), sql, func(c *Chunk) error {
		a, ok, err := c.Int64(0, 0)
		if err != nil {
			return err
		}
		if !ok {
			return fmt.Errorf("NULL count")
		}
		b, ok, err := c.Int64(0, 1)
		if err != nil {
			return err
		}
		if !ok {
			return fmt.Errorf("NULL non-NULL count")
		}
		count, nonnull = a, b
		return nil
	})
	if err != nil || count != 5000 || nonnull != 4999 {
		t.Fatalf("count=%d nonnull=%d err=%v", count, nonnull, err)
	}
	if db.MemoryUsed() != 0 {
		t.Fatalf("managed memory leak=%d", db.MemoryUsed())
	}
}
