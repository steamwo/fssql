package gosql

import (
	"context"
	"fmt"
	"strings"
	"sync/atomic"
	"testing"
)

// Golden results are independently generated with DuckDB 2.0.0 and also
// retained as SQL.SUBQUERY.NESTED cases in the compatibility corpus.
func TestNestedScalarDependencies(t *testing.T) {
	cases := []struct{ sql, want string }{
		{"SELECT (SELECT (SELECT 7)),(SELECT 1+(SELECT 2))", "7,3"},
		{"SELECT (SELECT (SELECT id FROM common_rows() WHERE FALSE))", "NULL"},
		{"SELECT (SELECT sum((SELECT 3)) FROM common_rows())", "21"},
		{"SELECT count(*) FROM common_rows() WHERE (SELECT (SELECT 1))=1", "7"},
		{"SELECT id FROM common_rows() WHERE g=2 ORDER BY (SELECT (SELECT 1)),id", "2\n3"},
		{"SELECT count(*) FROM common_rows() HAVING count(*)>(SELECT (SELECT 0))", "7"},
		{"SELECT (SELECT (SELECT 2)) IN (SELECT id FROM common_rows())", "true"},
		{"SELECT EXISTS(SELECT (SELECT CAST('bad' AS INTEGER)))", "true"},
		{"SELECT EXISTS(SELECT * FROM (SELECT (SELECT CAST('bad' AS INTEGER)) AS v) d)", "true"},
		{"SELECT o.g,EXISTS(SELECT (SELECT CAST('bad' AS INTEGER)) FROM common_rows() i WHERE i.g=o.g) FROM common_rows() o WHERE o.id=3 ORDER BY o.g NULLS LAST", "2,true\nNULL,false"},
		{"SELECT EXISTS(SELECT sum(o.id)+(SELECT CAST('bad' AS INTEGER)) FROM common_rows() i) FROM common_rows() o", "true"},
		{"SELECT (SELECT (SELECT count(*) FROM common_rows() i WHERE i.g=o.g) FROM common_rows() o WHERE o.id=3 AND o.g=2)", "2"},
		{"SELECT o.g,d.v FROM common_rows() o, LATERAL (SELECT (SELECT (SELECT o.id)) AS v) d WHERE o.id=3 ORDER BY o.g NULLS LAST", "2,3\nNULL,3"},
		{"SELECT (SELECT d.v FROM (SELECT (SELECT 9) AS v) d)", "9"},
		{"SELECT (SELECT (SELECT 1)),(SELECT (SELECT 2))", "1,2"},
		{"SELECT (SELECT (SELECT NULL)) IN (SELECT 1)", "NULL"},
		{"SELECT EXISTS(SELECT 1 ORDER BY (SELECT (SELECT 1)))", "true"},
		{"SELECT (SELECT EXISTS(SELECT (SELECT (SELECT CAST('bad' AS INTEGER)))))", "true"},
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

func TestNestedScalarReusePruningAndCleanup(t *testing.T) {
	var calls atomic.Int64
	fn := ScalarFunction{Name: "nested_tick", Result: Type{ID: "BIGINT"}, Eval: func([]ScalarValue) (ScalarValue, error) {
		calls.Add(1)
		return ScalarValue{Type: Type{ID: "BIGINT"}, Valid: true, Int64: 7}, nil
	}}
	db, err := OpenSQLDB(Config{TableFunctions: []TableFunction{commonSQLRows()}, ScalarFunctions: []ScalarFunction{fn}, TempDir: t.TempDir(), ManifestDir: t.TempDir(), SpillMemoryBudgetBytes: 128, Workers: 4})
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	for run := int64(1); run <= 2; run++ {
		var sum string
		if err := db.QueryRowContext(context.Background(), "SELECT sum((SELECT (SELECT nested_tick()))) FROM common_rows()").Scan(&sum); err != nil || sum != "49" {
			t.Fatalf("sum=%s err=%v", sum, err)
		}
		if calls.Load() != run {
			t.Fatalf("inner calls=%d want=%d", calls.Load(), run)
		}
	}
	var found bool
	if err := db.QueryRowContext(context.Background(), "SELECT EXISTS(SELECT * FROM (SELECT (SELECT nested_tick()) AS v) d)").Scan(&found); err != nil || !found {
		t.Fatalf("exists=%v err=%v", found, err)
	}
	if calls.Load() != 2 {
		t.Fatalf("pruned dependency evaluated: calls=%d", calls.Load())
	}
	typed := testTypedDB(t, 128, commonSQLRows())
	for _, sql := range []string{
		"SELECT (SELECT (SELECT id FROM common_rows()))",
		"SELECT (SELECT (SELECT 1) FROM common_rows())",
	} {
		_, err := typed.QueryChunks(context.Background(), sql, func(*Chunk) error { return nil })
		if err == nil || !strings.Contains(err.Error(), "more than one row") {
			t.Fatalf("scalar cardinality: %v", err)
		}
		if typed.MemoryUsed() != 0 {
			t.Fatalf("managed memory leak=%d", typed.MemoryUsed())
		}
	}
}
