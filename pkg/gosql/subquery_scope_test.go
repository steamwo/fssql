package gosql

import (
	"context"
	"fmt"
	"strings"
	"testing"
)

// Golden results are independently generated with DuckDB 2.0.0 and also
// retained as SQL.SUBQUERY.SCOPE cases in the compatibility corpus.
func TestOuterAggregateSubqueries(t *testing.T) {
	cases := []struct{ sql, want string }{
		{"SELECT (SELECT sum(o.id)) FROM common_rows() o", "12"},
		{"SELECT o.g,(SELECT sum(o.id)),(SELECT count(o.id)) FROM common_rows() o GROUP BY o.g ORDER BY o.g NULLS LAST", "1,4,3\n2,5,2\nNULL,3,1"},
		{"SELECT (SELECT sum(o.id) FROM common_rows() i WHERE FALSE) FROM common_rows() o", "NULL"},
		{"SELECT (SELECT sum(o.id) FROM common_rows() i LIMIT 1) FROM common_rows() o", "12"},
		{"SELECT (SELECT 1 WHERE sum(o.id)>0) FROM common_rows() o", "1"},
		{"SELECT (SELECT (SELECT sum(o.id))) FROM common_rows() o", "12"},
		{"SELECT (SELECT sum(i.id+sum(o.id)) FROM common_rows() i) FROM common_rows() o", "84"},
		{"SELECT o.g,(SELECT count(DISTINCT o.id)),(SELECT sum(DISTINCT o.id)+1) FROM common_rows() o GROUP BY o.g ORDER BY o.g NULLS LAST", "1,2,4\n2,2,6\nNULL,1,4"},
		{"SELECT o.g FROM common_rows() o GROUP BY o.g HAVING (SELECT sum(o.id))>4 ORDER BY o.g", "2"},
		{"SELECT (SELECT sum(o.id)),(SELECT count(o.id)) FROM common_rows() o WHERE FALSE", "NULL,0"},
		{"SELECT o.g,(SELECT sum(i.id+sum(o.id)) FROM common_rows() i WHERE i.g=o.g) FROM common_rows() o GROUP BY o.g ORDER BY o.g NULLS LAST", "1,16\n2,15\nNULL,NULL"},
		{"SELECT (SELECT sum(i.id+(SELECT sum(o.id))) FROM common_rows() i) FROM common_rows() o", "84"},
		{"SELECT (SELECT sum(o.id) FROM (SELECT 1 AS x) d) FROM common_rows() o", "12"},
		{"SELECT (SELECT sum(o.id) FROM common_rows() i GROUP BY i.g LIMIT 1) FROM common_rows() o", "12"},
		{"SELECT (SELECT sum((SELECT o.id))) FROM common_rows() o ORDER BY o.id NULLS LAST", "1\n1\n2\n2\n3\n3\nNULL"},
		{"SELECT (SELECT (SELECT sum(i.id+o.id)) FROM common_rows() i WHERE i.g=o.g) FROM common_rows() o WHERE o.id=3 ORDER BY o.g NULLS LAST", "11\nNULL"},
		{"SELECT (SELECT sum(o.id+(SELECT 1))) FROM common_rows() o", "18"},
		{"SELECT (SELECT sum(o.id+(SELECT o.id))) FROM common_rows() o", "24"},
		{"SELECT (SELECT sum(o.id+(SELECT count(*) FROM common_rows()))) FROM common_rows() o", "54"},
		{"SELECT o.g,(SELECT sum(o.id+(SELECT o.id))) FROM common_rows() o GROUP BY o.g ORDER BY o.g NULLS LAST", "1,8\n2,10\nNULL,6"},
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

func TestOuterAggregateScopeErrorsAndCleanup(t *testing.T) {
	db := testTypedDB(t, 128, commonSQLRows())
	cases := []struct{ sql, kind string }{
		{"SELECT o.id,(SELECT sum(o.id)) FROM common_rows() o", "GROUP BY"},
		{"SELECT sum((SELECT sum(o.id))) FROM common_rows() o", "nested aggregate"},
		{"SELECT (SELECT sum(sum(o.id))) FROM common_rows() o", "nested aggregate"},
		{"SELECT (SELECT sum(o.id) FROM common_rows() i) FROM common_rows() o", "more than one row"},
		{"SELECT o.g+1,(SELECT o.g) FROM common_rows() o GROUP BY o.g+1", "ungrouped correlated"},
		{"SELECT o.id FROM common_rows() o WHERE EXISTS(SELECT 1 WHERE sum(o.id)>0)", "aggregate is not allowed in WHERE"},
		{"SELECT (SELECT sum(o.id+(SELECT i.id)) FROM common_rows() i) FROM common_rows() o", "removed local scope"},
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
