package gosql

import (
	"context"
	"fmt"
	"reflect"
	"testing"
)

func TestSQLCompatibilitySamplingAndComposition(t *testing.T) {
	for _, budget := range []int64{1 << 20, 1} {
		t.Run(fmt.Sprint(budget), func(t *testing.T) {
			db, err := OpenSQLDB(Config{TableFunctions: []TableFunction{phase5RowsFunction()}, TempDir: t.TempDir(), ManifestDir: t.TempDir(), SpillMemoryBudgetBytes: budget})
			if err != nil {
				t.Fatal(err)
			}
			defer db.Close()
			cases := []struct {
				sql  string
				want []any
			}{
				{"SELECT count(*) FROM phase5_rows() TABLESAMPLE reservoir(3 ROWS) REPEATABLE(42)", []any{int64(3)}},
				{"SELECT count(*) FROM phase5_rows() USING SAMPLE 3", []any{int64(3)}},
				{"SELECT count(*) FROM phase5_rows() USING SAMPLE 100 PERCENT", []any{int64(5)}},
				{"SELECT count(*) FROM phase5_rows() USING SAMPLE 0% (system, 42)", []any{int64(0)}},
				{"SELECT count(*) FROM phase5_rows() USING SAMPLE 100% (bernoulli, 42)", []any{int64(5)}},
				{"SELECT count(*) FROM phase5_rows() USING SAMPLE reservoir(40%) REPEATABLE(42)", []any{int64(2)}},
				{"SELECT count(*) FROM phase5_rows() USING SAMPLE reservoir(50%) REPEATABLE(42)", []any{int64(3)}},
				{"SELECT count(*) FROM phase5_rows() USING SAMPLE reservoir(0%)", []any{int64(0)}},
				{"SELECT count(*) FROM phase5_rows() USING SAMPLE reservoir(100%)", []any{int64(5)}},
				{"SELECT sum(d.id), (SELECT count(*) FROM phase5_rows()) FROM (SELECT id FROM phase5_rows()) AS d", []any{"15", int64(5)}},
				{"SELECT count(*) FROM (SELECT id FROM phase5_rows()) AS d WHERE d.id > (SELECT 2)", []any{int64(3)}},
				{"SELECT d.n FROM (SELECT count(*) AS n FROM phase5_rows() WHERE id > (SELECT 2)) AS d", []any{int64(3)}},
				{"WITH c AS (SELECT count(*) AS n FROM phase5_rows() WHERE id > (SELECT 2)) SELECT a.n, b.n, (SELECT 7) FROM c AS a CROSS JOIN c AS b", []any{int64(3), int64(3), int64(7)}},
				{"SELECT count(*) FROM phase5_rows() HAVING count(*) = (SELECT 5)", []any{int64(5)}},
				{"SELECT sum(id), (SELECT 7 WHERE FALSE) FROM phase5_rows()", []any{"15", nil}},
				{"SELECT count(*) FROM phase5_rows() WHERE id > 2 USING SAMPLE 100%", []any{int64(3)}},
				{"SELECT count(*) FROM phase5_rows() AS a JOIN phase5_rows() AS b ON a.id = b.id USING SAMPLE 3 ROWS", []any{int64(3)}},
				{"SELECT count(a.id) FROM phase5_rows() AS a TABLESAMPLE 3 ROWS JOIN phase5_rows() AS b ON a.id = b.id", []any{int64(3)}},
				{"SELECT count(*) FROM phase5_rows() TABLESAMPLE 4 ROWS WHERE id > 0 USING SAMPLE 3 ROWS", []any{int64(3)}},
				{"SELECT CAST(7 AS BIGINT) % CAST(3 AS BIGINT), CAST(-7 AS BIGINT) % 3, CAST(7 AS BIGINT) % -3", []any{int64(1), int64(-1), int64(1)}},
				{"SELECT CAST(NULL AS BIGINT) % 2", []any{nil}},
				{"SELECT CAST('170141183460469231731687303715884105727' AS HUGEINT), CAST('-170141183460469231731687303715884105728' AS HUGEINT)", []any{"170141183460469231731687303715884105727", "-170141183460469231731687303715884105728"}},
				{"SELECT CAST(CAST('123' AS HUGEINT) AS BIGINT), CAST(CAST('123' AS HUGEINT) AS DECIMAL(10,2))", []any{int64(123), "123.00"}},
			}
			for _, tc := range cases {
				t.Run(tc.sql, func(t *testing.T) {
					got := make([]any, len(tc.want))
					dest := make([]any, len(tc.want))
					for i := range dest {
						dest[i] = &got[i]
					}
					if err := db.QueryRowContext(context.Background(), tc.sql).Scan(dest...); err != nil {
						t.Fatal(err)
					}
					if !reflect.DeepEqual(got, tc.want) {
						t.Fatalf("got %#v want %#v", got, tc.want)
					}
				})
			}
			for _, sql := range []string{"SELECT CAST(7 AS BIGINT) % 0", "SELECT CAST('-9223372036854775808' AS BIGINT) % -1", "SELECT CAST('170141183460469231731687303715884105728' AS HUGEINT)", "SELECT CAST(CAST('9223372036854775808' AS HUGEINT) AS BIGINT)"} {
				var value any
				if err := db.QueryRowContext(context.Background(), sql).Scan(&value); err == nil {
					t.Errorf("expected error: %s", sql)
				}
			}
		})
	}
}

func TestSQLCompatibilitySamplingRejectsInvalidSizes(t *testing.T) {
	db := phase5DB(t, 1<<20)
	for _, suffix := range []string{"bernoulli(3 ROWS)", "system(3 ROWS)", "bernoulli(101%)", "-1 ROWS", "3.5 ROWS", "3 (unknown)", "3 (reservoir, 1) REPEATABLE(2)"} {
		_, err := db.QueryChunks(context.Background(), "SELECT id FROM phase5_rows() USING SAMPLE "+suffix, func(*Chunk) error { return nil })
		if err == nil {
			t.Errorf("accepted invalid sample %s", suffix)
		}
	}
	if used := db.MemoryUsed(); used != 0 {
		t.Fatalf("managed memory leak: %d", used)
	}
	var value int64
	sqlDB, err := OpenSQLDB(Config{TempDir: t.TempDir(), ManifestDir: t.TempDir()})
	if err != nil {
		t.Fatal(err)
	}
	defer sqlDB.Close()
	if err := sqlDB.QueryRowContext(context.Background(), "SELECT (SELECT (SELECT 1))").Scan(&value); err != nil || value != 1 {
		t.Fatalf("nested scalar value=%d err=%v", value, err)
	}
}

func TestSQLCompatibilityGroupedScalarSubqueries(t *testing.T) {
	db, err := OpenSQLDB(Config{TableFunctions: []TableFunction{phase5RowsFunction()}, TempDir: t.TempDir(), ManifestDir: t.TempDir()})
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	rows, err := db.QueryContext(context.Background(), "SELECT grp, count(*), (SELECT 7) FROM phase5_rows() GROUP BY grp HAVING count(*) > (SELECT 1) ORDER BY grp")
	if err != nil {
		t.Fatal(err)
	}
	defer rows.Close()
	var got [][3]int64
	for rows.Next() {
		var row [3]int64
		if err := rows.Scan(&row[0], &row[1], &row[2]); err != nil {
			t.Fatal(err)
		}
		got = append(got, row)
	}
	if err := rows.Err(); err != nil {
		t.Fatal(err)
	}
	if !reflect.DeepEqual(got, [][3]int64{{1, 3, 7}, {2, 2, 7}}) {
		t.Fatalf("grouped scalar results: %v", got)
	}
}
