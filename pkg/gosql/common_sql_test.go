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

func commonSQLRows() TableFunction {
	return TableFunction{Name: "common_rows", Columns: []Column{{Name: "g", Type: Type{ID: "BIGINT"}, Nullable: true}, {Name: "id", Type: Type{ID: "BIGINT"}, Nullable: true}, {Name: "label", Type: Type{ID: "VARCHAR"}, Nullable: true}}, Open: func(context.Context) (BatchReader, error) {
		done := false
		return &batchReaderFunc{next: func(_ context.Context, w BatchWriter) (int, error) {
			if done {
				return 0, io.EOF
			}
			done = true
			rows := [][2]int64{{1, 1}, {1, 1}, {1, 2}, {1, 0}, {2, 2}, {2, 3}, {0, 3}}
			labels := []string{"a_x", "a_x", "βa", "", "b", "ab", "_%"}
			for i, row := range rows {
				if i == 6 {
					if err := w.SetNull(i, 0); err != nil {
						return 0, err
					}
				} else if err := w.SetInt64(i, 0, row[0]); err != nil {
					return 0, err
				}
				if i == 3 {
					if err := w.SetNull(i, 1); err != nil {
						return 0, err
					}
					if err := w.SetNull(i, 2); err != nil {
						return 0, err
					}
				} else {
					if err := w.SetInt64(i, 1, row[1]); err != nil {
						return 0, err
					}
					if err := w.SetString(i, 2, labels[i]); err != nil {
						return 0, err
					}
				}
			}
			return len(rows), io.EOF
		}}, nil
	}}
}

func TestCommonSQLPredicatesSetsAndAggregates(t *testing.T) {
	for _, budget := range []int64{1 << 20, 1} {
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
				{"SELECT 1 IN (1,NULL), 1 IN (2,NULL), 1 NOT IN (2,3), NOT 1 IN (1,2), NULL IN (NULL)", [][]any{{true, nil, true, false, nil}}},
				{"SELECT CAST('18446744073709551615' AS UBIGINT) IN (1,CAST('18446744073709551615' AS UBIGINT)), CAST('NaN' AS DOUBLE) IN (CAST('NaN' AS DOUBLE))", [][]any{{true, true}}},
				{"SELECT 5 BETWEEN 1 AND 10, 5 NOT BETWEEN 1 AND 3, NULL BETWEEN 1 AND 3, 0 BETWEEN 1 AND NULL", [][]any{{true, true, nil, false}}},
				{"SELECT '你好' LIKE '你_', 'a_b' LIKE 'a\\_b', 'a_b' LIKE 'a\\_b' ESCAPE '\\', 'abc' NOT LIKE 'a%', NULL LIKE '%'", [][]any{{true, false, true, false, nil}}},
				{"SELECT id FROM common_rows() WHERE id IN (1,3,NULL) AND label LIKE '%a%' ORDER BY id", [][]any{{int64(1)}, {int64(1)}, {int64(3)}}},
				{"SELECT EXISTS(SELECT 1), EXISTS(SELECT 1 WHERE FALSE), NOT EXISTS(SELECT id FROM common_rows()), EXISTS(SELECT NULL)", [][]any{{true, false, false, true}}},
				{"SELECT EXISTS(SELECT id,label FROM common_rows()), EXISTS(SELECT CAST('bad' AS INTEGER)), EXISTS(SELECT count(*) WHERE FALSE), EXISTS(SELECT 1 LIMIT 0)", [][]any{{true, true, true, false}}},
				{"SELECT EXISTS(SELECT CAST('bad' AS INTEGER) UNION ALL SELECT 1), EXISTS(SELECT * FROM (SELECT CAST('bad' AS INTEGER) AS id) AS d)", [][]any{{true, true}}},
				{"SELECT EXISTS(SELECT * FROM (SELECT sum(CAST('bad' AS INTEGER)) AS id) AS d)", [][]any{{true}}},
				{"SELECT EXISTS(SELECT DISTINCT id FROM common_rows() OFFSET 3), EXISTS(SELECT DISTINCT id FROM common_rows() OFFSET 4)", [][]any{{true, false}}},
				{"SELECT 1 AS k UNION ALL SELECT 2 UNION ALL SELECT 1 ORDER BY k", [][]any{{int64(1)}, {int64(1)}, {int64(2)}}},
				{"SELECT 3 AS k UNION ALL SELECT 1 UNION ALL SELECT 2 ORDER BY k LIMIT 1 OFFSET 1", [][]any{{int64(2)}}},
				{"SELECT NULL AS k UNION ALL SELECT 2 ORDER BY k NULLS FIRST", [][]any{{nil}, {int64(2)}}},
				{"WITH a AS (SELECT 1 AS k) SELECT k FROM a UNION ALL SELECT k+1 FROM a ORDER BY k", [][]any{{int64(1)}, {int64(2)}}},
				{"SELECT count(*) FROM (SELECT 1 AS k UNION ALL SELECT 1 UNION ALL SELECT 2) AS u", [][]any{{int64(3)}}},
				{"SELECT count(DISTINCT g), count(g), count(*), count(DISTINCT id) FROM common_rows()", [][]any{{int64(2), int64(6), int64(7), int64(3)}}},
				{"SELECT g,count(DISTINCT id),count(id),sum(id),count(DISTINCT label) FROM common_rows() GROUP BY g ORDER BY g NULLS FIRST", [][]any{{nil, int64(1), int64(1), "3", int64(1)}, {int64(1), int64(2), int64(3), "4", int64(2)}, {int64(2), int64(2), int64(2), "5", int64(2)}}},
				{"SELECT count(DISTINCT id) FROM common_rows()", [][]any{{int64(3)}}},
				{"SELECT count(DISTINCT label) FROM common_rows()", [][]any{{int64(5)}}},
				{"SELECT count(DISTINCT NULL), count(NULL)", [][]any{{int64(0), int64(0)}}},
				{"SELECT g,count(DISTINCT id) FROM common_rows() GROUP BY g HAVING count(DISTINCT id)>1 ORDER BY g", [][]any{{int64(1), int64(2)}, {int64(2), int64(2)}}},
			}
			for _, tc := range cases {
				t.Run(tc.sql, func(t *testing.T) {
					rows, err := db.QueryContext(context.Background(), tc.sql)
					if err != nil {
						t.Fatal(err)
					}
					defer rows.Close()
					names, err := rows.Columns()
					if err != nil {
						t.Fatal(err)
					}
					var got [][]any
					for rows.Next() {
						values := make([]any, len(names))
						dest := make([]any, len(names))
						for i := range dest {
							dest[i] = &values[i]
						}
						if err := rows.Scan(dest...); err != nil {
							t.Fatal(err)
						}
						got = append(got, values)
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

func TestCommonSQLJoinUsing(t *testing.T) {
	db, err := OpenSQLDB(Config{TempDir: t.TempDir(), ManifestDir: t.TempDir(), SpillMemoryBudgetBytes: 1})
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	prefix := "WITH a AS (SELECT 1 AS k,'L1' AS l UNION ALL SELECT 2,'L2' UNION ALL SELECT CAST(NULL AS INTEGER),'Ln'), b AS (SELECT 2 AS k,'R2' AS r UNION ALL SELECT 3,'R3' UNION ALL SELECT CAST(NULL AS INTEGER),'Rn') "
	cases := []struct {
		join string
		want [][]any
	}{
		{"INNER", [][]any{{int64(2), int64(2), int64(2), "L2", "R2"}}},
		{"LEFT", [][]any{{int64(1), int64(1), nil, "L1", nil}, {int64(2), int64(2), int64(2), "L2", "R2"}, {nil, nil, nil, "Ln", nil}}},
		{"RIGHT", [][]any{{int64(2), int64(2), int64(2), "L2", "R2"}, {int64(3), nil, int64(3), nil, "R3"}, {nil, nil, nil, nil, "Rn"}}},
		{"FULL", [][]any{{int64(1), int64(1), nil, "L1", nil}, {int64(2), int64(2), int64(2), "L2", "R2"}, {int64(3), nil, int64(3), nil, "R3"}, {nil, nil, nil, "Ln", nil}, {nil, nil, nil, nil, "Rn"}}},
	}
	for _, tc := range cases {
		t.Run(tc.join, func(t *testing.T) {
			rows, err := db.QueryContext(context.Background(), prefix+"SELECT k,a.k,b.k,l,r FROM a "+tc.join+" JOIN b USING(k) ORDER BY 1 NULLS LAST,l NULLS LAST,r NULLS LAST")
			if err != nil {
				t.Fatal(err)
			}
			defer rows.Close()
			var got [][]any
			for rows.Next() {
				row := make([]any, 5)
				dest := make([]any, 5)
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
	var k int64
	if err := db.QueryRowContext(context.Background(), "SELECT k FROM (SELECT CAST(1 AS TINYINT) AS k) a JOIN (SELECT CAST(1 AS BIGINT) AS k) b USING(k)").Scan(&k); err != nil || k != 1 {
		t.Fatalf("coerced USING key: %d %v", k, err)
	}
	rows, err := db.QueryContext(context.Background(), "SELECT * FROM (SELECT 10 AS v,1 AS k) a JOIN (SELECT 1 AS k,20 AS w) b USING(k)")
	if err != nil {
		t.Fatal(err)
	}
	defer rows.Close()
	names, err := rows.Columns()
	if err != nil {
		t.Fatal(err)
	}
	if !reflect.DeepEqual(names, []string{"v", "k", "w"}) {
		t.Fatalf("USING star columns: %v", names)
	}
	if err := db.QueryRowContext(context.Background(), "SELECT k FROM (SELECT 1 AS k) a JOIN (SELECT 1 AS k) b USING(k) JOIN (SELECT 1 AS k) c USING(k)").Scan(&k); err != nil || k != 1 {
		t.Fatalf("chained USING: %d %v", k, err)
	}
	var v int64
	if err := db.QueryRowContext(context.Background(), "SELECT * FROM (SELECT CAST(1 AS TINYINT) AS k,CAST(2 AS SMALLINT) AS v) a JOIN (SELECT CAST(1 AS BIGINT) AS k,CAST(2 AS INTEGER) AS v) b USING(v,k)").Scan(&k, &v); err != nil || k != 1 || v != 2 {
		t.Fatalf("composite USING and output order: %d %d %v", k, v, err)
	}
}

func TestCommonSQLDistinctAcrossBatchesAndCleanup(t *testing.T) {
	fn := TableFunction{Name: "many_common_rows", Columns: []Column{{Name: "g", Type: Type{ID: "BIGINT"}}, {Name: "id", Type: Type{ID: "BIGINT"}, Nullable: true}, {Name: "label", Type: Type{ID: "VARCHAR"}}}, Open: func(context.Context) (BatchReader, error) {
		next := 0
		return &batchReaderFunc{next: func(_ context.Context, w BatchWriter) (int, error) {
			if next >= 5000 {
				return 0, io.EOF
			}
			n := min(w.Capacity(), 5000-next)
			for row := 0; row < n; row++ {
				i := next + row
				if err := w.SetInt64(row, 0, int64(i%3)); err != nil {
					return 0, err
				}
				if i%11 == 0 {
					if err := w.SetNull(row, 1); err != nil {
						return 0, err
					}
				} else if err := w.SetInt64(row, 1, int64(i%17)); err != nil {
					return 0, err
				}
				if err := w.SetString(row, 2, fmt.Sprintf("值%d", i%13)); err != nil {
					return 0, err
				}
			}
			next += n
			return n, nil
		}}, nil
	}}
	for _, budget := range []int64{1 << 20, 128} {
		t.Run(fmt.Sprint(budget), func(t *testing.T) {
			db := testTypedDB(t, budget, fn)
			var got [][3]int64
			_, err := db.QueryChunks(context.Background(), "SELECT g,count(DISTINCT id),count(DISTINCT label) FROM many_common_rows() GROUP BY g ORDER BY g", func(c *Chunk) error {
				for r := 0; r < c.RowCount(); r++ {
					var row [3]int64
					for col := range row {
						v, ok, err := c.Int64(r, col)
						if err != nil || !ok {
							return fmt.Errorf("value: %v", err)
						}
						row[col] = v
					}
					got = append(got, row)
				}
				return nil
			})
			if err != nil {
				t.Fatal(err)
			}
			if !reflect.DeepEqual(got, [][3]int64{{0, 17, 13}, {1, 17, 13}, {2, 17, 13}}) {
				t.Fatalf("cross-batch DISTINCT: %v", got)
			}
			if used := db.MemoryUsed(); used != 0 {
				t.Fatalf("managed memory leak: %d", used)
			}
		})
	}
	db := testTypedDB(t, 128, commonSQLRows())
	for _, sql := range []string{"SELECT CAST('bad' AS BIGINT) AS x UNION ALL SELECT id FROM common_rows() ORDER BY x", "SELECT id FROM common_rows() UNION ALL SELECT CAST('bad' AS BIGINT)", "SELECT count(DISTINCT id)+CAST('bad' AS BIGINT) FROM common_rows()"} {
		_, err := db.QueryChunks(context.Background(), sql, func(*Chunk) error { return nil })
		if err == nil {
			t.Fatalf("expected execution error: %s", sql)
		}
		if used := db.MemoryUsed(); used != 0 {
			t.Fatalf("error path managed memory leak: %d (%v)", used, err)
		}
	}
}

func TestCommonSQLPreparedPredicatesAndSingleEvaluation(t *testing.T) {
	var calls atomic.Int64
	fn := ScalarFunction{Name: "predicate_once", Result: Type{ID: "BIGINT"}, Eval: func([]ScalarValue) (ScalarValue, error) {
		return ScalarValue{Type: Type{ID: "BIGINT"}, Valid: true, Int64: calls.Add(1)}, nil
	}}
	db, err := OpenSQLDB(Config{ScalarFunctions: []ScalarFunction{fn}, TempDir: t.TempDir(), ManifestDir: t.TempDir()})
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	var result bool
	for _, sql := range []string{"SELECT predicate_once() IN (1,2,3)", "SELECT predicate_once() BETWEEN 1 AND 3"} {
		if err := db.QueryRowContext(context.Background(), sql).Scan(&result); err != nil || !result {
			t.Fatalf("predicate result: %v %v", result, err)
		}
	}
	if calls.Load() != 2 {
		t.Fatalf("lhs evaluated %d times", calls.Load())
	}
	stmt, err := db.PrepareContext(context.Background(), "SELECT ? IN (?,NULL) AS value UNION ALL SELECT ? LIKE ?")
	if err != nil {
		t.Fatal(err)
	}
	defer stmt.Close()
	for i := 0; i < 2; i++ {
		rows, err := stmt.QueryContext(context.Background(), 1, 1, "你好", "你%")
		if err != nil {
			t.Fatal(err)
		}
		count := 0
		for rows.Next() {
			if err := rows.Scan(&result); err != nil || !result {
				t.Fatalf("prepared predicates: %v %v", result, err)
			}
			count++
		}
		err = rows.Err()
		rows.Close()
		if err != nil || count != 2 {
			t.Fatalf("prepared UNION rows=%d err=%v", count, err)
		}
	}
	for _, sql := range []string{"SELECT 'a' LIKE 'a\\' ESCAPE '\\'", "SELECT 'a' LIKE 'a' ESCAPE 'xx'", "SELECT 1 IN ()", "SELECT 1 INTERSECT BY NAME SELECT 2", "SELECT sum(DISTINCT 1,2)", "SELECT * FROM (SELECT 1 AS k) a JOIN (SELECT 1 AS k) b USING(missing)", "SELECT * FROM (SELECT 1 AS k) a JOIN (SELECT 1 AS k) b USING(k,k)"} {
		var value any
		if err := db.QueryRowContext(context.Background(), sql).Scan(&value); err == nil {
			t.Errorf("accepted invalid/unsupported SQL: %s", sql)
		}
	}
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	if _, err := stmt.QueryContext(ctx, 1, 1, "x", "_"); err == nil || !strings.Contains(err.Error(), "canceled") {
		t.Fatalf("canceled statement: %v", err)
	}
}
