package gosql

import (
	"context"
	"errors"
	"fmt"
	"io"
	"strings"
	"testing"
)

func TestSetOperationsPrepared(t *testing.T) {
	db, err := OpenSQLDB(Config{TempDir: t.TempDir(), ManifestDir: t.TempDir()})
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	for op, count := range map[string]int{"UNION": 1, "UNION ALL": 3, "INTERSECT": 1, "INTERSECT ALL": 1, "EXCEPT": 0, "EXCEPT ALL": 1} {
		t.Run(op, func(t *testing.T) {
			stmt, err := db.PrepareContext(context.Background(), "WITH l AS (SELECT ? AS v UNION ALL SELECT ?), r AS (SELECT ? AS v) SELECT v FROM l "+op+" SELECT v FROM r ORDER BY v")
			if err != nil {
				t.Fatal(err)
			}
			defer stmt.Close()
			for _, v := range []int64{2, 7} {
				rows, err := stmt.QueryContext(context.Background(), v, v, v)
				if err != nil {
					t.Fatal(err)
				}
				n := 0
				for rows.Next() {
					var got int64
					if err := rows.Scan(&got); err != nil {
						rows.Close()
						t.Fatal(err)
					}
					if got != v {
						rows.Close()
						t.Fatalf("value=%d want=%d", got, v)
					}
					n++
				}
				if err := rows.Err(); err != nil {
					rows.Close()
					t.Fatal(err)
				}
				rows.Close()
				if n != count {
					t.Fatalf("rows=%d want=%d", n, count)
				}
			}
		})
	}
}

func setRowsFunction() TableFunction {
	return TableFunction{Name: "set_rows", Columns: []Column{{Name: "id", Type: Type{ID: "BIGINT"}}, {Name: "v", Type: Type{ID: "BIGINT"}, Nullable: true}}, Open: func(context.Context) (BatchReader, error) {
		next := 0
		return &batchReaderFunc{next: func(ctx context.Context, w BatchWriter) (int, error) {
			if err := ctx.Err(); err != nil {
				return 0, err
			}
			if next >= 4097 {
				return 0, io.EOF
			}
			n := min(w.Capacity(), 4097-next)
			for r := 0; r < n; r++ {
				i := next + r
				if err := w.SetInt64(r, 0, int64(i)); err != nil {
					return 0, err
				}
				if i%7 == 0 {
					if err := w.SetNull(r, 1); err != nil {
						return 0, err
					}
				} else if err := w.SetInt64(r, 1, int64(i%17)); err != nil {
					return 0, err
				}
			}
			next += n
			return n, nil
		}}, nil
	}}
}

func TestSetOperationsAcrossBatchesAndCleanup(t *testing.T) {
	// The small policy forces the reused hash/distinct/sort paths to spill.
	for _, budget := range []int64{1 << 20, 4096} {
		t.Run(fmt.Sprint(budget), func(t *testing.T) {
			db := testTypedDB(t, budget, setRowsFunction())
			for op, want := range map[string]int64{"UNION": 18, "INTERSECT": 18, "EXCEPT": 0, "INTERSECT ALL": 2048, "EXCEPT ALL": 2049} {
				query := "SELECT count(*) FROM (SELECT v FROM set_rows() " + op + " SELECT v FROM set_rows() WHERE id < 2048) s"
				var got int64
				_, err := db.QueryChunks(context.Background(), query, func(c *Chunk) error {
					v, ok, e := c.Int64(0, 0)
					if e != nil {
						return e
					}
					if !ok {
						return fmt.Errorf("unexpected NULL count")
					}
					got = v
					return nil
				})
				if err != nil || got != want {
					t.Fatalf("%s: count=%d want=%d err=%v", op, got, want, err)
				}
				if used := db.MemoryUsed(); used != 0 {
					t.Fatalf("%s: managed leak=%d", op, used)
				}
			}
			for _, op := range []string{"UNION", "INTERSECT ALL", "EXCEPT ALL"} {
				_, err := db.QueryChunks(context.Background(), "SELECT v FROM set_rows() "+op+" SELECT CAST('bad' AS BIGINT)", func(*Chunk) error { return nil })
				if err == nil {
					t.Fatalf("%s: expected conversion error", op)
				}
				if used := db.MemoryUsed(); used != 0 {
					t.Fatalf("%s: error path leak=%d", op, used)
				}
			}
		})
	}
}

func TestSetOperationsRejectedSyntaxAndCancel(t *testing.T) {
	db := testTypedDB(t, 4096, setRowsFunction())
	for _, sql := range []string{"SELECT 1 INTERSECT BY NAME SELECT 2", "SELECT 1 LIMIT 1 LIMIT 2", "SELECT 1 ORDER BY 1 UNION SELECT 2", "SELECT 1 EXCEPT ALL DISTINCT SELECT 2"} {
		if _, err := db.QueryChunks(context.Background(), sql, func(*Chunk) error { return nil }); err == nil {
			t.Fatalf("accepted invalid/unsupported SQL: %s", sql)
		}
	}
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	if _, err := db.QueryChunks(ctx, "SELECT v FROM set_rows() INTERSECT ALL SELECT v FROM set_rows()", func(*Chunk) error { return nil }); err == nil {
		t.Fatal("canceled set query succeeded")
	}
	if used := db.MemoryUsed(); used != 0 {
		t.Fatalf("cancellation leak=%d", used)
	}
	ctx, cancel = context.WithCancel(context.Background())
	_, queryErr := db.QueryChunks(ctx, "SELECT v FROM set_rows() EXCEPT ALL SELECT v FROM set_rows() WHERE id < 2048", func(*Chunk) error { cancel(); return context.Canceled })
	cancel()
	if !errors.Is(queryErr, context.Canceled) {
		t.Fatalf("mid-query cancel=%v", queryErr)
	}
	if used := db.MemoryUsed(); used != 0 {
		t.Fatalf("mid-query cancellation leak=%d", used)
	}
	_, err := db.QueryChunks(context.Background(), "(WITH t AS (SELECT 1 AS v) SELECT v FROM t) UNION SELECT v FROM t", func(*Chunk) error { return nil })
	if err == nil || !strings.Contains(strings.ToLower(err.Error()), "t") {
		t.Fatalf("parenthesized CTE scope escaped: %v", err)
	}
}

func TestSetOperationsParallel(t *testing.T) {
	db, err := OpenSQLDB(Config{MemoryLimitBytes: 8 << 20, TempLimitBytes: 8 << 20, SpillMemoryBudgetBytes: 4096, TempDir: t.TempDir(), ManifestDir: t.TempDir(), Workers: 4, TableFunctions: []TableFunction{setRowsFunction()}})
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	for _, op := range []string{"INTERSECT ALL", "EXCEPT ALL"} {
		for attempt := 0; attempt < 2; attempt++ {
			var count int64
			err := db.QueryRowContext(context.Background(), "SELECT count(*) FROM (SELECT v FROM set_rows() "+op+" SELECT v FROM set_rows() WHERE id<2048) s").Scan(&count)
			want := int64(2048)
			if op == "EXCEPT ALL" {
				want = 2049
			}
			if err != nil || count != want {
				t.Fatalf("%s attempt=%d count=%d want=%d err=%v", op, attempt, count, want, err)
			}
		}
	}
}
