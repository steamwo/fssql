package gosql

import (
	"context"
	"errors"
	"fmt"
	"io"
	"strings"
	"sync/atomic"
	"testing"
)

// Golden results are independently generated with DuckDB 2.0.0 and also
// retained as SQL.SUBQUERY.RELATION cases in the compatibility corpus.
func TestRelationCorrelatedSubqueries(t *testing.T) {
	cases := []struct{ sql, want string }{
		{"SELECT o.g,(SELECT count(*) FROM (SELECT i.id FROM common_rows() i WHERE i.g=o.g) d) FROM common_rows() o WHERE o.id=3 ORDER BY o.g NULLS LAST", "2,2\nNULL,0"},
		{"SELECT o.g,(WITH d AS (SELECT i.id FROM common_rows() i WHERE i.g=o.g) SELECT count(*) FROM d) FROM common_rows() o WHERE o.id=3 ORDER BY o.g NULLS LAST", "2,2\nNULL,0"},
		{"SELECT o.id,(SELECT sum(d.x) FROM (SELECT o.id AS x UNION ALL SELECT o.id+1) d) FROM common_rows() o WHERE o.g=2 ORDER BY o.id", "2,5\n3,7"},
		{"SELECT o.g,(SELECT sum(d.id) FROM (SELECT i.id FROM common_rows() i WHERE i.g=o.g ORDER BY i.id DESC LIMIT 1) d) FROM common_rows() o WHERE o.id=3 ORDER BY o.g NULLS LAST", "2,3\nNULL,NULL"},
		{"SELECT o.g,(SELECT count(*) FROM common_rows() i JOIN common_rows() j ON i.id=j.id AND j.g=o.g) FROM common_rows() o WHERE o.id=3 ORDER BY o.g NULLS LAST", "2,4\nNULL,0"},
		{"SELECT o.g,(SELECT count(*)+100*count(j.id) FROM common_rows() i LEFT JOIN common_rows() j ON i.id=j.id AND j.g<o.g) FROM common_rows() o WHERE o.id=3 ORDER BY o.g NULLS LAST", "2,609\nNULL,7"},
		{"SELECT o.g,(SELECT count(*)+100*count(i.id) FROM common_rows() i RIGHT JOIN common_rows() j ON i.id=j.id AND i.g<o.g) FROM common_rows() o WHERE o.id=3 ORDER BY o.g NULLS LAST", "2,609\nNULL,7"},
		{"SELECT o.g,(SELECT count(*)+100*count(i.id)+10000*count(j.id) FROM common_rows() i FULL JOIN common_rows() j ON i.id=j.id AND i.g=o.g AND j.g=o.g) FROM common_rows() o WHERE o.id=3 ORDER BY o.g NULLS LAST", "2,60612\nNULL,60614"},
		{"SELECT o.g,(SELECT count(*) FROM common_rows() i SEMI JOIN common_rows() j ON i.id=j.id AND j.g=o.g) FROM common_rows() o WHERE o.id=3 ORDER BY o.g NULLS LAST", "2,4\nNULL,0"},
		{"SELECT o.g,(SELECT count(*) FROM common_rows() i ANTI JOIN common_rows() j ON i.id=j.id AND j.g=o.g) FROM common_rows() o WHERE o.id=3 ORDER BY o.g NULLS LAST", "2,3\nNULL,7"},
		{"SELECT o.g,(SELECT count(*) FROM (SELECT id FROM common_rows() WHERE g=o.g) i LEFT JOIN (SELECT id FROM common_rows() WHERE g=o.g) j ON i.id=j.id) FROM common_rows() o WHERE o.id IN (1,3) ORDER BY o.g NULLS LAST,o.id", "1,6\n1,6\n2,2\nNULL,0"},
		{"SELECT o.g,(SELECT count(*) FROM (SELECT DISTINCT i.id FROM common_rows() i WHERE i.g=o.g ORDER BY i.id LIMIT 1 OFFSET 1) d) FROM common_rows() o WHERE o.id IN (1,3) ORDER BY o.g NULLS LAST,o.id", "1,1\n1,1\n2,1\nNULL,0"},
		{"SELECT a.g,a.id,b.id FROM common_rows() a LEFT JOIN common_rows() b ON a.g=b.g AND EXISTS(SELECT 1 FROM common_rows() x WHERE x.id=a.id AND x.id=b.id) ORDER BY a.g NULLS LAST,a.id NULLS LAST,b.id NULLS LAST", "1,1,1\n1,1,1\n1,1,1\n1,1,1\n1,2,2\n1,NULL,NULL\n2,2,2\n2,3,3\nNULL,3,NULL"},
		{"SELECT a.g,a.id,b.g,b.id FROM common_rows() a FULL JOIN common_rows() b ON a.id=b.id AND EXISTS(SELECT 1 WHERE a.g=b.g) ORDER BY a.g NULLS LAST,a.id NULLS LAST,b.g NULLS LAST,b.id NULLS LAST", "1,1,1,1\n1,1,1,1\n1,1,1,1\n1,1,1,1\n1,2,1,2\n1,NULL,NULL,NULL\n2,2,2,2\n2,3,2,3\nNULL,3,NULL,NULL\nNULL,NULL,1,NULL\nNULL,NULL,NULL,3"},
		{"SELECT a.g,a.id FROM common_rows() a SEMI JOIN common_rows() b ON (SELECT count(*) FROM common_rows() x WHERE x.id=a.id AND x.g=b.g)>0 ORDER BY a.g NULLS LAST,a.id NULLS LAST", "1,1\n1,1\n1,2\n2,2\n2,3\nNULL,3"},
		{"SELECT a.id,b.id FROM common_rows() a JOIN common_rows() b ON a.id=b.id AND a.id=(SELECT 1) ORDER BY a.id,b.id", "1,1\n1,1\n1,1\n1,1"},
		{"SELECT a.g,a.id,b.g,b.id FROM common_rows() a LEFT JOIN common_rows() b ON a.id=(SELECT max(x.id) FROM common_rows() x WHERE x.g=b.g) ORDER BY a.g NULLS LAST,a.id NULLS LAST,b.g NULLS LAST,b.id NULLS LAST", "1,1,NULL,NULL\n1,1,NULL,NULL\n1,2,1,1\n1,2,1,1\n1,2,1,2\n1,2,1,NULL\n1,NULL,NULL,NULL\n2,2,1,1\n2,2,1,1\n2,2,1,2\n2,2,1,NULL\n2,3,2,2\n2,3,2,3\nNULL,3,2,2\nNULL,3,2,3"},
		{"SELECT o.g,(WITH d AS (SELECT i.id FROM common_rows() i WHERE i.g=o.g) SELECT count(*) FROM d a JOIN d b ON a.id=b.id) FROM common_rows() o WHERE o.id=3 ORDER BY o.g NULLS LAST", "2,2\nNULL,0"},
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

func TestSubqueryOuterJoinReusesInputAndCleansUp(t *testing.T) {
	var leftOpens, rightOpens atomic.Int64
	left, right := commonSQLRows(), commonSQLRows()
	left.Name, right.Name = "relation_left", "relation_right"
	openLeft, openRight := left.Open, right.Open
	left.Open = func(ctx context.Context) (BatchReader, error) { leftOpens.Add(1); return openLeft(ctx) }
	right.Open = func(ctx context.Context) (BatchReader, error) { rightOpens.Add(1); return openRight(ctx) }
	db, err := Open(Config{TableFunctions: []TableFunction{left, right}, TempDir: t.TempDir(), ManifestDir: t.TempDir(), SpillMemoryBudgetBytes: 128, Workers: 4})
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	sql := "SELECT count(*) FROM relation_left() a FULL JOIN relation_right() b ON a.id=b.id AND EXISTS(SELECT 1 WHERE a.g=b.g)"
	var got int64
	_, err = db.QueryChunks(context.Background(), sql, func(c *Chunk) error {
		value, ok, err := c.Int64(0, 0)
		if err != nil {
			return err
		}
		if !ok {
			return fmt.Errorf("NULL count")
		}
		got = value
		return nil
	})
	if err != nil || got != 11 {
		t.Fatalf("count=%d err=%v", got, err)
	}
	if leftOpens.Load() != 1 || rightOpens.Load() != 1 {
		t.Fatalf("source opens: left=%d right=%d", leftOpens.Load(), rightOpens.Load())
	}
	if db.MemoryUsed() != 0 {
		t.Fatalf("managed memory leak=%d", db.MemoryUsed())
	}
	wantErr := errors.New("stop relation result")
	_, err = db.QueryChunks(context.Background(), sql, func(*Chunk) error { return wantErr })
	if !errors.Is(err, wantErr) {
		t.Fatalf("callback error=%v", err)
	}
	if db.MemoryUsed() != 0 {
		t.Fatalf("callback cleanup leaked %d bytes", db.MemoryUsed())
	}
}

func TestSubqueryOuterJoinAcrossBatches(t *testing.T) {
	makeSource := func(name string) TableFunction {
		return TableFunction{Name: name, Columns: []Column{{Name: "id", Type: Type{ID: "BIGINT"}}}, Open: func(context.Context) (BatchReader, error) {
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
	}
	db, err := Open(Config{TableFunctions: []TableFunction{makeSource("relation_large_left"), makeSource("relation_large_right")}, MemoryLimitBytes: 8 << 20, TempLimitBytes: 64 << 20, TempDir: t.TempDir(), ManifestDir: t.TempDir(), SpillMemoryBudgetBytes: 4096, Workers: 4})
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	var got int64
	sql := "SELECT count(*) FROM relation_large_left() a FULL JOIN relation_large_right() b ON a.id=b.id AND EXISTS(SELECT 1 WHERE a.id<4999)"
	_, err = db.QueryChunks(context.Background(), sql, func(c *Chunk) error {
		value, ok, err := c.Int64(0, 0)
		if err != nil {
			return err
		}
		if !ok {
			return fmt.Errorf("NULL count")
		}
		got = value
		return nil
	})
	if err != nil || got != 5001 {
		t.Fatalf("count=%d err=%v", got, err)
	}
	if db.MemoryUsed() != 0 {
		t.Fatalf("managed memory leak=%d", db.MemoryUsed())
	}
}
