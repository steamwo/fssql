package gosql

import (
	"context"
	"fmt"
	"os"
	"path/filepath"
	"reflect"
	"runtime"
	"strings"
	"testing"
)

func TestRepeatBudgetAndOverflow(t *testing.T) {
	db, err := Open(Config{MemoryLimitBytes: 1 << 20, TempDir: t.TempDir(), ManifestDir: t.TempDir()})
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	for _, tc := range []struct{ query, message string }{
		{"SELECT length(repeat('x', 67108864))", "memory limit exceeded"},
		{"SELECT repeat('xx', 9223372036854775807)", "repeat output length overflow"},
		{"SELECT repeat('x', -1)", "repeat count must be non-negative"},
	} {
		var before, after runtime.MemStats
		runtime.ReadMemStats(&before)
		_, err := db.QueryChunks(context.Background(), tc.query, func(*Chunk) error { return nil })
		runtime.ReadMemStats(&after)
		// The rejected 64 MiB result must never be constructed on the Go heap.
		if allocated := after.TotalAlloc - before.TotalAlloc; allocated > 16<<20 {
			t.Fatalf("failed repeat allocated %d bytes outside the 1 MiB budget", allocated)
		}
		if err == nil || !strings.Contains(err.Error(), tc.message) {
			t.Fatalf("%s: %v", tc.query, err)
		}
		if used := db.MemoryUsed(); used != 0 {
			t.Fatalf("failed repeat leaked %d bytes", used)
		}
	}
	_, err = db.QueryChunks(context.Background(), "SELECT repeat('你好', 3), repeat('', 9223372036854775807), repeat('x', 0), repeat(NULL, 2)", func(c *Chunk) error {
		for col, want := range []string{"你好你好你好", "", ""} {
			got, valid, err := c.String(0, col)
			if err != nil || !valid || got != want {
				return fmt.Errorf("column %d=%q valid=%v err=%v", col, got, valid, err)
			}
		}
		null, err := c.IsNull(0, 3)
		if err != nil || !null {
			return fmt.Errorf("NULL repeat: %v, %v", null, err)
		}
		return nil
	})
	if err != nil {
		t.Fatal(err)
	}
	if used := db.MemoryUsed(); used != 0 {
		t.Fatalf("repeat leaked %d bytes", used)
	}
}

func TestFloatIntegerCastRounding(t *testing.T) {
	db := groupedRowsDB(t, 1<<20)
	for _, kind := range []string{"FLOAT", "DOUBLE"} {
		for _, target := range []string{"TINYINT", "SMALLINT", "INTEGER", "BIGINT", "UTINYINT", "USMALLINT", "UINTEGER", "UBIGINT"} {
			for _, tc := range []struct {
				input string
				want  int64
			}{{"1.8", 2}, {"2.5", 2}, {"3.5", 4}, {"4.5", 4}, {"-1.8", -2}, {"-2.5", -2}, {"-3.5", -4}} {
				if strings.HasPrefix(target, "U") && tc.want < 0 {
					continue
				}
				query := fmt.Sprintf("SELECT CAST(CAST(CAST('%s' AS %s) AS %s) AS VARCHAR)", tc.input, kind, target)
				_, err := db.QueryChunks(context.Background(), query, func(c *Chunk) error {
					got, valid, err := c.String(0, 0)
					if err != nil || !valid || got != fmt.Sprint(tc.want) {
						return fmt.Errorf("got %q valid=%v err=%v", got, valid, err)
					}
					return nil
				})
				if err != nil {
					t.Fatalf("%s: %v", query, err)
				}
			}
		}
	}
	for _, input := range []string{"127.8", "-128.8", "NaN", "Inf"} {
		query := fmt.Sprintf("SELECT CAST(CAST('%s' AS DOUBLE) AS TINYINT)", input)
		if _, err := db.QueryChunks(context.Background(), query, func(*Chunk) error { return nil }); err == nil {
			t.Fatalf("expected overflow: %s", query)
		}
	}
	if _, err := db.QueryChunks(context.Background(), "SELECT CAST(CAST('-0.1' AS DOUBLE) AS UBIGINT)", func(*Chunk) error { return nil }); err == nil {
		t.Fatal("negative floating-point input must not cast to unsigned")
	}
}

func TestGlobalWindowsOnFileScans(t *testing.T) {
	root := t.TempDir()
	path := filepath.Join(root, "input.csv")
	if err := os.WriteFile(path, []byte("id,v\n1,x\n2,y\n3,z\n"), 0600); err != nil {
		t.Fatal(err)
	}
	uri, err := FileURI(path)
	if err != nil {
		t.Fatal(err)
	}
	db, err := Open(Config{LocalFiles: &LocalFileConfig{RootDir: root}, TempDir: t.TempDir(), ManifestDir: t.TempDir()})
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	csv := fmt.Sprintf("read_csv('%s', HEADER=true)", uri)
	parquetURI, err := FileURI(filepath.Join(root, "input.parquet"))
	if err != nil {
		t.Fatal(err)
	}
	if _, err := db.CopyTo(context.Background(), "SELECT * FROM "+csv, parquetURI, CopyOptions{Format: CopyFormatParquet}); err != nil {
		t.Fatal(err)
	}
	for _, source := range []string{csv, fmt.Sprintf("read_parquet('%s')", parquetURI)} {
		for _, query := range []string{
			"SELECT COUNT(*) OVER () FROM " + source,
			"SELECT id, COUNT(*) OVER () FROM " + source + " ORDER BY id",
		} {
			column := 0
			if strings.HasPrefix(query, "SELECT id,") {
				column = 1
			}
			if got := collectInt64Column(t, db, query, column); !reflect.DeepEqual(got, []int64{3, 3, 3}) {
				t.Fatalf("%s: %v", query, got)
			}
		}
		query := "SELECT id, COUNT(*) OVER (), SUM(id) OVER () FROM " + source + " ORDER BY id"
		var got [][3]int64
		_, err := db.QueryChunks(context.Background(), query, func(c *Chunk) error {
			for r := 0; r < c.RowCount(); r++ {
				id, _, e := c.Int64(r, 0)
				if e != nil {
					return e
				}
				count, _, e := c.Int64(r, 1)
				if e != nil {
					return e
				}
				sum, valid, e := c.Int128(r, 2)
				if e != nil || !valid || sum.String() != "6" {
					return fmt.Errorf("sum=%s valid=%v err=%v", sum.String(), valid, e)
				}
				got = append(got, [3]int64{id, count, 6})
			}
			return nil
		})
		if err != nil {
			t.Fatalf("%s: %v", query, err)
		}
		if !reflect.DeepEqual(got, [][3]int64{{1, 3, 6}, {2, 3, 6}, {3, 3, 6}}) {
			t.Fatalf("rows=%v", got)
		}
		if used := db.MemoryUsed(); used != 0 {
			t.Fatalf("window leaked %d bytes", used)
		}
	}
}

func TestCompositeNullSafeInnerJoin(t *testing.T) {
	db := groupedRowsDB(t, 1<<20)
	prefix := "WITH data AS (SELECT CASE WHEN id=1 THEN CAST(NULL AS BIGINT) ELSE grp END AS x, id AS y FROM grouped_rows()) SELECT COUNT(*) FROM data a JOIN data b ON "
	for _, predicate := range []string{
		"a.x IS NOT DISTINCT FROM b.x AND a.y IS NOT DISTINCT FROM b.y",
		"a.x IS NOT DISTINCT FROM b.x AND a.y=b.y",
		"a.y=b.y AND a.x IS NOT DISTINCT FROM b.x",
	} {
		if got := collectInt64Column(t, db, prefix+predicate, 0); !reflect.DeepEqual(got, []int64{5}) {
			t.Fatalf("%s: %v", predicate, got)
		}
	}
}
