package gosql

import (
	"context"
	"database/sql"
	"fmt"
	"math"
	"reflect"
	"testing"
)

func bulkRows(t *testing.T, db interface {
	QueryContext(context.Context, string, ...any) (*sql.Rows, error)
}, query string, args ...any) ([]string, [][]string) {
	t.Helper()
	rows, err := db.QueryContext(context.Background(), query, args...)
	if err != nil {
		t.Fatalf("%s: %v", query, err)
	}
	defer rows.Close()
	cols, err := rows.ColumnTypes()
	if err != nil {
		t.Fatal(err)
	}
	types := make([]string, len(cols))
	for i, c := range cols {
		types[i] = c.DatabaseTypeName()
	}
	out := [][]string{}
	for rows.Next() {
		values := make([]any, len(cols))
		dest := make([]any, len(cols))
		for i := range values {
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
		out = append(out, row)
	}
	if err := rows.Err(); err != nil {
		t.Fatalf("%s: %v", query, err)
	}
	return types, out
}

func TestBulkCompatibilityScalarsAndRelations(t *testing.T) {
	db, err := OpenSQLDB(Config{Workers: 4, MemoryLimitBytes: 8 << 20, TempDir: t.TempDir(), ManifestDir: t.TempDir()})
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	for _, tc := range []struct {
		name, query string
		types       []string
		rows        [][]string
	}{
		{"by-name", "SELECT 1 AS a,2 AS b UNION ALL BY NAME SELECT 3 AS b,4 AS c ORDER BY a NULLS LAST", []string{"INTEGER", "INTEGER", "INTEGER"}, [][]string{{"1", "2", "NULL"}, {"NULL", "3", "4"}}},
		{"quantified", "SELECT 5 > ANY(SELECT 2 UNION ALL SELECT NULL) AS a,5 < ALL(SELECT 1 WHERE FALSE) AS b,CAST(NULL AS INTEGER)=ANY(SELECT 1 WHERE FALSE) AS c", []string{"BOOLEAN", "BOOLEAN", "BOOLEAN"}, [][]string{{"true", "true", "false"}}},
		{"lateral-range", "WITH t AS (SELECT 2 AS n UNION ALL SELECT 4) SELECT t.n,r.i FROM t,range(t.n) AS r(i) ORDER BY t.n,r.i", []string{"INTEGER", "BIGINT"}, [][]string{{"2", "0"}, {"2", "1"}, {"4", "0"}, {"4", "1"}, {"4", "2"}, {"4", "3"}}},
		{"series", "SELECT generate_series FROM generate_series(3,0,-1) ORDER BY generate_series", []string{"BIGINT"}, [][]string{{"0"}, {"1"}, {"2"}, {"3"}}},
		{"unnest", "SELECT u.v FROM unnest(string_split('a,,b,',',')) AS u(v) ORDER BY u.v", []string{"VARCHAR"}, [][]string{{""}, {""}, {"a"}, {"b"}}},
		{"regex", "SELECT regexp_replace('aba','a','X') AS a,regexp_replace('aba','a','X','g') AS b,regexp_extract('a7','(a)([0-9])',2) AS c,list_extract(regexp_extract_all('ab','(a)|(b)',1),2) AS d,array_length(string_split('a,,b,',',')) AS e", []string{"VARCHAR", "VARCHAR", "VARCHAR", "VARCHAR", "BIGINT"}, [][]string{{"Xba", "XbX", "7", "NULL", "4"}}},
		{"round-null", "SELECT round(CAST('1.25' AS DECIMAL(6,2)),NULL) AS n,round(CAST('1.25' AS DECIMAL(6,2)),abs(1)) AS d,coalesce(round(CAST('1.25' AS DECIMAL(6,2)),NULL),CAST('2.00' AS DECIMAL(6,2))) AS c", []string{"\"NULL\"", "DECIMAL(6,1)", "DECIMAL(6,2)"}, [][]string{{"NULL", "1.3", "2.00"}}},
		{"casts", "SELECT CAST(TRUE AS DECIMAL(4,2)) AS d,CAST(FALSE AS FLOAT) AS f,TRY_CAST('0xffff' AS SMALLINT) AS n,CAST('0b1010' AS INTEGER) AS b", []string{"DECIMAL(4,2)", "FLOAT", "SMALLINT", "INTEGER"}, [][]string{{"1.00", "0", "NULL", "10"}}},
		{"date-parts", "SELECT date_part(lower('YEAR'),CAST('2024-02-29' AS DATE)) AS y,struct_extract(date_part(['year','epoch'],CAST('2024-01-02' AS DATE)),'year') AS s", []string{"BIGINT", "BIGINT"}, [][]string{{"2024", "2024"}}},
		{"calendar", "SELECT CAST(CAST('0001-01-01 (BC)' AS TIMESTAMP) AS VARCHAR) AS b,CAST('infinity' AS TIMESTAMP) AS i,date_part('year',CAST('infinity' AS TIMESTAMP)) AS n", []string{"VARCHAR", "TIMESTAMP", "BIGINT"}, [][]string{{"0001-01-01 (BC) 00:00:00", "infinity", "NULL"}}},
		{"named-zone", "SELECT CAST(CAST('2024-01-02 03:04:05 America/New_York' AS TIMESTAMPTZ) AS VARCHAR) AS a,CAST(timezone('America/New_York',CAST('2024-01-02 03:04:05' AS TIMESTAMP)) AS VARCHAR) AS b", []string{"VARCHAR", "VARCHAR"}, [][]string{{"2024-01-02 08:04:05+00", "2024-01-02 08:04:05+00"}}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			types, rows := bulkRows(t, db, tc.query)
			if !reflect.DeepEqual(types, tc.types) || !reflect.DeepEqual(rows, tc.rows) {
				t.Fatalf("types=%v rows=%v want types=%v rows=%v", types, rows, tc.types, tc.rows)
			}
		})
	}
}

func TestBulkNestedValuesOwnData(t *testing.T) {
	db, err := Open(Config{MemoryLimitBytes: 1 << 20, TempDir: t.TempDir(), ManifestDir: t.TempDir()})
	if err != nil {
		t.Fatal(err)
	}
	var retained []Value
	_, err = db.QueryChunks(context.Background(), "SELECT list_value(CAST('18446744073709551615' AS UBIGINT),CAST(NULL AS UBIGINT)) AS nums,date_part(['year','epoch'],CAST('2024-01-02' AS DATE)) AS parts,regexp_extract('ab','(a)(b)',['left','right']) AS captures,regexp_extract_all('ab','(a)|(b)',1) AS optional", func(c *Chunk) error {
		for col := 0; col < c.ColumnCount(); col++ {
			v, err := c.Value(0, col)
			if err != nil {
				return err
			}
			retained = append(retained, v)
		}
		return nil
	})
	if err != nil {
		db.Close()
		t.Fatal(err)
	}
	if used := db.MemoryUsed(); used != 0 {
		db.Close()
		t.Fatalf("query leaked %d", used)
	}
	if err := db.Close(); err != nil {
		t.Fatal(err)
	}
	nums := retained[0].Data.([]Value)
	if nums[0].Data.(uint64) != math.MaxUint64 || nums[1].Valid {
		t.Fatalf("wide nested values=%v", nums)
	}
	parts := retained[1].Data.([]FieldValue)
	if parts[0].Name != "year" || parts[0].Value.Data.(int64) != 2024 {
		t.Fatalf("parts=%v", parts)
	}
	caps := retained[2].Data.([]FieldValue)
	if caps[0].Value.Data.(string) != "a" || caps[1].Value.Data.(string) != "b" {
		t.Fatalf("captures=%v", caps)
	}
	optional := retained[3].Data.([]Value)
	if !optional[0].Valid || optional[0].Data.(string) != "a" || optional[1].Valid {
		t.Fatalf("optional=%v", optional)
	}
}

func TestBulkFailureCancellationAndCleanup(t *testing.T) {
	db, err := Open(Config{MemoryLimitBytes: 256 << 10, TempDir: t.TempDir(), ManifestDir: t.TempDir()})
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	for _, query := range []string{"SELECT * FROM range(0,5,0)", "SELECT regexp_matches('x','[')", "SELECT regexp_matches(NULL,'[')", "SELECT regexp_replace('a','z','\\q')", "SELECT 1=ANY(SELECT 1,2)", "SELECT 1 AS a,2 AS A UNION BY NAME SELECT 3 AS a", "SELECT TRY_CAST(regexp_replace(repeat('a',65536),'a','x','g') AS BIGINT)"} {
		if _, err := db.QueryChunks(context.Background(), query, func(*Chunk) error { return nil }); err == nil {
			t.Fatalf("expected failure: %s", query)
		}
		if used := db.MemoryUsed(); used != 0 {
			t.Fatalf("%s leaked %d", query, used)
		}
	}
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	seen := 0
	_, err = db.QueryChunks(ctx, "SELECT range FROM range(1000000000000)", func(c *Chunk) error { seen += c.RowCount(); cancel(); return nil })
	if err == nil || seen == 0 {
		t.Fatalf("cancel: rows=%d err=%v", seen, err)
	}
	if used := db.MemoryUsed(); used != 0 {
		t.Fatalf("cancel leaked %d", used)
	}
}
