package gosql

import (
	"context"
	"database/sql"
	"testing"
	"time"
)

func TestTemporalCompatibilityPrepared(t *testing.T) {
	db, err := OpenSQLDB(Config{MemoryLimitBytes: 1 << 20, TempDir: t.TempDir(), ManifestDir: t.TempDir()})
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	stmt, err := db.PrepareContext(context.Background(), "SELECT CAST(? AS TIMESTAMPTZ) AS z, TRY_CAST(? AS TIMESTAMP_NS) AS n, date_part(?, CAST(? AS TIMESTAMP_NS)) AS p, CAST(CAST(? AS DOUBLE) AS DECIMAL(8,2)) AS d, CAST(CAST(? AS HUGEINT) AS BOOLEAN) AS b")
	if err != nil {
		t.Fatal(err)
	}
	defer stmt.Close()
	for _, tc := range []struct {
		part, text, decimal, huge, typ string
		want                           float64
		decimalWant                    string
		boolWant                       bool
	}{
		{"year", "2024-01-01T00:00:00.000000999Z", "2.675", "-1", "BIGINT", 2024, "2.68", true},
		{"epoch", "1969-12-31T23:59:59.999999999Z", "1.005", "0", "DOUBLE", 0, "1.00", false},
		{"microsecond", "2024-01-01T00:00:00.123456789Z", "-2.675", "170141183460469231731687303715884105727", "BIGINT", 123457, "-2.68", true},
	} {
		rows, err := stmt.QueryContext(context.Background(), "2024-01-02T03:04:05.123456789+08:30", "2263-01-01", tc.part, tc.text, tc.decimal, tc.huge)
		if err != nil {
			t.Fatal(err)
		}
		cols, err := rows.ColumnTypes()
		if err != nil {
			rows.Close()
			t.Fatal(err)
		}
		if len(cols) != 5 || cols[2].DatabaseTypeName() != tc.typ {
			rows.Close()
			t.Fatalf("part=%s types=%v", tc.part, cols)
		}
		var zoned time.Time
		var invalid sql.NullTime
		var part float64
		var decimal string
		var boolean bool
		if !rows.Next() {
			rows.Close()
			t.Fatalf("missing row: %v", rows.Err())
		}
		if err := rows.Scan(&zoned, &invalid, &part, &decimal, &boolean); err != nil {
			rows.Close()
			t.Fatal(err)
		}
		wantTime := time.Date(2024, 1, 1, 18, 34, 5, 123456000, time.UTC)
		if !zoned.Equal(wantTime) || invalid.Valid || part != tc.want || decimal != tc.decimalWant || boolean != tc.boolWant {
			rows.Close()
			t.Fatalf("%s: %v %v %v %s %v", tc.part, zoned, invalid, part, decimal, boolean)
		}
		if rows.Next() || rows.Err() != nil {
			rows.Close()
			t.Fatal("unexpected extra row/error")
		}
		if err := rows.Close(); err != nil {
			t.Fatal(err)
		}
	}
}

func TestTemporalCompatibilityCleanup(t *testing.T) {
	db, err := Open(Config{MemoryLimitBytes: 1 << 20, TempDir: t.TempDir(), ManifestDir: t.TempDir()})
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	for _, tc := range []struct {
		sql  string
		fail bool
	}{
		{"SELECT CAST('2263-01-01' AS TIMESTAMP_NS)", true},
		{"SELECT CAST('2024-01-01 24:00:00.1' AS TIMESTAMPTZ)", true},
		{"SELECT date_part('invalid',CAST('2024-01-01' AS DATE))", true},
		{"SELECT date_part('invalid',CAST('2024-01-01' AS DATE)) WHERE FALSE", true},
		{"SELECT date_part(lower('YEAR'),CAST('2024-01-01' AS DATE))", false},
		{"SELECT CAST(CAST('1000' AS DOUBLE) AS DECIMAL(5,2))", true},
		{"SELECT TRY_CAST(CAST('NaN' AS DOUBLE) AS DECIMAL(38,2)), TRY_CAST('bad' AS TIMESTAMPTZ)", false},
		{"SELECT date_part('year',CAST(NULL AS DATE)) WHERE FALSE", false},
	} {
		_, err := db.QueryChunks(context.Background(), tc.sql, func(*Chunk) error { return nil })
		if (err != nil) != tc.fail {
			t.Fatalf("%s: %v", tc.sql, err)
		}
		if used := db.MemoryUsed(); used != 0 {
			t.Fatalf("%s: leaked %d", tc.sql, used)
		}
	}
}

func TestTemporalCompatibilityNullMetadata(t *testing.T) {
	db, err := OpenSQLDB(Config{MemoryLimitBytes: 1 << 20, TempDir: t.TempDir(), ManifestDir: t.TempDir()})
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	for _, query := range []string{
		"SELECT date_part('year',CAST(NULL AS DATE)) AS v",
		"SELECT date_part('invalid',CAST(NULL AS TIMESTAMP)) AS v",
		"SELECT date_part(NULL,CAST('2024-01-01' AS DATE)) AS v",
	} {
		rows, err := db.Query(query)
		if err != nil {
			t.Fatal(err)
		}
		cols, err := rows.ColumnTypes()
		if err != nil {
			rows.Close()
			t.Fatal(err)
		}
		if len(cols) != 1 || cols[0].DatabaseTypeName() != "DOUBLE" {
			rows.Close()
			t.Fatalf("%s: types=%v", query, cols)
		}
		var v any
		if !rows.Next() {
			rows.Close()
			t.Fatal("missing NULL row")
		}
		if err := rows.Scan(&v); err != nil {
			rows.Close()
			t.Fatal(err)
		}
		if v != nil {
			rows.Close()
			t.Fatalf("%s: value=%v", query, v)
		}
		if rows.Next() || rows.Err() != nil {
			rows.Close()
			t.Fatal("unexpected extra row/error")
		}
		if err := rows.Close(); err != nil {
			t.Fatal(err)
		}
	}
}
