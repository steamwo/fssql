package gosql

import (
	"context"
	"database/sql"
	"strings"
	"testing"
)

func TestRoundTryCastPreparedAndCleanup(t *testing.T) {
	db, err := OpenSQLDB(Config{MemoryLimitBytes: 1 << 20, TempDir: t.TempDir(), ManifestDir: t.TempDir()})
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	stmt, err := db.PrepareContext(context.Background(), "SELECT round(TRY_CAST(? AS DECIMAL(8,3)), CAST(? AS INTEGER)) AS v")
	if err != nil {
		t.Fatal(err)
	}
	defer stmt.Close()
	for _, tc := range []struct {
		text      string
		precision int64
		want, typ string
		valid     bool
	}{
		{"1.235", 2, "1.24", "DECIMAL(8,2)", true},
		{"-1.500", 0, "-2", "DECIMAL(8,0)", true},
		{"bad", 1, "", "DECIMAL(8,1)", false},
		{"9.999", 3, "9.999", "DECIMAL(8,3)", true},
	} {
		rows, err := stmt.QueryContext(context.Background(), tc.text, tc.precision)
		if err != nil {
			t.Fatal(err)
		}
		cols, err := rows.ColumnTypes()
		if err != nil {
			rows.Close()
			t.Fatal(err)
		}
		if len(cols) != 1 || cols[0].DatabaseTypeName() != tc.typ {
			rows.Close()
			t.Fatalf("unexpected result types: %v, want %s", cols, tc.typ)
		}
		var got sql.NullString
		if !rows.Next() {
			rows.Close()
			t.Fatalf("missing row: %v", rows.Err())
		}
		if err := rows.Scan(&got); err != nil {
			rows.Close()
			t.Fatal(err)
		}
		if got.Valid != tc.valid || got.String != tc.want {
			rows.Close()
			t.Fatalf("got=%+v want=%s", got, tc.want)
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

func TestRoundTryCastFailuresAndCleanup(t *testing.T) {
	db, err := Open(Config{MemoryLimitBytes: 1 << 20, TempDir: t.TempDir(), ManifestDir: t.TempDir()})
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	for _, tc := range []struct {
		sql     string
		failure bool
	}{
		{"SELECT TRY_CAST('wrong' AS TIMESTAMP), TRY_CAST('128' AS TINYINT), TRY_CAST('1.5' AS BIGINT)", false},
		{"SELECT round(CAST('-128' AS TINYINT),-1)", true},
		{"SELECT TRY_CAST(abs(CAST('-9223372036854775808' AS BIGINT)) AS INTEGER)", true},
		{"SELECT TRY_CAST(repeat('x',2097152) AS BIGINT)", true}, // Resource errors in the input must propagate.
		{"SELECT TRY_CAST(CAST('1.25' AS DOUBLE) AS DECIMAL(6,2))", false},
		{"SELECT round(CAST('1.25' AS DECIMAL(6,2)),NULL)", false},   // Resolved SQLNULL metadata is covered by the bulk suite.
		{"SELECT round(CAST('1.25' AS DECIMAL(6,2)),abs(1))", false}, // Pure built-in precision folding is supported.
		{"SELECT TRY_CAST('bad' AS BIGINT), round(CAST('1.5' AS DOUBLE)) WHERE FALSE", false},
	} {
		_, err := db.QueryChunks(context.Background(), tc.sql, func(*Chunk) error { return nil })
		if tc.failure != (err != nil) {
			t.Fatalf("%s: %v", tc.sql, err)
		}
		if used := db.MemoryUsed(); used != 0 {
			t.Fatalf("%s: leaked %d", tc.sql, used)
		}
	}
	// Unsupported type syntax must not turn into a successful NULL conversion.
	if _, err := db.QueryChunks(context.Background(), "SELECT TRY_CAST('x' AS missing_type)", func(*Chunk) error { return nil }); err == nil || !strings.Contains(err.Error(), "unsupported CAST type") {
		t.Fatalf("unsupported type: %v", err)
	}
}
