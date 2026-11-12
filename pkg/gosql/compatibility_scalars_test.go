package gosql

import (
	"context"
	"math"
	"strings"
	"testing"
)

func TestUpperAbsPreparedAndCleanup(t *testing.T) {
	db, err := OpenSQLDB(Config{MemoryLimitBytes: 1 << 20, TempDir: t.TempDir(), ManifestDir: t.TempDir()})
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	stmt, err := db.PrepareContext(context.Background(), "SELECT upper(?), abs(?)")
	if err != nil {
		t.Fatal(err)
	}
	defer stmt.Close()
	for _, tc := range []struct {
		text, want string
		n          int64
	}{{"ßä中😊", "ẞÄ中😊", -42}, {"", "", 0}} {
		var text string
		var n int64
		if err := stmt.QueryRowContext(context.Background(), tc.text, tc.n).Scan(&text, &n); err != nil {
			t.Fatal(err)
		}
		if text != tc.want || n != -tc.n {
			t.Fatalf("got %q %d", text, n)
		}
	}
	if err := stmt.QueryRowContext(context.Background(), "x", int64(math.MinInt64)).Scan(new(string), new(int64)); err == nil || !strings.Contains(err.Error(), "overflow") {
		t.Fatalf("overflow=%v", err)
	}
	// A failed execution must not poison a reusable prepared statement.
	var text string
	var n int64
	if err := stmt.QueryRowContext(context.Background(), "ok", int64(-1)).Scan(&text, &n); err != nil || text != "OK" || n != 1 {
		t.Fatalf("after error: %q %d %v", text, n, err)
	}
}

func TestUpperAbsNativeNullEmptyAndCleanup(t *testing.T) {
	db, err := Open(Config{MemoryLimitBytes: 1 << 20, TempDir: t.TempDir(), ManifestDir: t.TempDir()})
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	for _, query := range []string{
		"SELECT upper(NULL), abs(NULL)",
		"SELECT upper('x'), abs(-1) WHERE FALSE",
		"SELECT abs(CAST('-9223372036854775808' AS BIGINT))",
	} {
		_, queryErr := db.QueryChunks(context.Background(), query, func(chunk *Chunk) error { return nil })
		if strings.Contains(query, "922337") != (queryErr != nil) {
			t.Fatalf("%s: %v", query, queryErr)
		}
		if used := db.MemoryUsed(); used != 0 {
			t.Fatalf("%s: managed leak=%d", query, used)
		}
	}
}
