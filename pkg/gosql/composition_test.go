package gosql

import (
	"context"
	"strings"
	"testing"
)

func TestNativeBlockingStableOperatorsHaveTemporaryScope(t *testing.T) {
	db, err := Open(Config{MemoryLimitBytes: 32 << 20, TempLimitBytes: 64 << 20, SpillMemoryBudgetBytes: 1 << 20})
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()

	queries := []string{
		"SELECT DISTINCT 2 AS value",
		"SELECT 2 AS value ORDER BY value",
		"WITH a AS (SELECT 1 AS k) SELECT count(*) AS value FROM a",
		"WITH a AS (SELECT 1 AS k), b AS (SELECT 1 AS k) SELECT a.k FROM a JOIN b ON a.k = b.k",
	}
	for _, sqlText := range queries {
		var rows int
		if _, err := db.QueryChunks(context.Background(), sqlText, func(chunk *Chunk) error {
			rows += chunk.RowCount()
			return nil
		}); err != nil {
			t.Fatalf("%s: %v", sqlText, err)
		}
		if rows != 1 {
			t.Fatalf("%s: rows=%d want=1", sqlText, rows)
		}
		if used := db.MemoryUsed(); used != 0 {
			t.Fatalf("%s: managed memory leak=%d", sqlText, used)
		}
	}
}

func TestDatabaseSQLBlockingQueryUsesSameProductionComposition(t *testing.T) {
	db, err := OpenSQLDB(Config{MemoryLimitBytes: 32 << 20, TempLimitBytes: 64 << 20, SpillMemoryBudgetBytes: 1 << 20})
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	rows, err := db.QueryContext(context.Background(), "SELECT DISTINCT 9 AS value ORDER BY value")
	if err != nil {
		t.Fatal(err)
	}
	defer rows.Close()
	if !rows.Next() {
		t.Fatalf("Next: %v", rows.Err())
	}
	var value int64
	if err := rows.Scan(&value); err != nil {
		t.Fatal(err)
	}
	if value != 9 {
		t.Fatalf("value=%d", value)
	}
}

func TestNativeCopyIsConfiguredAndFailsClosedThroughVFS(t *testing.T) {
	db, err := Open(Config{})
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	err = db.Exec(context.Background(), "COPY (SELECT 1 AS value) TO 's3://bucket/out.csv' (FORMAT csv)")
	if err == nil {
		t.Fatal("expected unconfigured S3 VFS failure")
	}
	if strings.Contains(err.Error(), "COPY executor is not configured") || strings.Contains(err.Error(), "blocked") {
		t.Fatalf("COPY did not reach configured Export/VFS adapter: %v", err)
	}
	if !strings.Contains(err.Error(), "unsupported scheme") {
		t.Fatalf("COPY error=%v", err)
	}
	if used := db.MemoryUsed(); used != 0 {
		t.Fatalf("managed memory leak=%d", used)
	}
}

func TestOpenRejectsInvalidS3CredentialsBeforeQuery(t *testing.T) {
	_, err := Open(Config{S3: &S3Config{Endpoint: "https://s3.example.com", Region: "us-east-1"}})
	if err == nil || !strings.Contains(err.Error(), "missing access key") {
		t.Fatalf("error=%v", err)
	}
}
