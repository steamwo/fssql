package gosql

import (
	"context"
	"reflect"
	"testing"
)

func TestNullSafeJoinMatchesNulls(t *testing.T) {
	db := groupedRowsDB(t, 1<<20)
	got := collectInt64Column(t, db, `SELECT 7 FROM (SELECT CAST(NULL AS BIGINT) AS id) a JOIN (SELECT CAST(NULL AS BIGINT) AS id) b ON a.id IS NOT DISTINCT FROM b.id`, 0)
	if !reflect.DeepEqual(got, []int64{7}) { t.Fatalf("NULL-safe join rows=%v", got) }
}

func TestUnsupportedWindowFrameFailsClosed(t *testing.T) {
	db := groupedRowsDB(t, 1<<20)
	_, err := db.QueryChunks(context.Background(), `SELECT SUM(id) OVER (ORDER BY id RANGE BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) FROM grouped_rows()`, func(*Chunk) error { return nil })
	if err == nil { t.Fatal("unsupported RANGE frame unexpectedly succeeded") }
}

func TestRepeatableSamplingWithMultipleWorkers(t *testing.T) {
	db, err := Open(Config{
		MemoryLimitBytes: 8 << 20,
		TempLimitBytes: 8 << 20,
		SpillMemoryBudgetBytes: 1,
		TempDir: t.TempDir(),
		ManifestDir: t.TempDir(),
		TableFunctions: []TableFunction{groupedRowsFunction()},
		Workers: 2,
	})
	if err != nil { t.Fatal(err) }
	defer db.Close()
	query := `SELECT id FROM grouped_rows() USING SAMPLE reservoir(3 ROWS) REPEATABLE(42) ORDER BY id`
	first := collectInt64Column(t, db, query, 0)
	second := collectInt64Column(t, db, query, 0)
	if !reflect.DeepEqual(first, second) { t.Fatalf("repeatable multi-worker sample differs: %v != %v", first, second) }
}
