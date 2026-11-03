package gosql

import (
	"context"
	"reflect"
	"testing"
)

func TestPhase5NullSafeJoinMatchesNulls(t *testing.T) {
	db := phase5DB(t, 1<<20)
	got := collectPhase5Int(t, db, `SELECT 7 FROM (SELECT CAST(NULL AS BIGINT) AS id) a JOIN (SELECT CAST(NULL AS BIGINT) AS id) b ON a.id IS NOT DISTINCT FROM b.id`, 0)
	if !reflect.DeepEqual(got, []int64{7}) { t.Fatalf("NULL-safe join rows=%v", got) }
}

func TestPhase5UnsupportedWindowFrameFailsClosed(t *testing.T) {
	db := phase5DB(t, 1<<20)
	_, err := db.QueryChunks(context.Background(), `SELECT SUM(id) OVER (ORDER BY id RANGE BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) FROM phase5_rows()`, func(*Chunk) error { return nil })
	if err == nil { t.Fatal("unsupported RANGE frame unexpectedly succeeded") }
}

func TestPhase5RepeatableSamplingWithMultipleWorkers(t *testing.T) {
	db, err := Open(Config{
		MemoryLimitBytes: 8 << 20,
		TempLimitBytes: 8 << 20,
		SpillMemoryBudgetBytes: 1,
		TempDir: t.TempDir(),
		ManifestDir: t.TempDir(),
		TableFunctions: []TableFunction{phase5RowsFunction()},
		Workers: 2,
	})
	if err != nil { t.Fatal(err) }
	defer db.Close()
	query := `SELECT id FROM phase5_rows() USING SAMPLE reservoir(3 ROWS) REPEATABLE(42) ORDER BY id`
	first := collectPhase5Int(t, db, query, 0)
	second := collectPhase5Int(t, db, query, 0)
	if !reflect.DeepEqual(first, second) { t.Fatalf("repeatable multi-worker sample differs: %v != %v", first, second) }
}
