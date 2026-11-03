package gosql

import (
	"context"
	"testing"
)

func TestPhase4DecimalCastRescalesExactly(t *testing.T) {
	db, err := Open(Config{TempDir: t.TempDir(), ManifestDir: t.TempDir()})
	if err != nil { t.Fatal(err) }
	defer db.Close()
	_, err = db.QueryChunks(context.Background(), "SELECT CAST(CAST('1.2' AS DECIMAL(2,1)) AS DECIMAL(4,2))", func(c *Chunk) error {
		got, ok, err := c.Int64(0, 0)
		if err != nil { return err }
		if !ok || got != 120 { t.Fatalf("rescaled decimal=%d valid=%v want=120", got, ok) }
		return nil
	})
	if err != nil { t.Fatal(err) }
	if used := db.MemoryUsed(); used != 0 { t.Fatalf("managed memory leaked: %d", used) }
}
