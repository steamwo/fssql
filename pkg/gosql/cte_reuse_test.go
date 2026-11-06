package gosql

import (
	"context"
	"fmt"
	"testing"
)

func TestMultiReferenceCTEReusesVolatileResult(t *testing.T) {
	db := groupedRowsDB(t, 1)
	rows := 0
	_, err := db.QueryChunks(context.Background(), `WITH x AS (SELECT id, random() AS r FROM grouped_rows())
SELECT a.id, a.r, b.r FROM x AS a JOIN x AS b ON a.id = b.id ORDER BY a.id`, func(c *Chunk) error {
		for row := 0; row < c.RowCount(); row++ {
			left, lok, err := c.Float64(row, 1)
			if err != nil { return err }
			right, rok, err := c.Float64(row, 2)
			if err != nil { return err }
			if !lok || !rok { return fmt.Errorf("volatile CTE result unexpectedly NULL") }
			if left != right { return fmt.Errorf("CTE volatile result was regenerated: %v != %v", left, right) }
			rows++
		}
		return nil
	})
	if err != nil { t.Fatal(err) }
	if rows != 5 { t.Fatalf("rows=%d want=5", rows) }
}
