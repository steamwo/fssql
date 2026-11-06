package gosql

import (
	"context"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"testing"
)

func TestParquetDecimalPrecisionBoundaries(t *testing.T) {
	root := os.Getenv("FSSQL_DECIMAL_INTEROP_DIR")
	if root == "" {
		root = t.TempDir()
	} else if err := os.MkdirAll(root, 0700); err != nil {
		t.Fatal(err)
	}
	db, err := Open(Config{LocalFiles: &LocalFileConfig{RootDir: root}, TempDir: t.TempDir(), ManifestDir: t.TempDir()})
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	for _, width := range []int{9, 10, 18, 19, 38} {
		value := strings.Repeat("9", width-2) + ".99"
		uri, err := FileURI(filepath.Join(root, fmt.Sprintf("decimal_%d.parquet", width)))
		if err != nil {
			t.Fatal(err)
		}
		sql := fmt.Sprintf("SELECT CAST('-%s' AS DECIMAL(%d,2)) AS d UNION ALL SELECT CAST(NULL AS DECIMAL(%d,2)) UNION ALL SELECT CAST('%s' AS DECIMAL(%d,2))", value, width, width, value, width)
		if _, err := db.CopyTo(context.Background(), sql, uri, CopyOptions{Format: CopyFormatParquet}); err != nil {
			t.Fatal(err)
		}
		row := 0
		_, err = db.QueryChunks(context.Background(), "SELECT CAST(d AS VARCHAR) FROM read_parquet('"+strings.ReplaceAll(uri, "'", "''")+"')", func(c *Chunk) error {
			for i := 0; i < c.RowCount(); i++ {
				if row >= 3 {
					return fmt.Errorf("extra row")
				}
				got, ok, err := c.String(i, 0)
				if err != nil {
					return err
				}
				want := []string{"-" + value, "", value}[row]
				if ok != (row != 1) || got != want {
					return fmt.Errorf("width %d row %d: %q valid=%v want=%q", width, row, got, ok, want)
				}
				row++
			}
			return nil
		})
		if err != nil {
			t.Fatal(err)
		}
		if row != 3 {
			t.Fatalf("width %d rows=%d", width, row)
		}
		if db.MemoryUsed() != 0 {
			t.Fatalf("memory leaked=%d", db.MemoryUsed())
		}
	}
}
