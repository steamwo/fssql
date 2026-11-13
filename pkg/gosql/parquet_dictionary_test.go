package gosql

import (
	"context"
	"fmt"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"testing"
)

// These include a zero-bit dictionary and a whole NULL data page, also under
// modular encryption. An explicit directory retains independent-reader inputs.
func TestParquetDictionaryArtifacts(t *testing.T) {
	root := os.Getenv("FSSQL_DICTIONARY_INTEROP_DIR")
	if root == "" {
		root = t.TempDir()
	}
	if err := os.MkdirAll(root, 0700); err != nil {
		t.Fatal(err)
	}
	db := openWideConversionDB(t, root)
	query := "SELECT '' AS empty_s, CAST(NULL AS VARCHAR) AS null_s, CASE WHEN range>=512 AND range<1024 THEN NULL ELSE '重复' END AS mixed_s, CAST(range AS VARCHAR) AS high_s FROM range(4101)"
	for _, mode := range []string{"plain", "aes128", "aes256"} {
		uri, err := FileURI(filepath.Join(root, mode+".parquet"))
		if err != nil {
			t.Fatal(err)
		}
		result, err := db.CopyTo(context.Background(), query, uri, wideConversionOptions(mode))
		if err != nil || result.Rows != 4101 || db.MemoryUsed() != 0 {
			t.Fatalf("mode=%s result=%+v memory=%d error=%v", mode, result, db.MemoryUsed(), err)
		}
		if mode != "plain" {
			continue
		}
		position := 0
		_, err = db.QueryChunks(context.Background(), "SELECT * FROM read_parquet('"+strings.ReplaceAll(uri, "'", "''")+"')", func(chunk *Chunk) error {
			for row := 0; row < chunk.RowCount(); row++ {
				for col := 0; col < 4; col++ {
					value, valid, err := chunk.String(row, col)
					if err != nil {
						return err
					}
					wantValid := col != 1 && !(col == 2 && position >= 512 && position < 1024)
					want := ""
					if col == 2 {
						want = "重复"
					} else if col == 3 {
						want = strconv.Itoa(position)
					}
					if valid != wantValid || valid && value != want {
						return fmt.Errorf("row=%d column=%d value=%q valid=%v", position, col, value, valid)
					}
				}
				position++
			}
			return nil
		})
		if err != nil || position != 4101 || db.MemoryUsed() != 0 {
			t.Fatalf("rows=%d memory=%d error=%v", position, db.MemoryUsed(), err)
		}
	}
}
