package gosql

import (
	"context"
	"fmt"
	"os"
	"path/filepath"
	"reflect"
	"strings"
	"testing"
)

func TestParquetEarlyFilterAcrossBatches(t *testing.T) {
	root := t.TempDir()
	var csv strings.Builder
	csv.WriteString("id,payload,flag\n")
	for row := 0; row < 4200; row++ {
		flag := "1"
		if row%3 == 0 {
			flag = ""
		}
		fmt.Fprintf(&csv, "%d,payload-%d-%s,%s\n", row, row, strings.Repeat("x", 80), flag)
	}
	path := filepath.Join(root, "rows.csv")
	if err := os.WriteFile(path, []byte(csv.String()), 0600); err != nil {
		t.Fatal(err)
	}
	csvURI, err := FileURI(path)
	if err != nil {
		t.Fatal(err)
	}
	parquetURI, err := FileURI(filepath.Join(root, "rows.parquet"))
	if err != nil {
		t.Fatal(err)
	}
	db, err := Open(Config{MemoryLimitBytes: 32 << 20, LocalFiles: &LocalFileConfig{RootDir: root}, TempDir: t.TempDir(), ManifestDir: t.TempDir(), Workers: 1})
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	if _, err := db.CopyTo(context.Background(), fmt.Sprintf("SELECT id, payload, CASE WHEN flag = '' THEN NULL ELSE flag END AS flag FROM read_csv('%s', HEADER=true, columns={'id':'BIGINT','payload':'VARCHAR','flag':'VARCHAR'})", csvURI), parquetURI, CopyOptions{Format: CopyFormatParquet}); err != nil {
		t.Fatal(err)
	}
	for _, tc := range []struct {
		predicate string
		rows      []int
	}{
		{"id >= 2050 AND id < 2056 AND flag IS NOT NULL ORDER BY id", []int{2050, 2051, 2053, 2054}},
		{"id >= 2047 ORDER BY id LIMIT 3 OFFSET 2", []int{2049, 2050, 2051}},
		{"id = 1 OR id = 4096 ORDER BY id", []int{1, 4096}},
		{"CAST(id AS VARCHAR) = '2050'", []int{2050}},
		{"id < 0", nil},
	} {
		query := fmt.Sprintf("SELECT payload FROM read_parquet('%s') WHERE %s", parquetURI, tc.predicate)
		var got, want []string
		for _, row := range tc.rows {
			want = append(want, fmt.Sprintf("payload-%d-%s", row, strings.Repeat("x", 80)))
		}
		_, err := db.QueryChunks(context.Background(), query, func(chunk *Chunk) error {
			for row := 0; row < chunk.RowCount(); row++ {
				value, valid, err := chunk.String(row, 0)
				if err != nil {
					return err
				}
				if !valid {
					return fmt.Errorf("NULL payload")
				}
				got = append(got, value)
			}
			return nil
		})
		if err != nil {
			t.Fatalf("%s: %v", query, err)
		}
		if !reflect.DeepEqual(got, want) {
			t.Fatalf("%s: got=%v want=%v", query, got, want)
		}
		if used := db.MemoryUsed(); used != 0 {
			t.Fatalf("%s leaked %d bytes", query, used)
		}
	}
}
