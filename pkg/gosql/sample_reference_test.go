package gosql

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"os"
	"reflect"
	"testing"
)

func TestMigrationStreamingSampleReference(t *testing.T) {
	for _, path := range []string{"edge-agent-migration-reference.json", "edge-agent-caller-reference.json"} {
		t.Run(path, func(t *testing.T) {
			var ref struct {
				Samples []struct {
					Method string
					Suffix string
					IDs    []int64
				}
			}
			data, err := os.ReadFile("../../test/corpus/" + path)
			if err != nil {
				t.Fatal(err)
			}
			if err = json.Unmarshal(data, &ref); err != nil {
				t.Fatal(err)
			}
			fn := TableFunction{Name: "sample_rows", Columns: []Column{{Name: "id", Type: Type{ID: "BIGINT"}}}, Open: func(context.Context) (BatchReader, error) {
				offset := 0
				return &batchReaderFunc{next: func(_ context.Context, w BatchWriter) (int, error) {
					n := min(w.Capacity(), 8193-offset)
					if n == 0 {
						return 0, io.EOF
					}
					for i := 0; i < n; i++ {
						if err := w.SetInt64(i, 0, int64(offset+i)); err != nil {
							return 0, err
						}
					}
					offset += n
					return n, nil
				}}, nil
			}}
			db, err := Open(Config{TableFunctions: []TableFunction{fn}, TempDir: t.TempDir(), ManifestDir: t.TempDir()})
			if err != nil {
				t.Fatal(err)
			}
			defer db.Close()
			for _, tc := range ref.Samples {
				if tc.Method == "reservoir" {
					continue
				}
				t.Run(tc.Suffix, func(t *testing.T) {
					actual := make([]int64, 0)
					_, err := db.QueryChunks(context.Background(), "SELECT id FROM sample_rows() USING SAMPLE "+tc.Suffix, func(c *Chunk) error {
						for i := 0; i < c.RowCount(); i++ {
							v, ok, e := c.Int64(i, 0)
							if e != nil || !ok {
								return fmt.Errorf("id: %v", e)
							}
							actual = append(actual, v)
						}
						return nil
					})
					if err != nil {
						t.Fatal(err)
					}
					if !reflect.DeepEqual(actual, tc.IDs) {
						t.Fatalf("sample rows=%d want=%d; first=%v want=%v", len(actual), len(tc.IDs), actual[:min(10, len(actual))], tc.IDs[:min(10, len(tc.IDs))])
					}
				})
			}
		})
	}
}
