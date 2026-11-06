package gosql

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"sort"
	"strconv"
	"testing"

	"github.com/steamwo/fssql/internal/core/memory"
)

func reservoirSource(population, batch int) TableFunction {
	return TableFunction{Name: "reservoir_rows", Columns: []Column{{Name: "id", Type: Type{ID: "BIGINT"}}}, Open: func(context.Context) (BatchReader, error) {
		offset := 0
		return &batchReaderFunc{next: func(_ context.Context, w BatchWriter) (int, error) {
			n := min(batch, w.Capacity(), population-offset)
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
}

func sampleDigest(values []int64) string {
	h := sha256.New()
	for _, v := range values {
		_, _ = io.WriteString(h, strconv.FormatInt(v, 10)+"\n")
	}
	return hex.EncodeToString(h.Sum(nil))
}

func TestDuckDBReservoirReference(t *testing.T) {
	for _, version := range []string{"1.5.5", "2.0.0"} {
		var ref struct {
			Version, Profile string
			Threads          int
			BatchRows        int `json:"batch_rows"`
			Cases            []struct {
				Population int
				BatchRows  int `json:"batch_rows"`
				Suffix     string
				Rows       int
				Ordered    string `json:"ordered_sha256"`
				Set        string `json:"set_sha256"`
			}
		}
		data, err := os.ReadFile("../../test/corpus/reservoir-duckdb-" + version + "-msvc.json")
		if err != nil {
			t.Fatal(err)
		}
		if err = json.Unmarshal(data, &ref); err != nil {
			t.Fatal(err)
		}
		if ref.Version != version || ref.Profile != "duckdb-msvc" || ref.Threads != 1 || ref.BatchRows != 2048 {
			t.Fatal("unrecognized oracle contract")
		}
		for _, tc := range ref.Cases {
			t.Run(version+"/"+strconv.Itoa(tc.Population)+"/"+tc.Suffix, func(t *testing.T) {
				batch := tc.BatchRows
				if batch == 0 {
					batch = ref.BatchRows
				}
				db, err := Open(Config{SampleCompatibility: ref.Profile, TableFunctions: []TableFunction{reservoirSource(tc.Population, batch)}, TempDir: t.TempDir(), ManifestDir: t.TempDir()})
				if err != nil {
					t.Fatal(err)
				}
				defer db.Close()
				var ids []int64
				_, err = db.QueryChunks(context.Background(), "SELECT id FROM reservoir_rows() USING SAMPLE "+tc.Suffix, func(c *Chunk) error {
					for row := 0; row < c.RowCount(); row++ {
						v, ok, err := c.Int64(row, 0)
						if err != nil || !ok {
							return fmt.Errorf("invalid sampled id: %v", err)
						}
						ids = append(ids, v)
					}
					return nil
				})
				if err != nil {
					t.Fatal(err)
				}
				ordered := sampleDigest(ids)
				sort.Slice(ids, func(i, j int) bool { return ids[i] < ids[j] })
				if len(ids) != tc.Rows || ordered != tc.Ordered || sampleDigest(ids) != tc.Set {
					t.Fatalf("reservoir mismatch: rows=%d want=%d ordered=%s want=%s first=%v", len(ids), tc.Rows, ordered, tc.Ordered, ids[:min(20, len(ids))])
				}
				if db.MemoryUsed() != 0 {
					t.Fatalf("retained memory=%d", db.MemoryUsed())
				}
			})
		}
	}
}

func TestDuckDBReservoirFailureCleanup(t *testing.T) {
	for _, profile := range []string{"duckdb", "duckdb-msvc"} {
		t.Run(profile, func(t *testing.T) {
			root := t.TempDir()
			db, err := Open(Config{SampleCompatibility: profile, TableFunctions: []TableFunction{reservoirSource(8193, 137)}, TempDir: root, ManifestDir: t.TempDir()})
			if err != nil {
				t.Fatal(err)
			}
			defer db.Close()
			stop := errors.New("stop sample output")
			_, err = db.QueryChunks(context.Background(), "SELECT id FROM reservoir_rows() USING SAMPLE reservoir(17%) REPEATABLE(42)", func(*Chunk) error { return stop })
			if !errors.Is(err, stop) {
				t.Fatal(err)
			}
			if db.MemoryUsed() != 0 {
				t.Fatal("sample error retained memory")
			}
			var entries []string
			if err := filepath.WalkDir(root, func(path string, e os.DirEntry, err error) error {
				if err != nil {
					return err
				}
				if !e.IsDir() {
					entries = append(entries, path)
				}
				return nil
			}); err != nil {
				t.Fatal(err)
			}
			if len(entries) != 0 {
				t.Fatal("sample error retained spill files")
			}
			ctx, cancel := context.WithCancel(context.Background())
			cancel()
			_, err = db.QueryChunks(ctx, "SELECT id FROM reservoir_rows() USING SAMPLE reservoir(17 ROWS)", func(*Chunk) error { return nil })
			if !errors.Is(err, context.Canceled) {
				t.Fatal(err)
			}
			ctx, cancel = context.WithCancel(context.Background())
			_, err = db.QueryChunks(ctx, "SELECT id FROM reservoir_rows() USING SAMPLE reservoir(3000 ROWS) REPEATABLE(42)", func(*Chunk) error { cancel(); return nil })
			cancel()
			if !errors.Is(err, context.Canceled) || db.MemoryUsed() != 0 {
				t.Fatalf("sample cancellation/cleanup: %v memory=%d", err, db.MemoryUsed())
			}
			limited, err := Open(Config{SampleCompatibility: profile, MemoryLimitBytes: 32 << 10, TableFunctions: []TableFunction{reservoirSource(8193, 137)}, TempDir: t.TempDir(), ManifestDir: t.TempDir()})
			if err != nil {
				t.Fatal(err)
			}
			defer limited.Close()
			_, err = limited.QueryChunks(context.Background(), "SELECT id FROM reservoir_rows() USING SAMPLE reservoir(9000 ROWS) REPEATABLE(42)", func(*Chunk) error { return nil })
			if !errors.Is(err, memory.ErrLimitExceeded) || limited.MemoryUsed() != 0 {
				t.Fatalf("sample quota/cleanup: %v memory=%d", err, limited.MemoryUsed())
			}
		})
	}
	if db, err := Open(Config{SampleCompatibility: "unknown"}); err == nil {
		db.Close()
		t.Fatal("unknown sample profile accepted")
	}
}
