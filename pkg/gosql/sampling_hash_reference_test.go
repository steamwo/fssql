package gosql

import (
	"crypto/md5"
	"encoding/hex"
	"encoding/json"
	"os"
	"reflect"
	"testing"
)

func TestSamplingCanonicalHashReference(t *testing.T) {
	for _, version := range []string{"1.5.5", "2.0.0"} {
		t.Run(version, func(t *testing.T) {
			var ref struct {
				Version      string
				CanonicalSQL string `json:"canonical_sql"`
				Canonical    [][]any
				Cases        []struct {
					SQL string
					IDs []int64
				}
			}
			data, err := os.ReadFile("../../test/corpus/sampling-hash-duckdb-" + version + ".json")
			if err != nil {
				t.Fatal(err)
			}
			if err = json.Unmarshal(data, &ref); err != nil {
				t.Fatal(err)
			}
			if ref.Version != version {
				t.Fatal("wrong hash oracle version")
			}
			db, err := OpenSQLDB(Config{TempDir: t.TempDir(), ManifestDir: t.TempDir()})
			if err != nil {
				t.Fatal(err)
			}
			defer db.Close()
			rows, err := db.Query(ref.CanonicalSQL)
			if err != nil {
				t.Fatal(err)
			}
			defer rows.Close()
			count := 0
			for rows.Next() {
				var id int64
				var payload, digest string
				if err = rows.Scan(&id, &payload, &digest); err != nil {
					t.Fatal(err)
				}
				sum := md5.Sum([]byte(payload))
				want := ref.Canonical[count]
				if id != int64(want[0].(float64)) || payload != want[1].(string) || digest != want[2].(string) || digest != hex.EncodeToString(sum[:]) {
					t.Fatalf("canonical row %d differs: %q %q", id, payload, digest)
				}
				count++
			}
			if err = rows.Err(); err != nil {
				t.Fatal(err)
			}
			if count != len(ref.Canonical) {
				t.Fatal("canonical row count mismatch")
			}
			rows.Close()
			for _, tc := range ref.Cases {
				rows, err := db.Query(tc.SQL)
				if err != nil {
					t.Fatal(err)
				}
				ids := make([]int64, 0)
				for rows.Next() {
					var id int64
					if err = rows.Scan(&id); err != nil {
						t.Fatal(err)
					}
					ids = append(ids, id)
				}
				if err = rows.Err(); err != nil {
					t.Fatal(err)
				}
				rows.Close()
				if !reflect.DeepEqual(ids, tc.IDs) {
					t.Fatalf("hash ordering/threshold/tie mismatch: %v want=%v", ids, tc.IDs)
				}
			}
		})
	}
}
