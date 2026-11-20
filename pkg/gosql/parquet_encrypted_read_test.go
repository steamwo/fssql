package gosql

import (
	"bytes"
	"context"
	"database/sql"
	"encoding/binary"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"sync/atomic"
	"testing"
)

func TestReadEncryptedParquet(t *testing.T) {
	ctx := context.Background()
	for _, algorithm := range []string{ParquetAES128GCM, ParquetAES256GCM} {
		t.Run(algorithm, func(t *testing.T) {
			root := t.TempDir()
			keySize := 16
			if algorithm == ParquetAES256GCM {
				keySize = 32
			}
			key := bytes.Repeat([]byte{0x42}, keySize)
			var deny atomic.Bool
			config := Config{
				LocalFiles:  &LocalFileConfig{RootDir: root},
				TempDir:     t.TempDir(),
				ManifestDir: t.TempDir(),
				ParquetKeys: func(_ context.Context, id, alg string) ([]byte, error) {
					if id != "read-back" || alg != algorithm {
						return nil, fmt.Errorf("unexpected export key %q/%q", id, alg)
					}
					return key, nil
				},
				ParquetReadKeys: func(_ context.Context, id string) ([]byte, error) {
					if id != "read-back" || deny.Load() {
						return nil, fmt.Errorf("key %q unavailable", id)
					}
					return key, nil
				},
			}
			db, err := Open(config)
			if err != nil {
				t.Fatal(err)
			}
			defer db.Close()
			path := filepath.Join(root, "encrypted.parquet")
			uri, err := FileURI(path)
			if err != nil {
				t.Fatal(err)
			}
			_, err = db.CopyTo(ctx, "SELECT range AS id, CASE WHEN range % 11 = 0 THEN NULL ELSE 'same' END AS note FROM range(1024)", uri, CopyOptions{
				Format:              CopyFormatParquet,
				Compression:         "snappy",
				ParquetRowGroupRows: 256,
				ParquetPageRows:     64,
				Encryption:          &ParquetEncryptionOptions{Algorithm: algorithm, KeyID: "read-back"},
			})
			if err != nil {
				t.Fatal(err)
			}
			readFrom := func(uri string) string { return " FROM read_parquet('" + strings.ReplaceAll(uri, "'", "''") + "')" }
			countSQL := "SELECT count(*)" + readFrom(uri)
			sqlDB, err := OpenSQLDB(config)
			if err != nil {
				t.Fatal(err)
			}
			defer sqlDB.Close()

			var count int64
			if err := sqlDB.QueryRowContext(ctx, countSQL).Scan(&count); err != nil || count != 1024 {
				t.Fatalf("count = %d, error = %v", count, err)
			}
			var minID, maxID int64
			if err := sqlDB.QueryRowContext(ctx, "SELECT min(id), max(id)"+readFrom(uri)).Scan(&minID, &maxID); err != nil || minID != 0 || maxID != 1023 {
				t.Fatalf("bounds = %d..%d, error = %v", minID, maxID, err)
			}
			rows, err := sqlDB.QueryContext(ctx, "SELECT id, note"+readFrom(uri)+" WHERE id >= 450 ORDER BY id")
			if err != nil {
				t.Fatal(err)
			}
			defer rows.Close()
			i := int64(450)
			for ; rows.Next(); i++ {
				var id int64
				var note sql.NullString
				if err := rows.Scan(&id, &note); err != nil {
					t.Fatal(err)
				}
				if id != i || note.Valid != (i%11 != 0) || (note.Valid && note.String != "same") {
					t.Fatalf("row %d = (%d, %+v)", i, id, note)
				}
			}
			if err := rows.Err(); err != nil || i != 1024 {
				t.Fatalf("read through row %d, error = %v", i, err)
			}

			// A prior successful query must not grant a later query access.
			deny.Store(true)
			if err := sqlDB.QueryRowContext(ctx, countSQL).Scan(&count); err == nil || !strings.Contains(err.Error(), "unavailable") {
				t.Fatalf("denied footer key: %v", err)
			}
			deny.Store(false)

			for _, failure := range []struct {
				name string
				keys ParquetReadKeyResolver
				want string
			}{
				{"no resolver", nil, "Config.ParquetReadKeys"},
				{"no key bytes", func(context.Context, string) ([]byte, error) { return nil, nil }, "must be 16 or 32 bytes"},
				{"wrong key", func(context.Context, string) ([]byte, error) { return make([]byte, keySize), nil }, "decrypt footer: encrypted module authentication failed"},
			} {
				t.Run(failure.name, func(t *testing.T) {
					wrong := config
					wrong.ParquetReadKeys = failure.keys
					reader, err := OpenSQLDB(wrong)
					if err != nil {
						t.Fatal(err)
					}
					defer reader.Close()
					if err := reader.QueryRowContext(ctx, countSQL).Scan(&count); err == nil || !strings.Contains(err.Error(), failure.want) {
						t.Fatalf("error = %v, want %q", err, failure.want)
					}
				})
			}

			original, err := os.ReadFile(path)
			if err != nil {
				t.Fatal(err)
			}
			// The first data column has no dictionary. The first encrypted
			// header starts after PARE and its body follows the header frame.
			bodyAt := 8 + int(binary.LittleEndian.Uint32(original[4:8]))
			for _, tamper := range []struct {
				module string
				at     int
			}{
				{"footer", len(original) - 9},
				{"page header", 4 + 4 + 12},
				{"page body", bodyAt + 4 + 12},
			} {
				t.Run("tampered "+tamper.module, func(t *testing.T) {
					corrupted := bytes.Clone(original)
					corrupted[tamper.at] ^= 1
					tamperedPath := filepath.Join(root, "tampered-"+tamper.module+".parquet")
					if err := os.WriteFile(tamperedPath, corrupted, 0o600); err != nil {
						t.Fatal(err)
					}
					tamperedURI, err := FileURI(tamperedPath)
					if err != nil {
						t.Fatal(err)
					}
					var id int64
					err = sqlDB.QueryRowContext(ctx, "SELECT id"+readFrom(tamperedURI)+" LIMIT 1").Scan(&id)
					if err == nil || errors.Is(err, sql.ErrNoRows) || !strings.Contains(err.Error(), "decrypt "+tamper.module+": encrypted module authentication failed") {
						t.Fatalf("error = %v", err)
					}
				})
			}
		})
	}
}
