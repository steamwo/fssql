package gosql

import (
	"bytes"
	"context"
	"encoding/base64"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"reflect"
	"strings"
	"testing"

	"github.com/steamwo/fssql/test/parquetcheck"
)

func encryptionRows(count int, fail bool) TableFunction {
	return TableFunction{Name: "encryption_rows", Columns: []Column{{Name: "id", Type: Type{ID: "BIGINT"}, Nullable: true}, {Name: "note", Type: Type{ID: "VARCHAR"}, Nullable: true}}, Open: func(context.Context) (BatchReader, error) {
		row := 0
		return &batchReaderFunc{next: func(ctx context.Context, w BatchWriter) (int, error) {
			if err := ctx.Err(); err != nil {
				return 0, err
			}
			if row == count {
				if fail {
					return 0, errors.New("injected encrypted input failure")
				}
				return 0, io.EOF
			}
			n := 257
			if count-row < n {
				n = count - row
			}
			for i := 0; i < n; i++ {
				id, note := encryptionValue(row + i)
				if id != nil {
					if err := w.SetInt64(i, 0, id.(int64)); err != nil {
						return 0, err
					}
				}
				if note != nil {
					if err := w.SetString(i, 1, note.(string)); err != nil {
						return 0, err
					}
				}
			}
			row += n
			return n, nil
		}}, nil
	}}
}

func encryptionValue(row int) (any, any) {
	var id, note any
	if row%17 != 0 {
		id = int64(row - 2000)
	}
	if row%11 != 0 {
		note = ""
		if row%13 != 0 {
			note = fmt.Sprintf("记录-%d-\n'x'", row)
		}
	}
	return id, note
}

func encryptedCopySQL(uri, query, encoded, codec string) string {
	return "COPY (" + query + ") TO '" + strings.ReplaceAll(uri, "'", "''") + "' (FORMAT ENCRYPTED_PARQUET, COMPRESSION " + codec + ", ENCRYPTION_CONFIG {footer_key_value: from_base64('" + encoded + "'), master_key_id: 'key-001'})"
}

func TestEncryptedCopySQLAndAPI(t *testing.T) {
	for _, n := range []int{16, 32} {
		for _, codec := range []string{"identity", "snappy"} {
			for _, count := range []int{0, 4101} {
				t.Run(fmt.Sprintf("AES%d/%s/rows%d", n*8, codec, count), func(t *testing.T) {
					ctx := context.Background()
					root, manifest := t.TempDir(), t.TempDir()
					key := bytes.Repeat([]byte{0x79}, n)
					encoded := base64.StdEncoding.EncodeToString(key)
					callbackCalls := 0
					config := Config{TableFunctions: []TableFunction{encryptionRows(count, false)}, LocalFiles: &LocalFileConfig{RootDir: root}, TempDir: t.TempDir(), ManifestDir: manifest, ParquetKeys: func(_ context.Context, id, algorithm string) ([]byte, error) {
						callbackCalls++
						if id != "key-001" || algorithm != fmt.Sprintf("AES-%d-GCM", n*8) {
							return nil, errors.New("unexpected key request")
						}
						return key, nil
					}}
					db, err := Open(config)
					if err != nil {
						t.Fatal(err)
					}
					defer db.Close()
					apiPath := filepath.Join(root, "api.parquet")
					apiURI, _ := FileURI(apiPath)
					if _, err = db.CopyTo(ctx, "SELECT * FROM encryption_rows()", apiURI, CopyOptions{Format: CopyFormatParquet, Compression: codec, Encryption: &ParquetEncryptionOptions{Algorithm: fmt.Sprintf("AES-%d-GCM", n*8), KeyID: "key-001"}}); err != nil {
						t.Fatal(err)
					}
					if callbackCalls != 1 || !bytes.Equal(key, bytes.Repeat([]byte{0x79}, n)) || db.MemoryUsed() != 0 {
						t.Fatal("API key ownership or resource leak")
					}
					// SQL needs no ParquetKeys callback, even when one is configured.
					config.ParquetKeys = nil
					sqlDB, err := OpenSQLDB(config)
					if err != nil {
						t.Fatal(err)
					}
					defer sqlDB.Close()
					sqlPath := filepath.Join(root, "sql.parquet")
					sqlURI, _ := FileURI(sqlPath)
					stmt := encryptedCopySQL(sqlURI, "SELECT * FROM encryption_rows()", encoded, codec)
					prepared, err := sqlDB.PrepareContext(ctx, stmt)
					if err != nil {
						t.Fatal(err)
					}
					defer prepared.Close()
					if _, err = prepared.ExecContext(ctx); err != nil {
						t.Fatal(err)
					}
					before, err := os.ReadFile(sqlPath)
					if err != nil {
						t.Fatal(err)
					}
					if _, err = prepared.ExecContext(ctx); err == nil {
						t.Fatal("existing target overwritten")
					}
					if _, err = sqlDB.ExecContext(ctx, encryptedCopySQL(sqlURI, "SELECT * FROM encryption_rows() WHERE id IS NULL", encoded, codec)); err == nil {
						t.Fatal("conflicting target overwritten")
					}
					after, _ := os.ReadFile(sqlPath)
					if !bytes.Equal(before, after) {
						t.Fatal("target changed after conflict")
					}
					apiBytes, err := os.ReadFile(apiPath)
					if err != nil {
						t.Fatal(err)
					}
					codecID := int64(0)
					if codec == "snappy" {
						codecID = 1
					}
					a := parquetcheck.Encrypted(t, apiBytes, key, "key-001", codecID)
					b := parquetcheck.Encrypted(t, before, key, "key-001", codecID)
					if !reflect.DeepEqual(a.Values, b.Values) || a.Groups != b.Groups || a.Pages != b.Pages || a.Modules != b.Modules || a.KeyID != b.KeyID || a.Codec != b.Codec {
						t.Fatal("SQL/API encryption contracts differ")
					}
					if bytes.Equal(a.FileAAD, b.FileAAD) || bytes.Equal(apiBytes, before) {
						t.Fatal("encryption reused file randomness")
					}
					if len(a.Values) != count {
						t.Fatalf("rows=%d want=%d", len(a.Values), count)
					}
					for row, values := range a.Values {
						id, note := encryptionValue(row)
						if !reflect.DeepEqual(values, []any{id, note}) {
							t.Fatalf("row %d differs", row)
						}
					}
					if count > 0 && (a.Groups != 3 || a.Pages <= a.Groups*2 || a.Modules != 2*a.Pages+1) {
						t.Fatal("fixture did not exercise multiple groups/pages/columns")
					}
					assertNoPersistedKey(t, manifest, key, encoded)
				})
			}
		}
	}
}

func TestEncryptedCopySQLFailureCleanup(t *testing.T) {
	root, manifest := t.TempDir(), t.TempDir()
	key := bytes.Repeat([]byte{'z'}, 16)
	encoded := base64.StdEncoding.EncodeToString(key)
	db, err := OpenSQLDB(Config{TableFunctions: []TableFunction{encryptionRows(3000, true)}, LocalFiles: &LocalFileConfig{RootDir: root}, TempDir: t.TempDir(), ManifestDir: manifest})
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	uri, _ := FileURI(filepath.Join(root, "failed.parquet"))
	for _, stmt := range []string{encryptedCopySQL(uri, "SELECT * FROM encryption_rows()", encoded, "snappy"), strings.Replace(encryptedCopySQL(uri, "SELECT * FROM encryption_rows()", encoded, "snappy"), "master_key_id:", "unknown_field:", 1), "COPY (SELECT 1) TO '" + uri + "' (FORMAT ENCRYPTED_PARQUET)"} {
		if _, err := db.ExecContext(context.Background(), stmt); err == nil || strings.Contains(err.Error(), encoded) {
			t.Fatal("missing failure or key in error")
		}
		entries, err := os.ReadDir(root)
		if err != nil {
			t.Fatal(err)
		}
		if len(entries) != 0 {
			t.Fatal("partial output leaked")
		}
	}
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	if _, err := db.ExecContext(ctx, encryptedCopySQL(uri, "SELECT * FROM encryption_rows()", encoded, "identity")); !errors.Is(err, context.Canceled) {
		t.Fatal(err)
	}
	assertNoPersistedKey(t, manifest, key, encoded)
}

func assertNoPersistedKey(t *testing.T, root string, key []byte, encoded string) {
	t.Helper()
	if err := filepath.WalkDir(root, func(path string, entry os.DirEntry, err error) error {
		if err != nil {
			return err
		}
		if entry.IsDir() || filepath.Ext(path) != ".json" {
			return nil
		}
		data, err := os.ReadFile(path)
		if err != nil {
			return err
		}
		if bytes.Contains(data, key) || bytes.Contains(data, []byte(encoded)) {
			t.Fatal("key persisted in manifest")
		}
		var metadata struct {
			Algorithm string `json:"encryption_algorithm"`
			KeyID     string `json:"key_id"`
		}
		if err := json.Unmarshal(data, &metadata); err != nil {
			return err
		}
		if metadata.Algorithm != fmt.Sprintf("AES-%d-GCM", len(key)*8) || metadata.KeyID != "key-001" {
			t.Fatal("manifest encryption identity mismatch")
		}
		return nil
	}); err != nil {
		t.Fatal(err)
	}
}
