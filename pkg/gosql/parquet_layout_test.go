package gosql

import (
	"bytes"
	"context"
	"encoding/binary"
	"errors"
	"fmt"
	"math"
	"os"
	"path/filepath"
	"reflect"
	"strings"
	"testing"

	"github.com/steamwo/fssql/internal/core/memory"
	"github.com/steamwo/fssql/internal/format/parquet"
	"github.com/steamwo/fssql/test/parquetcheck"
)

func TestParquetCopyLayout(t *testing.T) {
	const count = 8201
	for _, tc := range []struct {
		name        string
		group, page int
		wantGroup   int
		wantPage    int
		codec       string
	}{
		{"default", 0, 0, 2048, 512, "snappy"},
		{"large", 8192, 2048, 8192, 2048, "snappy"},
		{"group_only", 4096, 0, 4096, 512, "identity"},
		{"page_only", 0, 2048, 2048, 2048, "snappy"},
		{"small_group", 300, 0, 300, 300, "identity"},
		{"unaligned", 1000, 333, 1000, 333, "snappy"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			root := t.TempDir()
			db, err := Open(Config{Workers: 1, TableFunctions: []TableFunction{encryptionRows(count, false)}, LocalFiles: &LocalFileConfig{RootDir: root}, TempDir: t.TempDir(), ManifestDir: t.TempDir()})
			if err != nil {
				t.Fatal(err)
			}
			defer db.Close()
			path := filepath.Join(root, "layout.parquet")
			uri, _ := FileURI(path)
			result, err := db.CopyTo(context.Background(), "SELECT * FROM encryption_rows()", uri, CopyOptions{
				Format: CopyFormatParquet, Compression: tc.codec,
				ParquetRowGroupRows: tc.group, ParquetPageRows: tc.page,
			})
			if err != nil || result.Rows != count || db.MemoryUsed() != 0 {
				t.Fatalf("result=%+v memory=%d err=%v", result, db.MemoryUsed(), err)
			}
			data, err := os.ReadFile(path)
			if err != nil {
				t.Fatal(err)
			}
			if result.Bytes != int64(len(data)) || len(data) < 12 {
				t.Fatalf("result bytes=%d file=%d", result.Bytes, len(data))
			}
			footerLen := int(binary.LittleEndian.Uint32(data[len(data)-8:]))
			if footerLen > len(data)-12 {
				t.Fatal("invalid footer length")
			}
			meta, err := parquet.DecodeFileMetaData(data[len(data)-8-footerLen : len(data)-8])
			if err != nil || meta.NumRows != count || len(meta.RowGroups) != (count+tc.wantGroup-1)/tc.wantGroup {
				t.Fatalf("metadata=%+v err=%v", meta, err)
			}
			for groupIndex, group := range meta.RowGroups {
				wantRows := min(tc.wantGroup, count-groupIndex*tc.wantGroup)
				if group.NumRows != int64(wantRows) {
					t.Fatalf("group %d rows=%d want=%d", groupIndex, group.NumRows, wantRows)
				}
				for col, column := range group.Columns {
					if column.MetaData == nil {
						t.Fatal("missing column metadata")
					}
					offset := column.MetaData.DataPageOffset
					for row := 0; row < wantRows; row += tc.wantPage {
						if offset < 0 || offset >= int64(len(data)) {
							t.Fatal("invalid page offset")
						}
						header, size, err := parquet.DecodePageHeader(data[offset:])
						want := min(tc.wantPage, wantRows-row)
						if err != nil || header.Type != parquet.PageData || header.Data == nil || int(header.Data.NumValues) != want {
							t.Fatalf("group=%d col=%d row=%d header=%+v want=%d err=%v", groupIndex, col, row, header, want, err)
						}
						offset += int64(size) + int64(header.CompressedPageSize)
					}
				}
			}
			row := 0
			_, err = db.QueryChunks(context.Background(), "SELECT id,note FROM read_parquet('"+strings.ReplaceAll(uri, "'", "''")+"')", func(chunk *Chunk) error {
				for i := 0; i < chunk.RowCount(); i++ {
					id, note := encryptionValue(row)
					gotID, validID, err := chunk.Int64(i, 0)
					if err != nil || validID != (id != nil) || (validID && gotID != id.(int64)) {
						return fmt.Errorf("row %d id=%d valid=%v want=%v err=%v", row, gotID, validID, id, err)
					}
					gotNote, validNote, err := chunk.String(i, 1)
					if err != nil || validNote != (note != nil) || (validNote && gotNote != note.(string)) {
						return fmt.Errorf("row %d note=%q valid=%v want=%v err=%v", row, gotNote, validNote, note, err)
					}
					row++
				}
				return nil
			})
			if err != nil || row != count || db.MemoryUsed() != 0 {
				t.Fatalf("readback rows=%d memory=%d err=%v", row, db.MemoryUsed(), err)
			}
		})
	}
}

func TestSQLParquetCopyLayout(t *testing.T) {
	root := t.TempDir()
	db, err := OpenSQLDB(Config{LocalFiles: &LocalFileConfig{RootDir: root}, TempDir: t.TempDir(), ManifestDir: t.TempDir()})
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	path := filepath.Join(root, "layout.parquet")
	uri, err := FileURI(path)
	if err != nil {
		t.Fatal(err)
	}
	ctx := context.Background()
	query := "COPY (SELECT * FROM range(5)) TO ?"
	for _, options := range []string{
		"FORMAT CSV, PARQUET_ROW_GROUP_ROWS 0",
		"FORMAT PARQUET, PARQUET_ROW_GROUP_ROWS 3, PARQUET_PAGE_ROWS 4",
	} {
		if _, err := db.ExecContext(ctx, query+" ("+options+")", uri); err == nil {
			t.Fatalf("invalid options succeeded: %s", options)
		}
	}
	if _, err := db.ExecContext(ctx, query+" (FORMAT PARQUET, PARQUET_ROW_GROUP_ROWS ?, PARQUET_PAGE_ROWS 2)", uri, 3); err != nil {
		t.Fatal(err)
	}
	data, err := os.ReadFile(path)
	if err != nil || len(data) < 12 {
		t.Fatalf("read parquet bytes=%d err=%v", len(data), err)
	}
	footerLen := int(binary.LittleEndian.Uint32(data[len(data)-8:]))
	if footerLen > len(data)-12 {
		t.Fatal("invalid footer length")
	}
	meta, err := parquet.DecodeFileMetaData(data[len(data)-8-footerLen : len(data)-8])
	if err != nil || len(meta.RowGroups) != 2 || meta.RowGroups[0].NumRows != 3 || meta.RowGroups[1].NumRows != 2 {
		t.Fatalf("metadata=%+v err=%v", meta, err)
	}
	offset := meta.RowGroups[0].Columns[0].MetaData.DataPageOffset
	header, _, err := parquet.DecodePageHeader(data[offset:])
	if err != nil || header.Data == nil || header.Data.NumValues != 2 {
		t.Fatalf("first page=%+v err=%v", header, err)
	}
}

func TestParquetCopyInvalidLayoutHasNoSideEffects(t *testing.T) {
	root, manifests := t.TempDir(), t.TempDir()
	keyCalls := 0
	db, err := Open(Config{LocalFiles: &LocalFileConfig{RootDir: root}, TempDir: t.TempDir(), ManifestDir: manifests,
		ParquetKeys: func(context.Context, string, string) ([]byte, error) {
			keyCalls++
			return bytes.Repeat([]byte{1}, 16), nil
		}})
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	uri, _ := FileURI(filepath.Join(root, "invalid.parquet"))
	for _, tc := range []struct {
		name        string
		format      CopyFormat
		group, page int
	}{
		{"negative_group", CopyFormatParquet, -1, 0},
		{"negative_page", CopyFormatParquet, 0, -1},
		{"page_exceeds_group", CopyFormatParquet, 3, 4},
		{"page_exceeds_default_group", CopyFormatParquet, 0, 2049},
		{"group_overflow", CopyFormatParquet, math.MaxInt, 0},
		{"csv", CopyFormatCSV, 8192, 2048},
		{"json", CopyFormatJSON, 8192, 0},
		{"ndjson", CopyFormatNDJSON, 0, 2048},
		{"default_csv", "", 8192, 0},
	} {
		t.Run(tc.name, func(t *testing.T) {
			options := CopyOptions{
				Format: tc.format, ParquetRowGroupRows: tc.group, ParquetPageRows: tc.page,
			}
			if tc.format == CopyFormatParquet {
				options.Encryption = &ParquetEncryptionOptions{Algorithm: ParquetAES128GCM, KeyID: "layout-key"}
			}
			result, err := db.CopyTo(context.Background(), "SELECT 1 AS id", uri, options)
			if err == nil || !strings.Contains(strings.ToLower(err.Error()), "parquet") || !reflect.DeepEqual(result, ExportResult{}) || keyCalls != 0 || db.MemoryUsed() != 0 {
				t.Fatalf("result=%+v err=%v keyCalls=%d used=%d", result, err, keyCalls, db.MemoryUsed())
			}
			for _, dir := range []string{root, manifests} {
				entries, err := os.ReadDir(dir)
				if err != nil || len(entries) != 0 {
					t.Fatalf("side effects in %s: entries=%v err=%v", dir, entries, err)
				}
			}
		})
	}
}

func TestParquetCopyLayoutMemoryLimitCleanup(t *testing.T) {
	root := t.TempDir()
	db, err := Open(Config{MemoryLimitBytes: 1 << 20, LocalFiles: &LocalFileConfig{RootDir: root}, TempDir: t.TempDir(), ManifestDir: t.TempDir()})
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	uri, _ := FileURI(filepath.Join(root, "budget.parquet"))
	result, err := db.CopyTo(context.Background(), "SELECT 1 AS id", uri, CopyOptions{Format: CopyFormatParquet, ParquetRowGroupRows: 1 << 24})
	if !errors.Is(err, memory.ErrLimitExceeded) || !reflect.DeepEqual(result, ExportResult{}) || db.MemoryUsed() != 0 {
		t.Fatalf("result=%+v err=%v used=%d", result, err, db.MemoryUsed())
	}
	entries, err := os.ReadDir(root)
	if err != nil || len(entries) != 0 {
		t.Fatalf("incomplete output entries=%v err=%v", entries, err)
	}
}

func TestEncryptedParquetCopyLayout(t *testing.T) {
	const count = 8201
	root := t.TempDir()
	key := bytes.Repeat([]byte{9}, 16)
	db, err := Open(Config{Workers: 1, TableFunctions: []TableFunction{encryptionRows(count, false)}, LocalFiles: &LocalFileConfig{RootDir: root}, TempDir: t.TempDir(), ManifestDir: t.TempDir(),
		ParquetKeys: func(context.Context, string, string) ([]byte, error) { return key, nil }})
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	path := filepath.Join(root, "encrypted.parquet")
	uri, _ := FileURI(path)
	result, err := db.CopyTo(context.Background(), "SELECT * FROM encryption_rows()", uri, CopyOptions{
		Format: CopyFormatParquet, ParquetRowGroupRows: 8192, ParquetPageRows: 2048,
		Encryption: &ParquetEncryptionOptions{Algorithm: ParquetAES128GCM, KeyID: "layout-key"},
	})
	if err != nil || result.Rows != count || db.MemoryUsed() != 0 {
		t.Fatalf("result=%+v err=%v used=%d", result, err, db.MemoryUsed())
	}
	data, err := os.ReadFile(path)
	if err != nil {
		t.Fatal(err)
	}
	decoded := parquetcheck.Encrypted(t, data, key, "layout-key", 1)
	if decoded.Groups != 2 || decoded.Pages != 10 || len(decoded.Values) != count {
		t.Fatalf("groups=%d pages=%d rows=%d", decoded.Groups, decoded.Pages, len(decoded.Values))
	}
	for row, values := range decoded.Values {
		id, note := encryptionValue(row)
		if !reflect.DeepEqual(values, []any{id, note}) {
			t.Fatalf("row %d got=%v want=%v", row, values, []any{id, note})
		}
	}
}
