package gosql

import (
	"context"
	"encoding/binary"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/steamwo/fssql/internal/format/parquet"
)

func TestParquetCopyCompression(t *testing.T) {
	for _, tc := range []struct {
		name, sqlOptions, compression string
		source                        string
		api                           bool
		codec                         parquet.CompressionCodec
	}{
		{name: "sql_inferred", codec: parquet.CodecSnappy},
		{name: "sql_format", sqlOptions: "FORMAT parquet", codec: parquet.CodecSnappy},
		{name: "sql_none", sqlOptions: "COMPRESSION none", codec: parquet.CodecUncompressed},
		{name: "sql_identity", sqlOptions: "COMPRESSION identity", codec: parquet.CodecUncompressed},
		{name: "sql_snappy", sqlOptions: "COMPRESSION snappy", codec: parquet.CodecSnappy},
		{name: "api_default", api: true, codec: parquet.CodecSnappy},
		{name: "api_blank", api: true, compression: "  ", codec: parquet.CodecSnappy},
		{name: "api_none", api: true, compression: "none", codec: parquet.CodecUncompressed},
		{name: "api_identity", api: true, compression: "identity", codec: parquet.CodecUncompressed},
		{name: "api_snappy", api: true, compression: "snappy", codec: parquet.CodecSnappy},
		{name: "sql_csv_default", source: "csv", codec: parquet.CodecSnappy},
		{name: "sql_ndjson_default", source: "ndjson", codec: parquet.CodecSnappy},
		{name: "api_csv_default", api: true, source: "csv", codec: parquet.CodecSnappy},
		{name: "api_ndjson_default", api: true, source: "ndjson", codec: parquet.CodecSnappy},
	} {
		t.Run(tc.name, func(t *testing.T) {
			root := t.TempDir()
			db, err := Open(Config{LocalFiles: &LocalFileConfig{RootDir: root}, TempDir: t.TempDir(), ManifestDir: t.TempDir()})
			if err != nil {
				t.Fatal(err)
			}
			defer db.Close()
			path := filepath.Join(root, "output.parquet")
			uri, err := FileURI(path)
			if err != nil {
				t.Fatal(err)
			}
			literal := "'" + strings.ReplaceAll(uri, "'", "''") + "'"
			query := "SELECT 1 AS id, '记录' AS note UNION ALL SELECT 2, NULL UNION ALL SELECT 3, ''"
			if tc.source != "" {
				input := filepath.Join(root, "input."+tc.source)
				data := "id,note\n1,记录\n2,\n3,tail\n"
				if tc.source == "ndjson" {
					data = "{\"id\":1,\"note\":\"记录\"}\n{\"id\":2,\"note\":null}\n{\"id\":3,\"note\":\"\"}\n"
				}
				if err := os.WriteFile(input, []byte(data), 0600); err != nil {
					t.Fatal(err)
				}
				inputURI, err := FileURI(input)
				if err != nil {
					t.Fatal(err)
				}
				inputLiteral := "'" + strings.ReplaceAll(inputURI, "'", "''") + "'"
				query = "SELECT * FROM read_ndjson(" + inputLiteral + ")"
				if tc.source == "csv" {
					query = "SELECT * FROM read_csv(" + inputLiteral + ", HEADER=true, columns={'id':'BIGINT','note':'VARCHAR'})"
				}
			}
			ctx := context.Background()
			if tc.api {
				result, err := db.CopyTo(ctx, query, uri, CopyOptions{Format: CopyFormatParquet, Compression: tc.compression})
				if err != nil || result.Rows != 3 {
					t.Fatalf("CopyTo result=%+v err=%v", result, err)
				}
			} else {
				sql := "COPY (" + query + ") TO " + literal
				if tc.sqlOptions != "" {
					sql += " (" + tc.sqlOptions + ")"
				}
				if err := db.Exec(ctx, sql); err != nil {
					t.Fatal(err)
				}
			}
			data, err := os.ReadFile(path)
			if err != nil {
				t.Fatal(err)
			}
			if len(data) < 12 || string(data[:4]) != "PAR1" || string(data[len(data)-4:]) != "PAR1" {
				t.Fatal("invalid Parquet file")
			}
			footerLen := int(binary.LittleEndian.Uint32(data[len(data)-8 : len(data)-4]))
			if footerLen > len(data)-12 {
				t.Fatal("invalid Parquet footer length")
			}
			metadata, err := parquet.DecodeFileMetaData(data[len(data)-8-footerLen : len(data)-8])
			if err != nil {
				t.Fatal(err)
			}
			if metadata.NumRows != 3 || len(metadata.RowGroups) != 1 || len(metadata.RowGroups[0].Columns) != 2 {
				t.Fatalf("unexpected metadata: %+v", metadata)
			}
			for _, column := range metadata.RowGroups[0].Columns {
				if column.MetaData == nil || column.MetaData.Codec != tc.codec {
					t.Fatalf("column metadata=%+v, want codec %v", column.MetaData, tc.codec)
				}
			}
			rows := 0
			wantNotes := []string{"记录", "", ""}
			if tc.source == "csv" {
				wantNotes[2] = "tail"
			}
			_, err = db.QueryChunks(ctx, "SELECT id,note FROM read_parquet("+literal+") ORDER BY id", func(chunk *Chunk) error {
				for i := 0; i < chunk.RowCount(); i++ {
					id, valid, err := chunk.Int64(i, 0)
					if err != nil || !valid || id != int64(rows+1) || rows >= 3 {
						return fmt.Errorf("unexpected id %d valid=%v err=%v", id, valid, err)
					}
					note, valid, err := chunk.String(i, 1)
					if err != nil || valid != (rows != 1) || note != wantNotes[rows] {
						return fmt.Errorf("unexpected note %q valid=%v err=%v", note, valid, err)
					}
					rows++
				}
				return nil
			})
			if err != nil || rows != 3 || db.MemoryUsed() != 0 {
				t.Fatalf("readback rows=%d memory=%d err=%v", rows, db.MemoryUsed(), err)
			}
		})
	}
}
