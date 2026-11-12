package gosql

import (
	"bufio"
	"bytes"
	"context"
	"encoding/csv"
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"testing"
)

const wideConversionColumns = 64

func BenchmarkWideTextToParquet(b *testing.B) {
	const rows = 100_000
	for _, format := range []string{"csv", "ndjson"} {
		b.Run(format, func(b *testing.B) {
			root := b.TempDir()
			input := writeWideConversionFixture(b, root, format, rows)
			info, err := os.Stat(input)
			if err != nil {
				b.Fatal(err)
			}
			query := wideConversionQuery(b, input, format)
			for _, mode := range []string{"plain", "aes128", "aes256"} {
				b.Run(mode, func(b *testing.B) {
					db := openWideConversionDB(b, root)
					options := wideConversionOptions(mode)
					var outputBytes int64
					b.SetBytes(info.Size())
					b.ReportAllocs()
					b.ResetTimer()
					b.StopTimer()
					for i := 0; i < b.N; i++ {
						path := filepath.Join(root, fmt.Sprintf("%s-%d.parquet", mode, i))
						uri, err := FileURI(path)
						if err != nil {
							b.Fatal(err)
						}
						b.StartTimer()
						result, err := db.CopyTo(context.Background(), query, uri, options)
						b.StopTimer()
						if err != nil || result.Rows != rows || len(result.Columns) != wideConversionColumns || result.Bytes <= 0 || db.MemoryUsed() != 0 {
							b.Fatalf("rows=%d columns=%d bytes=%d memory=%d err=%v", result.Rows, len(result.Columns), result.Bytes, db.MemoryUsed(), err)
						}
						outputBytes = result.Bytes
						if err := os.Remove(path); err != nil {
							b.Fatal(err)
						}
					}
					b.ReportMetric(float64(rows*b.N)/b.Elapsed().Seconds(), "rows/s")
					b.ReportMetric(float64(info.Size()), "input-B/op")
					b.ReportMetric(float64(outputBytes), "parquet-B/op")
				})
			}
		})
	}
}

// TestWideTextToParquetArtifacts exercises multiple groups/pages at 64 columns.
// An explicit output directory retains the files for independent reader checks.
func TestWideTextToParquetArtifacts(t *testing.T) {
	root := os.Getenv("FSSQL_WIDE_INTEROP_DIR")
	if root == "" {
		root = t.TempDir()
	}
	const rows = 4101
	for _, format := range []string{"csv", "ndjson"} {
		t.Run(format, func(t *testing.T) {
			dir := filepath.Join(root, format)
			if err := os.MkdirAll(dir, 0700); err != nil {
				t.Fatal(err)
			}
			input := writeWideConversionFixture(t, dir, format, rows)
			query := wideConversionQuery(t, input, format)
			db := openWideConversionDB(t, dir)
			for _, mode := range []string{"plain", "aes128", "aes256"} {
				path := filepath.Join(dir, mode+".parquet")
				uri, err := FileURI(path)
				if err != nil {
					t.Fatal(err)
				}
				result, err := db.CopyTo(context.Background(), query, uri, wideConversionOptions(mode))
				if err != nil || result.Rows != rows || len(result.Columns) != wideConversionColumns || db.MemoryUsed() != 0 {
					t.Fatalf("%s rows=%d columns=%d memory=%d err=%v", mode, result.Rows, len(result.Columns), db.MemoryUsed(), err)
				}
				data, err := os.ReadFile(path)
				if err != nil {
					t.Fatal(err)
				}
				magic := "PAR1"
				if mode != "plain" {
					magic = "PARE"
				}
				if len(data) < 12 || int64(len(data)) != result.Bytes || string(data[:4]) != magic || string(data[len(data)-4:]) != magic {
					t.Fatalf("%s invalid output envelope/size", mode)
				}
			}
		})
	}
}

func openWideConversionDB(tb testing.TB, root string) *DB {
	tb.Helper()
	db, err := Open(Config{MemoryLimitBytes: 256 << 20, Workers: 1, LocalFiles: &LocalFileConfig{RootDir: root}, TempDir: tb.TempDir(), ManifestDir: tb.TempDir(), ParquetKeys: func(_ context.Context, id, algorithm string) ([]byte, error) {
		if id != "wide-benchmark-key" {
			return nil, fmt.Errorf("unexpected benchmark key ID")
		}
		n := 16
		if algorithm == ParquetAES256GCM {
			n = 32
		} else if algorithm != ParquetAES128GCM {
			return nil, fmt.Errorf("unexpected benchmark algorithm")
		}
		// Public synthetic test material, never a production key.
		return bytes.Repeat([]byte{0x79}, n), nil
	}})
	if err != nil {
		tb.Fatal(err)
	}
	tb.Cleanup(func() {
		if err := db.Close(); err != nil {
			tb.Error(err)
		}
	})
	return db
}

func wideConversionOptions(mode string) CopyOptions {
	opts := CopyOptions{Format: CopyFormatParquet}
	if mode != "plain" {
		algorithm := ParquetAES128GCM
		if mode == "aes256" {
			algorithm = ParquetAES256GCM
		}
		opts.Encryption = &ParquetEncryptionOptions{Algorithm: algorithm, KeyID: "wide-benchmark-key"}
	}
	return opts
}

func wideConversionQuery(tb testing.TB, input, format string) string {
	tb.Helper()
	uri, err := FileURI(input)
	if err != nil {
		tb.Fatal(err)
	}
	literal := "'" + strings.ReplaceAll(uri, "'", "''") + "'"
	if format == "ndjson" {
		return "SELECT * FROM read_ndjson(" + literal + ")"
	}
	var columns strings.Builder
	for col := 0; col < wideConversionColumns; col++ {
		if col > 0 {
			columns.WriteByte(',')
		}
		fmt.Fprintf(&columns, "'c%02d':'%s'", col, []string{"BIGINT", "DOUBLE", "VARCHAR", "BOOLEAN"}[col%4])
	}
	return "SELECT * FROM read_csv(" + literal + ", HEADER=true, columns={" + columns.String() + "})"
}

func writeWideConversionFixture(tb testing.TB, root, format string, rows int) string {
	tb.Helper()
	path := filepath.Join(root, "input."+format)
	file, err := os.Create(path)
	if err != nil {
		tb.Fatal(err)
	}
	defer file.Close()
	buffer := bufio.NewWriterSize(file, 64<<10)
	csvWriter, jsonWriter := csv.NewWriter(buffer), json.NewEncoder(buffer)
	names, record := make([]string, wideConversionColumns), make([]string, wideConversionColumns)
	values := make(map[string]any, wideConversionColumns)
	for col := range names {
		names[col] = fmt.Sprintf("c%02d", col)
	}
	if format == "csv" {
		if err := csvWriter.Write(names); err != nil {
			tb.Fatal(err)
		}
	}
	for row := 0; row < rows; row++ {
		for col, name := range names {
			var value any
			text := ""
			if col == 0 || (row+col)%10 != 0 {
				switch col % 4 {
				case 0:
					v := int64(row)*97 - int64(col*17)
					value, text = v, strconv.FormatInt(v, 10)
				case 1:
					v := float64((row*17+col)%4000)/4 - 500
					value, text = v, strconv.FormatFloat(v, 'f', 2, 64)
				case 2:
					if col%8 == 2 {
						text = fmt.Sprintf("acct-%02d-%08x", col, uint32(row)*2654435761)
					} else {
						text = fmt.Sprintf("区域-%02d, \"x\"", (row+col)%32)
					}
					value = text
				case 3:
					v := (row+col)%3 == 0
					value, text = v, strconv.FormatBool(v)
				}
			}
			values[name], record[col] = value, text
		}
		if format == "csv" {
			err = csvWriter.Write(record)
		} else {
			err = jsonWriter.Encode(values)
		}
		if err != nil {
			tb.Fatal(err)
		}
	}
	csvWriter.Flush()
	if err := csvWriter.Error(); err != nil {
		tb.Fatal(err)
	}
	if err := buffer.Flush(); err != nil {
		tb.Fatal(err)
	}
	if err := file.Close(); err != nil {
		tb.Fatal(err)
	}
	return path
}
