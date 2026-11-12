package gosql

import (
	"bufio"
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

// BenchmarkTextToParquet measures binding, text scanning, encoding and atomic
// local-file commit. Fixture generation, DB startup and output deletion are not timed.
func BenchmarkTextToParquet(b *testing.B) {
	for _, rows := range []int{100_000, 1_000_000} {
		b.Run(strconv.Itoa(rows), func(b *testing.B) {
			for _, format := range []string{"csv", "ndjson"} {
				b.Run(format, func(b *testing.B) {
					root := b.TempDir()
					input := writeTextConversionFixture(b, root, format, rows)
					info, err := os.Stat(input)
					if err != nil {
						b.Fatal(err)
					}
					inputURI, err := FileURI(input)
					if err != nil {
						b.Fatal(err)
					}
					query := "SELECT * FROM read_" + format + "('" + strings.ReplaceAll(inputURI, "'", "''") + "')"
					if format == "csv" {
						query = "SELECT * FROM read_csv('" + strings.ReplaceAll(inputURI, "'", "''") + "', HEADER=true, columns={'id':'BIGINT','account':'VARCHAR','region':'VARCHAR','amount':'DOUBLE','active':'BOOLEAN','note':'VARCHAR'})"
					}
					for _, codec := range []string{"snappy", "identity"} {
						b.Run(codec, func(b *testing.B) {
							db, err := Open(Config{MemoryLimitBytes: 256 << 20, Workers: 1, LocalFiles: &LocalFileConfig{RootDir: root}, TempDir: b.TempDir(), ManifestDir: b.TempDir()})
							if err != nil {
								b.Fatal(err)
							}
							defer db.Close()
							compression := codec
							if codec == "snappy" {
								compression = "" // Exercise the public default.
							}
							var outputBytes int64
							b.SetBytes(info.Size())
							b.ReportAllocs()
							b.ResetTimer()
							b.StopTimer()
							for i := 0; i < b.N; i++ {
								output := filepath.Join(root, fmt.Sprintf("%s-%d.parquet", codec, i))
								outputURI, err := FileURI(output)
								if err != nil {
									b.Fatal(err)
								}
								b.StartTimer()
								result, err := db.CopyTo(context.Background(), query, outputURI, CopyOptions{Format: CopyFormatParquet, Compression: compression})
								b.StopTimer()
								if err != nil || result.Rows != int64(rows) || result.Bytes <= 0 || db.MemoryUsed() != 0 {
									b.Fatalf("result=%+v memory=%d err=%v", result, db.MemoryUsed(), err)
								}
								outputBytes = result.Bytes
								if err := os.Remove(output); err != nil {
									b.Fatal(err)
								}
							}
							b.ReportMetric(float64(rows*b.N)/b.Elapsed().Seconds(), "rows/s")
							b.ReportMetric(float64(outputBytes), "parquet-B/op")
						})
					}
				})
			}
		})
	}
}

// writeTextConversionFixture produces matching six-column data with integers,
// high/low-cardinality strings, exact quarter-valued doubles, booleans and 10% NULL notes.
func writeTextConversionFixture(tb testing.TB, root, format string, rows int) string {
	tb.Helper()
	path := filepath.Join(root, "input."+format)
	file, err := os.Create(path)
	if err != nil {
		tb.Fatal(err)
	}
	defer file.Close()
	buffer := bufio.NewWriterSize(file, 64<<10)
	csvWriter, jsonWriter := csv.NewWriter(buffer), json.NewEncoder(buffer)
	if format == "csv" {
		if err := csvWriter.Write([]string{"id", "account", "region", "amount", "active", "note"}); err != nil {
			tb.Fatal(err)
		}
	}
	for i := 0; i < rows; i++ {
		row := struct {
			ID      int     `json:"id"`
			Account string  `json:"account"`
			Region  string  `json:"region"`
			Amount  float64 `json:"amount"`
			Active  bool    `json:"active"`
			Note    *string `json:"note"`
		}{ID: i, Account: fmt.Sprintf("acct-%08x", uint32(i)*2654435761), Region: fmt.Sprintf("region-%02d", i%32), Amount: float64(i%4000) / 4, Active: i%2 == 0}
		note := ""
		if i%10 != 0 {
			note = fmt.Sprintf("记录-%04d, \"ok\"", i%1000)
			row.Note = &note
		}
		if format == "csv" {
			err = csvWriter.Write([]string{strconv.Itoa(i), row.Account, row.Region, strconv.FormatFloat(row.Amount, 'f', 2, 64), strconv.FormatBool(row.Active), note})
		} else {
			err = jsonWriter.Encode(row)
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
