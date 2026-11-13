package gosql

import (
	"context"
	"io"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/steamwo/fssql/internal/core/memory"
	"github.com/steamwo/fssql/internal/core/types"
	"github.com/steamwo/fssql/internal/format/parquet"
	"github.com/steamwo/fssql/internal/format/scan"
	"github.com/steamwo/fssql/internal/storage/local"
	"github.com/steamwo/fssql/internal/storage/vfs"
)

func parquetStatisticsFixture(tb testing.TB) (*local.Driver, vfs.URI, vfs.ObjectInfo) {
	tb.Helper()
	root := tb.TempDir()
	db, err := Open(Config{MemoryLimitBytes: 64 << 20, Workers: 1, LocalFiles: &LocalFileConfig{RootDir: root}, TempDir: tb.TempDir(), ManifestDir: tb.TempDir()})
	if err != nil {
		tb.Fatal(err)
	}
	defer db.Close()
	uri, err := local.URIFromPath(filepath.Join(root, "ordered.parquet"))
	if err != nil {
		tb.Fatal(err)
	}
	result, err := db.CopyTo(context.Background(), "SELECT range AS id FROM range(8192)", uri.String(), CopyOptions{Format: CopyFormatParquet, Compression: "identity"})
	if err != nil || result.Rows != 8192 || db.MemoryUsed() != 0 {
		tb.Fatalf("copy=%+v memory=%d error=%v", result, db.MemoryUsed(), err)
	}
	driver, err := local.NewDriver(root)
	if err != nil {
		tb.Fatal(err)
	}
	info, err := driver.Stat(context.Background(), uri)
	if err != nil {
		tb.Fatal(err)
	}
	return driver, uri, info
}

func scanStatisticsFixture(tb testing.TB, driver vfs.Driver, uri vfs.URI, identity vfs.ObjectIdentity, footer parquet.Footer, scope *memory.Scope, bm *memory.BufferManager) (int, parquet.RowGroupScanMetrics) {
	tb.Helper()
	filter := scan.Filter{Column: "id", Op: scan.GreaterEqual, Value: scan.Literal{Type: types.LogicalType{ID: types.BigInt}, Int64: 7936}}
	rows := 0
	var total parquet.RowGroupScanMetrics
	for _, group := range footer.Metadata.RowGroups {
		reader, err := parquet.NewRowGroupScanner(driver, uri, identity, footer.Schema, group, []string{"id"}, []scan.Filter{filter}, scope, bm, 2048)
		if err != nil {
			tb.Fatal(err)
		}
		for {
			chunk, err := reader.Next(context.Background())
			if err == io.EOF {
				break
			}
			if err != nil {
				reader.Close()
				tb.Fatal(err)
			}
			for row := 0; row < chunk.Cardinality; row++ {
				value, valid := chunk.Columns[0].Int64(row)
				if !valid || value != int64(7936+rows) {
					tb.Fatalf("row=%d value=%d valid=%v", rows, value, valid)
				}
				rows++
			}
			chunk.Release()
		}
		reader.Close()
		metrics := reader.Metrics()
		total.RowGroupsPruned += metrics.RowGroupsPruned
		total.PagesSkipped += metrics.PagesSkipped
		total.PagesRead += metrics.PagesRead
		total.TransferredBytes += metrics.TransferredBytes
	}
	if rows != 256 || scope.Used() != 0 {
		tb.Fatalf("rows=%d managed=%d", rows, scope.Used())
	}
	return rows, total
}

func TestParquetExportStatisticsPruneReads(t *testing.T) {
	driver, uri, info := parquetStatisticsFixture(t)
	scope := memory.NewManager(64 << 20).NewScope()
	bm := memory.NewBufferManager(nil)
	footer, err := parquet.DiscoverFooter(context.Background(), driver, parquet.FooterSource{URI: uri, Size: info.Size, Identity: info.Identity}, scope, bm, 0)
	if err != nil {
		t.Fatal(err)
	}
	_, metrics := scanStatisticsFixture(t, driver, uri, info.Identity, footer, scope, bm)
	if metrics.RowGroupsPruned != 3 || metrics.PagesSkipped != 3 || metrics.PagesRead != 1 {
		t.Fatalf("unexpected scan metrics: %+v", metrics)
	}
	var fullBytes int64
	for _, group := range footer.Metadata.RowGroups {
		fullBytes += group.Columns[0].MetaData.TotalCompressedSize
	}
	if metrics.TransferredBytes >= fullBytes/2 {
		t.Fatalf("pruning did not reduce physical reads: metrics=%+v columnBytes=%d", metrics, fullBytes)
	}
}

// Retain a small typed artifact for independent reader/statistics checks when
// explicitly requested by the verification environment.
func TestParquetStatisticsTypedInterop(t *testing.T) {
	root := os.Getenv("FSSQL_PARQUET_STATS_DIR")
	if root == "" {
		root = t.TempDir()
	}
	if err := os.MkdirAll(root, 0700); err != nil {
		t.Fatal(err)
	}
	db, err := Open(Config{MemoryLimitBytes: 64 << 20, Workers: 1, LocalFiles: &LocalFileConfig{RootDir: root}, TempDir: t.TempDir(), ManifestDir: t.TempDir()})
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	uri, err := FileURI(filepath.Join(root, "typed.parquet"))
	if err != nil {
		t.Fatal(err)
	}
	query := "SELECT CAST(range-2048 AS DECIMAL(38,4)) AS d, CASE WHEN range%2=0 THEN CAST('18446744073709551615' AS UBIGINT) ELSE CAST(range AS UBIGINT) END AS u, CASE WHEN range%5=0 THEN CAST('NaN' AS DOUBLE) ELSE CAST(range-2048 AS DOUBLE) END AS f, CASE WHEN range%3=0 THEN NULL WHEN range%3=1 THEN '' ELSE '区域' END AS s, CASE WHEN range=0 THEN '" + strings.Repeat("x", 1025) + "' ELSE 'short' END AS long_s FROM range(4101)"
	result, err := db.CopyTo(context.Background(), query, uri, CopyOptions{Format: CopyFormatParquet})
	if err != nil || result.Rows != 4101 {
		t.Fatalf("copy=%+v error=%v", result, err)
	}
	var count int64
	_, err = db.QueryChunks(context.Background(), "SELECT COUNT(*) FROM read_parquet('"+strings.ReplaceAll(uri, "'", "''")+"') WHERE f > 100000", func(chunk *Chunk) error {
		var cellErr error
		count, _, cellErr = chunk.Int64(0, 0)
		return cellErr
	})
	if err != nil || count != 821 {
		t.Fatalf("NaN comparison count=%d error=%v", count, err)
	}
	_, err = db.QueryChunks(context.Background(), "SELECT COUNT(*) FROM read_parquet('"+strings.ReplaceAll(uri, "'", "''")+"') WHERE s = ''", func(chunk *Chunk) error {
		var cellErr error
		count, _, cellErr = chunk.Int64(0, 0)
		return cellErr
	})
	if err != nil || count != 1367 {
		t.Fatalf("empty string count=%d error=%v", count, err)
	}
	if db.MemoryUsed() != 0 {
		t.Fatalf("managed leak=%d", db.MemoryUsed())
	}
}

func BenchmarkParquetExportStatisticsPruning(b *testing.B) {
	driver, uri, info := parquetStatisticsFixture(b)
	scope := memory.NewManager(64 << 20).NewScope()
	bm := memory.NewBufferManager(nil)
	footer, err := parquet.DiscoverFooter(context.Background(), driver, parquet.FooterSource{URI: uri, Size: info.Size, Identity: info.Identity}, scope, bm, 0)
	if err != nil {
		b.Fatal(err)
	}
	var metrics parquet.RowGroupScanMetrics
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		_, metrics = scanStatisticsFixture(b, driver, uri, info.Identity, footer, scope, bm)
	}
	b.StopTimer()
	b.ReportMetric(float64(metrics.TransferredBytes), "read-B/op")
	b.ReportMetric(float64(metrics.RowGroupsPruned), "pruned-groups/op")
	b.ReportMetric(float64(metrics.PagesSkipped), "skipped-pages/op")
}
