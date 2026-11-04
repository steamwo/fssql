package gosql

import (
	"context"
	"fmt"
	"io"
	"math"
	"os"
	"path/filepath"
	"testing"
)

func remediationUnsignedRows() TableFunction {
	return TableFunction{
		Name: "remediation_unsigned_rows",
		Columns: []Column{{Name: "u", Type: Type{ID: "UBIGINT"}, Nullable: false}},
		Open: func(context.Context) (BatchReader, error) {
			emitted := false
			return &batchReaderFunc{next: func(_ context.Context, w BatchWriter) (int, error) {
				if emitted {
					return 0, io.EOF
				}
				emitted = true
				if err := w.SetUint64(0, 0, 1); err != nil {
					return 0, err
				}
				if err := w.SetUint64(1, 0, math.MaxUint64); err != nil {
					return 0, err
				}
				return 2, io.EOF
			}}, nil
		},
	}
}

func remediationBlobRows() TableFunction {
	return TableFunction{
		Name: "remediation_blob_rows",
		Columns: []Column{{Name: "b", Type: Type{ID: "BLOB"}, Nullable: false}},
		Open: func(context.Context) (BatchReader, error) {
			emitted := false
			return &batchReaderFunc{next: func(_ context.Context, w BatchWriter) (int, error) {
				if emitted {
					return 0, io.EOF
				}
				emitted = true
				if err := w.SetBytes(0, 0, []byte{0x00, 0xff, 0x41}); err != nil {
					return 0, err
				}
				return 1, io.EOF
			}}, nil
		},
	}
}

func TestRemediationUnsignedPredicateAndAggregate(t *testing.T) {
	db := testTypedDB(t, 1<<20, remediationUnsignedRows())
	var predicate []uint64
	_, err := db.QueryChunks(context.Background(), "SELECT u FROM remediation_unsigned_rows() WHERE u > CAST(0 AS UBIGINT) ORDER BY u", func(c *Chunk) error {
		for row := 0; row < c.RowCount(); row++ {
			v, ok, err := c.Uint64(row, 0)
			if err != nil || !ok {
				return fmt.Errorf("u valid=%v err=%v", ok, err)
			}
			predicate = append(predicate, v)
		}
		return nil
	})
	if err != nil {
		t.Fatal(err)
	}
	if len(predicate) != 2 || predicate[0] != 1 || predicate[1] != math.MaxUint64 {
		t.Fatalf("unsigned predicate=%v", predicate)
	}

	_, err = db.QueryChunks(context.Background(), "SELECT MIN(u), MAX(u), SUM(u) FROM remediation_unsigned_rows()", func(c *Chunk) error {
		min, ok, err := c.Uint64(0, 0)
		if err != nil || !ok || min != 1 {
			return fmt.Errorf("min=%d valid=%v err=%v", min, ok, err)
		}
		max, ok, err := c.Uint64(0, 1)
		if err != nil || !ok || max != math.MaxUint64 {
			return fmt.Errorf("max=%d valid=%v err=%v", max, ok, err)
		}
		sum, ok, err := c.Int128(0, 2)
		if err != nil || !ok || sum.String() != "18446744073709551616" {
			return fmt.Errorf("sum=%s valid=%v err=%v", sum.String(), ok, err)
		}
		return nil
	})
	if err != nil {
		t.Fatal(err)
	}
}

func TestRemediationCastRangeAndTemporalUnits(t *testing.T) {
	db, err := Open(Config{TempDir: t.TempDir(), ManifestDir: t.TempDir()})
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()

	if _, err := db.QueryChunks(context.Background(), "SELECT CAST(128 AS TINYINT)", func(*Chunk) error { return nil }); err == nil {
		t.Fatal("CAST(128 AS TINYINT) must fail instead of wrapping")
	}
	sql := "SELECT CAST(CAST(CAST('2020-01-02' AS DATE) AS TIMESTAMP) AS VARCHAR), CAST(CAST(CAST('2020-01-02 03:04:05.123456789' AS TIMESTAMP_NS) AS TIMESTAMP) AS VARCHAR)"
	_, err = db.QueryChunks(context.Background(), sql, func(c *Chunk) error {
		first, ok, e := c.String(0, 0)
		if e != nil || !ok || first != "2020-01-02 00:00:00" {
			return fmt.Errorf("date->timestamp=%q valid=%v err=%v", first, ok, e)
		}
		second, ok, e := c.String(0, 1)
		if e != nil || !ok || second != "2020-01-02 03:04:05.123456" {
			return fmt.Errorf("ns->us=%q valid=%v err=%v", second, ok, e)
		}
		return nil
	})
	if err != nil {
		t.Fatal(err)
	}
}

func TestRemediationLocalCopyAndBlobCSV(t *testing.T) {
	root := t.TempDir()
	outPath := filepath.Join(root, "blob.csv")
	outURI, err := FileURI(outPath)
	if err != nil {
		t.Fatal(err)
	}
	db, err := Open(Config{
		LocalFiles:     &LocalFileConfig{RootDir: root},
		TableFunctions: []TableFunction{remediationBlobRows()},
		TempDir:        t.TempDir(),
		ManifestDir:    t.TempDir(),
	})
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	header := false
	result, err := db.CopyTo(context.Background(), "SELECT b FROM remediation_blob_rows()", outURI, CopyOptions{Format: CopyFormatCSV, Header: &header})
	if err != nil {
		t.Fatal(err)
	}
	if result.Rows != 1 {
		t.Fatalf("copy rows=%d", result.Rows)
	}
	data, err := os.ReadFile(outPath)
	if err != nil {
		t.Fatal(err)
	}
	if string(data) != "00FF41\n" {
		t.Fatalf("blob csv=%q", data)
	}
}
