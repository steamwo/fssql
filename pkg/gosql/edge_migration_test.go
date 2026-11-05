package gosql

import (
	"context"
	"encoding/binary"
	"encoding/json"
	"github.com/steamwo/fssql/internal/format/parquet"
	"io"
	"os"
	"path/filepath"
	"reflect"
	"testing"
	"time"
)

func migrationSource() TableFunction {
	return TableFunction{Name: "migration_rows", Columns: []Column{
		{Name: "u", Type: Type{ID: "UTINYINT"}, Nullable: true},
		{Name: "d", Type: Type{ID: "DECIMAL", Width: 10, Scale: 2}, Nullable: true},
		{Name: "b", Type: Type{ID: "BLOB"}, Nullable: true},
		{Name: "tz", Type: Type{ID: "TIMESTAMPTZ"}, Nullable: true},
		{Name: "ns", Type: Type{ID: "TIMESTAMP_NS"}, Nullable: true},
		{Name: "note", Type: Type{ID: "VARCHAR"}, Nullable: true},
	}, Open: func(context.Context) (BatchReader, error) {
		done := false
		return &batchReaderFunc{next: func(_ context.Context, w BatchWriter) (int, error) {
			if done {
				return 0, io.EOF
			}
			done = true
			tm := time.Date(2026, 1, 2, 3, 4, 5, 123456000, time.UTC)
			for _, err := range []error{w.SetUint64(0, 0, 255), w.SetDecimal(0, 1, "12.30"), w.SetBytes(0, 2, []byte{'A', 0, 255, '\\', '"', ',', '\n'}), w.SetTimestamp(0, 3, tm), w.SetTimestampNS(0, 4, tm.UnixNano()+789), w.SetString(0, 5, "x"), w.SetUint64(2, 0, 0), w.SetDecimal(2, 1, "0.00"), w.SetBytes(2, 2, []byte{}), w.SetString(2, 5, "")} {
				if err != nil {
					return 0, err
				}
			}
			return 3, io.EOF
		}}, nil
	}}
}

func TestEdgeMigrationReference(t *testing.T) {
	for i, name := range []string{"edge-agent-migration-reference.json", "edge-agent-caller-reference.json"} {
		t.Run([]string{"stable", "caller"}[i], func(t *testing.T) { testEdgeMigrationReference(t, name) })
	}
}

func testEdgeMigrationReference(t *testing.T, fixture string) {
	var ref struct {
		ReferenceVersion string            `json:"reference_version"`
		Types            []string          `json:"types"`
		CSV              map[string]string `json:"csv"`
		CastTZ           string            `json:"cast_tz"`
	}
	data, err := os.ReadFile("../../test/corpus/" + fixture)
	if err != nil {
		t.Fatal(err)
	}
	if err = json.Unmarshal(data, &ref); err != nil {
		t.Fatal(err)
	}
	if ref.ReferenceVersion != "2.0.0" && ref.ReferenceVersion != "1.5.5" {
		t.Fatal("wrong reference version")
	}
	ctx := context.Background()
	root := t.TempDir()
	config := Config{TableFunctions: []TableFunction{migrationSource()}, LocalFiles: &LocalFileConfig{RootDir: root}, TempDir: t.TempDir(), ManifestDir: t.TempDir()}
	sqlDB, err := OpenSQLDB(config)
	if err != nil {
		t.Fatal(err)
	}
	defer sqlDB.Close()
	rows, err := sqlDB.QueryContext(ctx, "SELECT * FROM migration_rows()")
	if err != nil {
		t.Fatal(err)
	}
	cols, err := rows.ColumnTypes()
	if err != nil {
		t.Fatal(err)
	}
	var names []string
	for _, c := range cols {
		names = append(names, c.DatabaseTypeName())
	}
	if !reflect.DeepEqual(names, ref.Types) {
		t.Fatalf("metadata=%v want=%v", names, ref.Types)
	}
	if err = rows.Close(); err != nil {
		t.Fatal(err)
	}
	rows, err = sqlDB.QueryContext(ctx, "DESCRIBE SELECT * FROM migration_rows()")
	if err != nil {
		t.Fatal(err)
	}
	names = nil
	for rows.Next() {
		var name, typ string
		var a, b, c, d any
		if err = rows.Scan(&name, &typ, &a, &b, &c, &d); err != nil {
			t.Fatal(err)
		}
		names = append(names, typ)
	}
	if err = rows.Err(); err != nil {
		t.Fatal(err)
	}
	rows.Close()
	if !reflect.DeepEqual(names, ref.Types) {
		t.Fatalf("describe=%v want=%v", names, ref.Types)
	}
	var text string
	if err = sqlDB.QueryRowContext(ctx, "SELECT CAST(tz AS VARCHAR) FROM migration_rows() LIMIT 1").Scan(&text); err != nil {
		t.Fatal(err)
	}
	if text != ref.CastTZ {
		t.Fatalf("cast=%q want=%q", text, ref.CastTZ)
	}
	db, err := Open(config)
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	for _, force := range []bool{false, true} {
		key := "plain"
		if force {
			key = "force"
		}
		path := filepath.Join(root, key+".csv")
		uri, _ := FileURI(path)
		res, err := db.CopyTo(ctx, "SELECT * FROM migration_rows()", uri, CopyOptions{Format: CopyFormatCSV, ForceQuote: force, DuckDBCompatible: true})
		if err != nil {
			t.Fatal(err)
		}
		if res.Rows != 3 {
			t.Fatal(res)
		}
		actual, err := os.ReadFile(path)
		if err != nil {
			t.Fatal(err)
		}
		if string(actual) != ref.CSV[key] {
			t.Fatalf("%s CSV=%q want=%q", key, actual, ref.CSV[key])
		}
	}
	if db.MemoryUsed() != 0 {
		t.Fatalf("memory leak %d", db.MemoryUsed())
	}
	path := filepath.Join(root, "snappy.parquet")
	uri, _ := FileURI(path)
	if _, err = db.CopyTo(ctx, "SELECT * FROM migration_rows()", uri, CopyOptions{Format: CopyFormatParquet, Compression: "snappy"}); err != nil {
		t.Fatal(err)
	}
	pq, err := os.ReadFile(path)
	if err != nil {
		t.Fatal(err)
	}
	size := int(binary.LittleEndian.Uint32(pq[len(pq)-8 : len(pq)-4]))
	meta, err := parquet.DecodeFileMetaData(pq[len(pq)-8-size : len(pq)-8])
	if err != nil {
		t.Fatal(err)
	}
	for _, group := range meta.RowGroups {
		for _, c := range group.Columns {
			if c.MetaData == nil || c.MetaData.Codec != parquet.CodecSnappy {
				t.Fatalf("wrong codec: %+v", c.MetaData)
			}
		}
	}
	names = nil
	_, err = db.QueryChunks(ctx, "DESCRIBE SELECT * FROM read_parquet('"+uri+"')", func(c *Chunk) error {
		for i := 0; i < c.RowCount(); i++ {
			name, _, e := c.String(i, 1)
			if e != nil {
				return e
			}
			names = append(names, name)
		}
		return nil
	})
	if err != nil {
		t.Fatal(err)
	}
	if !reflect.DeepEqual(names, ref.Types) {
		t.Fatalf("Parquet types=%v want=%v", names, ref.Types)
	}
}
