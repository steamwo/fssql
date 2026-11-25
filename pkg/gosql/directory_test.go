package gosql

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"io/fs"
	"os"
	"path/filepath"
	"strings"
	"testing"
)

func TestOpenDirectorySQLDB(t *testing.T) {
	ctx := context.Background()
	parent := t.TempDir()
	root := filepath.Join(parent, "sales data")
	if err := os.Mkdir(root, 0o700); err != nil {
		t.Fatal(err)
	}
	const manifest = `{"tables":[
		{"name":"orders","format":"parquet","pattern":"orders/**/*.parquet"},
		{"name":"customers","format":"parquet","pattern":"customers/*.parquet"}
	]}`
	if err := os.WriteFile(filepath.Join(root, "fssql.json"), []byte(manifest), 0o600); err != nil {
		t.Fatal(err)
	}
	config := Config{TempDir: t.TempDir(), ManifestDir: t.TempDir()}
	writerConfig := config
	writerConfig.LocalFiles = &LocalFileConfig{RootDir: root}
	writer, err := OpenSQLDB(writerConfig)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = writer.Close() })
	writePart := func(path, query string) {
		t.Helper()
		filename := filepath.Join(root, filepath.FromSlash(path))
		if err := os.MkdirAll(filepath.Dir(filename), 0o700); err != nil {
			t.Fatal(err)
		}
		uri, err := FileURI(filename)
		if err != nil {
			t.Fatal(err)
		}
		if _, err := writer.ExecContext(ctx, "COPY ("+query+") TO ? (FORMAT PARQUET)", uri); err != nil {
			t.Fatal(err)
		}
	}
	writePart("orders/2026-01-01/part.parquet", "SELECT 1 AS id, CAST('2026-01-01' AS DATE) AS order_date")
	writePart("customers/part.parquet", "SELECT 'Ada' AS name")

	openDirectory := func(root string) *sql.DB {
		t.Helper()
		db, err := OpenDirectorySQLDB(ctx, root, config)
		if err != nil {
			t.Fatal(err)
		}
		t.Cleanup(func() { _ = db.Close() })
		return db
	}
	checkTables := func(db *sql.DB, wantOrders, wantSecondDay int) {
		t.Helper()
		var orders, secondDay int
		var customer string
		if err := db.QueryRowContext(ctx, "SELECT count(*) FROM orders").Scan(&orders); err != nil {
			t.Fatal(err)
		}
		if err := db.QueryRowContext(ctx, "SELECT count(*) FROM orders WHERE order_date = CAST('2026-01-02' AS DATE)").Scan(&secondDay); err != nil {
			t.Fatal(err)
		}
		if err := db.QueryRowContext(ctx, "SELECT name FROM customers").Scan(&customer); err != nil {
			t.Fatal(err)
		}
		if orders != wantOrders || secondDay != wantSecondDay || customer != "Ada" {
			t.Fatalf("orders=%d, secondDay=%d, customer=%q, want %d, %d, Ada", orders, secondDay, customer, wantOrders, wantSecondDay)
		}
	}
	db := openDirectory(root)
	checkTables(db, 1, 0)
	writePart("orders/2026-01-02/part.parquet", "SELECT 2 AS id, CAST('2026-01-02' AS DATE) AS order_date")
	checkTables(db, 2, 1)

	output := filepath.Join(root, "output.parquet")
	outputURI, err := FileURI(output)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := db.ExecContext(ctx, "COPY (SELECT * FROM orders) TO ? (FORMAT PARQUET)", outputURI); err == nil {
		t.Fatal("directory database allowed local COPY")
	}
	if _, err := os.Stat(output); !errors.Is(err, fs.ErrNotExist) {
		t.Fatalf("local COPY output stat: %v, want not exist", err)
	}
	if err := db.Close(); err != nil {
		t.Fatal(err)
	}
	db = openDirectory(root)
	checkTables(db, 2, 1)
	if err := db.Close(); err != nil {
		t.Fatal(err)
	}
	if err := writer.Close(); err != nil {
		t.Fatal(err)
	}

	moved := filepath.Join(parent, "moved sales")
	if err := os.Rename(root, moved); err != nil {
		t.Fatal(err)
	}
	checkTables(openDirectory(moved), 2, 1)
}

func TestOpenDirectorySQLDBTextFormats(t *testing.T) {
	for _, tc := range []struct {
		format   string
		column   string
		first    string
		second   string
		mismatch string
	}{
		{format: "csv", column: "column0", first: "10\n20\n", second: "30\n", mismatch: "invalid\n"},
		{format: "json", column: "amount", first: `[{"amount":10},{"amount":20}]`, second: `[{"amount":30}]`, mismatch: `[{"amount":"invalid"}]`},
		{format: "ndjson", column: "amount", first: "{\"amount\":10}\n{\"amount\":20}\n", second: `{"amount":30}` + "\n", mismatch: `{"amount":"invalid"}` + "\n"},
	} {
		t.Run(tc.format, func(t *testing.T) {
			ctx := context.Background()
			root := t.TempDir()
			pattern := "orders/**/*." + tc.format
			manifest := `{"tables":[{"name":"orders","format":"` + tc.format + `","pattern":"` + pattern + `"}]}`
			if err := os.WriteFile(filepath.Join(root, directoryCatalogFile), []byte(manifest), 0o600); err != nil {
				t.Fatal(err)
			}
			writePart := func(day, data string) {
				t.Helper()
				dir := filepath.Join(root, "orders", day)
				if err := os.MkdirAll(dir, 0o700); err != nil {
					t.Fatal(err)
				}
				if err := os.WriteFile(filepath.Join(dir, "part."+tc.format), []byte(data), 0o600); err != nil {
					t.Fatal(err)
				}
			}
			writePart("day1", tc.first)
			db, err := OpenDirectorySQLDB(ctx, root, Config{TempDir: t.TempDir(), ManifestDir: t.TempDir()})
			if err != nil {
				t.Fatal(err)
			}
			defer db.Close()

			query := "SELECT count(*), sum(" + tc.column + ") FROM orders"
			checkOrders := func(wantRows, wantTotal int) {
				t.Helper()
				var rows, total int
				if err := db.QueryRowContext(ctx, query).Scan(&rows, &total); err != nil {
					t.Fatal(err)
				}
				if rows != wantRows || total != wantTotal {
					t.Fatalf("rows=%d, total=%d, want %d, %d", rows, total, wantRows, wantTotal)
				}
			}
			checkOrders(2, 30)
			writePart("day2", tc.second)
			checkOrders(3, 60)

			// A file that sorts first changes the inferred type at query binding.
			writePart("day0", tc.mismatch)
			var rows, total int
			err = db.QueryRowContext(ctx, query).Scan(&rows, &total)
			if err == nil || !strings.Contains(err.Error(), "schema drifted") {
				t.Fatalf("query error=%v, want schema drift", err)
			}
		})
	}
}

func TestOpenDirectorySQLDBReaderOptions(t *testing.T) {
	for _, tc := range []struct {
		name, format, options, first, earlier string
		wantFirst, wantTotal                  float64
	}{
		{"CSV header", "csv", `{"header":true}`, "amount\n10\n", "amount\n20\n", 10, 30},
		{"CSV columns", "csv", `{"header":true,"columns":"amount DOUBLE"}`, "label\n10.5\n", "label\n20\n", 10.5, 30.5},
		{"JSON columns", "json", `{"columns":"amount DOUBLE"}`, `[{"amount":10.5,"extra":true}]`, `[{"amount":20}]`, 10.5, 30.5},
		{"NDJSON columns", "ndjson", `{"columns":"amount DOUBLE"}`, "{\"amount\":10.5,\"extra\":true}\n", "{\"amount\":20}\n", 10.5, 30.5},
	} {
		t.Run(tc.name, func(t *testing.T) {
			root := t.TempDir()
			manifest := `{"tables":[{"name":"orders","format":"` + tc.format + `","pattern":"*.` + tc.format + `","options":` + tc.options + `}]}`
			for name, data := range map[string]string{directoryCatalogFile: manifest, "b." + tc.format: tc.first} {
				if err := os.WriteFile(filepath.Join(root, name), []byte(data), 0o600); err != nil {
					t.Fatal(err)
				}
			}
			ctx := context.Background()
			db, err := OpenDirectorySQLDB(ctx, root, Config{TempDir: t.TempDir(), ManifestDir: t.TempDir()})
			if err != nil {
				t.Fatal(err)
			}
			defer db.Close()
			check := func(wantCount int, wantTotal float64) {
				t.Helper()
				var count int
				var total float64
				err := db.QueryRowContext(ctx, "SELECT count(*), sum(amount) FROM orders").Scan(&count, &total)
				if err != nil || count != wantCount || total != wantTotal {
					t.Fatalf("count=%d, sum=%v, err=%v, want %d, %v", count, total, err, wantCount, wantTotal)
				}
			}
			check(1, tc.wantFirst)
			if err := os.WriteFile(filepath.Join(root, "a."+tc.format), []byte(tc.earlier), 0o600); err != nil {
				t.Fatal(err)
			}
			check(2, tc.wantTotal)
		})
	}
}

func TestOpenDirectorySQLDBValidatesDeclaredSchema(t *testing.T) {
	for _, tc := range []struct {
		format, valid, invalid, want string
	}{
		{"csv", "1\n", "wrong\n", "column 1: invalid BIGINT"},
		{"json", `[{"id":1}]`, `[{"id":"wrong"}]`, "field id: expected BIGINT"},
		{"ndjson", "{\"id\":1}\n", "{\"id\":\"wrong\"}\n", "field id: expected BIGINT"},
	} {
		t.Run(tc.format, func(t *testing.T) {
			root := t.TempDir()
			manifest := `{"tables":[{"name":"orders","format":"` + tc.format + `","pattern":"*.` + tc.format + `","options":{"columns":"id BIGINT","validate_schema":true}}]}`
			badFile := "b." + tc.format
			for name, content := range map[string]string{directoryCatalogFile: manifest, "a." + tc.format: tc.valid, badFile: tc.invalid} {
				if err := os.WriteFile(filepath.Join(root, name), []byte(content), 0o600); err != nil {
					t.Fatal(err)
				}
			}
			ctx := context.Background()
			config := Config{TempDir: t.TempDir(), ManifestDir: t.TempDir()}
			db, err := OpenDirectorySQLDB(ctx, root, config)
			if db != nil {
				_ = db.Close()
				t.Fatal("schema validation returned a database")
			}
			if err == nil || !strings.Contains(err.Error(), `validate schema of directory table "orders"`) || !strings.Contains(err.Error(), badFile) || !strings.Contains(err.Error(), tc.want) {
				t.Fatalf("open error = %v, want the table, %s and %q", err, badFile, tc.want)
			}
			if err := os.WriteFile(filepath.Join(root, badFile), []byte(tc.valid), 0o600); err != nil {
				t.Fatal(err)
			}
			db, err = OpenDirectorySQLDB(ctx, root, config)
			if err != nil {
				t.Fatal(err)
			}
			defer db.Close()
			var count int
			if err := db.QueryRowContext(ctx, "SELECT count(*) FROM orders").Scan(&count); err != nil || count != 2 {
				t.Fatalf("count=%d, err=%v, want 2", count, err)
			}
		})
	}
}

func TestOpenDirectorySQLDBParquetPartitions(t *testing.T) {
	ctx := context.Background()
	root := t.TempDir()
	config := Config{TempDir: t.TempDir(), ManifestDir: t.TempDir()}
	writerConfig := config
	writerConfig.LocalFiles = &LocalFileConfig{RootDir: root}
	writer, err := Open(writerConfig)
	if err != nil {
		t.Fatal(err)
	}
	defer writer.Close()
	for _, name := range []string{"loose.parquet", "year=2026/region=east/data.parquet"} {
		path := filepath.Join(root, filepath.FromSlash(name))
		if err := os.MkdirAll(filepath.Dir(path), 0o700); err != nil {
			t.Fatal(err)
		}
		uri, err := FileURI(path)
		if err != nil {
			t.Fatal(err)
		}
		if _, err := writer.CopyTo(ctx, "SELECT CAST(7 AS BIGINT) AS id, 'data' AS label", uri, CopyOptions{Format: CopyFormatParquet}); err != nil {
			t.Fatal(err)
		}
	}
	badDir := filepath.Join(root, "year=2027")
	if err := os.Mkdir(badDir, 0o700); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(badDir, "broken.parquet"), []byte("broken"), 0o600); err != nil {
		t.Fatal(err)
	}
	manifest := `{"tables":[{"name":"orders","format":"parquet","pattern":"**/*.parquet","options":{"partition_columns":"year BIGINT, region VARCHAR"}}]}`
	if err := os.WriteFile(filepath.Join(root, directoryCatalogFile), []byte(manifest), 0o600); err != nil {
		t.Fatal(err)
	}
	db, err := OpenDirectorySQLDB(ctx, root, config)
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	var year, id int64
	var region string
	err = db.QueryRowContext(ctx, "SELECT year, region, id FROM orders WHERE year = ? AND id = 7", 2026).Scan(&year, &region, &id)
	if err != nil || year != 2026 || region != "east" || id != 7 {
		t.Fatalf("year=%d region=%q id=%d err=%v", year, region, id, err)
	}
	var count int
	for predicate, want := range map[string]int{"year = 2028": 0, "year IS NULL": 1, "region = 'east'": 1} {
		err := db.QueryRowContext(ctx, "SELECT count(*) FROM orders WHERE "+predicate).Scan(&count)
		if err != nil || count != want {
			t.Fatalf("%s count=%d err=%v, want %d", predicate, count, err, want)
		}
	}
	if err := db.QueryRowContext(ctx, "SELECT count(*) FROM orders").Scan(&count); err == nil || !strings.Contains(err.Error(), "broken.parquet") {
		t.Fatalf("unfiltered query error=%v, want error for the corrupt partition", err)
	}
}

func TestOpenDirectorySQLDBExcludesManifest(t *testing.T) {
	root := t.TempDir()
	files := map[string]string{
		directoryCatalogFile: `{"tables":[{"name":"orders","format":"json","pattern":"*.json"}]}`,
		"orders.json":        `[{"id":7}]`,
	}
	for name, contents := range files {
		if err := os.WriteFile(filepath.Join(root, name), []byte(contents), 0o600); err != nil {
			t.Fatal(err)
		}
	}
	ctx := context.Background()
	db, err := OpenDirectorySQLDB(ctx, root, Config{TempDir: t.TempDir(), ManifestDir: t.TempDir()})
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	var count, id int
	if err := db.QueryRowContext(ctx, "SELECT count(*), sum(id) FROM orders").Scan(&count, &id); err != nil {
		t.Fatal(err)
	}
	if count != 1 || id != 7 {
		t.Fatalf("count=%d, id sum=%d, want 1, 7", count, id)
	}
}

func TestOpenDirectorySQLDBRejectsInvalidOptions(t *testing.T) {
	for _, tc := range []struct {
		name, format, options, want string
	}{
		{"header only for CSV", "json", `{"header":false}`, "option header requires format csv"},
		{"columns only for text", "parquet", `{"columns":"id BIGINT"}`, "option columns requires a text format"},
		{"validation requires columns", "csv", `{"validate_schema":true}`, "option validate_schema requires explicit text columns"},
		{"unsupported JSON type", "json", `{"columns":"id DATE"}`, "unsupported explicit type"},
		{"unknown option", "csv", `{"headers":true}`, `unknown field "headers"`},
	} {
		t.Run(tc.name, func(t *testing.T) {
			root := t.TempDir()
			manifest := `{"tables":[{"name":"orders","format":"` + tc.format + `","pattern":"orders.data","options":` + tc.options + `}]}`
			if err := os.WriteFile(filepath.Join(root, directoryCatalogFile), []byte(manifest), 0o600); err != nil {
				t.Fatal(err)
			}
			db, err := OpenDirectorySQLDB(context.Background(), root, Config{TempDir: t.TempDir(), ManifestDir: t.TempDir()})
			if db != nil {
				_ = db.Close()
				t.Fatal("invalid options returned a database")
			}
			if err == nil || !strings.Contains(err.Error(), tc.want) {
				t.Fatalf("open error=%v, want %q", err, tc.want)
			}
		})
	}
}

func TestOpenDirectorySQLDBRejectsInvalidCatalog(t *testing.T) {
	for _, tc := range []struct {
		name    string
		format  string
		pattern string
		want    string
	}{
		{name: "parent traversal", format: "parquet", pattern: "orders/../x.parquet", want: "relative file pattern"},
		{name: "absolute", format: "parquet", pattern: "/x.parquet", want: "relative file pattern"},
		{name: "drive-relative", format: "parquet", pattern: "C:x.parquet", want: "relative file pattern"},
		{name: "backslash", format: "parquet", pattern: `orders\..\x.parquet`, want: "relative file pattern"},
		{name: "unsupported format", format: "xml", pattern: "orders/*.xml", want: "unsupported format"},
		{name: "no matches", format: "parquet", pattern: "orders/*.parquet", want: `register directory table "orders"`},
		{name: "manifest as data", format: "json", pattern: directoryCatalogFile, want: `register directory table "orders"`},
	} {
		t.Run(tc.name, func(t *testing.T) {
			root := t.TempDir()
			catalog := map[string][]map[string]string{"tables": {{"name": "orders", "format": tc.format, "pattern": tc.pattern}}}
			data, err := json.Marshal(catalog)
			if err != nil {
				t.Fatal(err)
			}
			if err := os.WriteFile(filepath.Join(root, "fssql.json"), data, 0o600); err != nil {
				t.Fatal(err)
			}
			db, err := OpenDirectorySQLDB(context.Background(), root, Config{TempDir: t.TempDir(), ManifestDir: t.TempDir()})
			if db != nil {
				_ = db.Close()
				t.Fatal("invalid catalog returned a database")
			}
			if err == nil || !strings.Contains(err.Error(), tc.want) {
				t.Fatalf("open error=%v, want %q", err, tc.want)
			}
		})
	}
}
