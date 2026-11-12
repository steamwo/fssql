package gosql

import (
	"context"
	"fmt"
	"os"
	"path/filepath"
	"reflect"
	"strings"
	"testing"
)

func TestBulkSQLSessionIsolationSettingsAndParameters(t *testing.T) {
	root := t.TempDir()
	db, err := OpenSQLDB(Config{MemoryLimitBytes: 4 << 20, Workers: 2, LocalFiles: &LocalFileConfig{RootDir: root}, TempDir: t.TempDir(), ManifestDir: t.TempDir()})
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	db.SetMaxOpenConns(4)
	db.SetMaxIdleConns(0)
	ctx := context.Background()
	a, err := db.Conn(ctx)
	if err != nil {
		t.Fatal(err)
	}
	defer a.Close()
	b, err := db.Conn(ctx)
	if err != nil {
		t.Fatal(err)
	}
	defer b.Close()
	for _, q := range []string{"CREATE VIEW shared_v AS SELECT 1 AS n", "CREATE TEMP VIEW shared_v AS SELECT 2 AS n", "CREATE TEMP TABLE private_snapshot AS SELECT 'owned' AS v"} {
		if _, err := a.ExecContext(ctx, q); err != nil {
			t.Fatalf("%s: %v", q, err)
		}
	}
	for _, tc := range []struct {
		conn        string
		query, want string
	}{{"a", "SELECT n FROM shared_v", "2"}, {"a", "SELECT n FROM main.shared_v", "1"}, {"b", "SELECT n FROM shared_v", "1"}, {"a", "SELECT v FROM temp.private_snapshot", "owned"}} {
		conn := a
		if tc.conn == "b" {
			conn = b
		}
		_, rows := bulkRows(t, conn, tc.query)
		if !reflect.DeepEqual(rows, [][]string{{tc.want}}) {
			t.Fatalf("%s: %v", tc.query, rows)
		}
	}
	if rows, err := b.QueryContext(ctx, "SELECT * FROM temp.private_snapshot"); err == nil {
		rows.Close()
		t.Fatal("TEMP object crossed connection boundary")
	}
	var native *DB
	var token string
	if err := a.Raw(func(raw any) error {
		native = raw.(*sqlConn).db.owner
		r := native.materialized
		r.mu.Lock()
		defer r.mu.Unlock()
		for key, m := range r.items {
			if m.ownerSession != "" {
				token = key
			}
		}
		return nil
	}); err != nil {
		t.Fatal(err)
	}
	if token == "" {
		t.Fatal("temporary materialization not found")
	}
	if rows, err := b.QueryContext(ctx, fmt.Sprintf("SELECT * FROM fssql_materialized('%s')", token)); err == nil {
		rows.Close()
		t.Fatal("snapshot token bypassed TEMP isolation")
	}
	if _, err := a.ExecContext(ctx, "CREATE OR REPLACE TEMP VIEW shared_v AS SELECT 3 AS n"); err != nil {
		t.Fatal(err)
	}
	_, rows := bulkRows(t, a, "SELECT n FROM shared_v")
	if !reflect.DeepEqual(rows, [][]string{{"3"}}) {
		t.Fatal(rows)
	}
	statement, err := a.PrepareContext(ctx, "CREATE OR REPLACE VIEW parameter_v AS SELECT ? AS v")
	if err != nil {
		t.Fatal(err)
	}
	defer statement.Close()
	for _, value := range []any{int64(7), "quote'; SELECT 99 --"} {
		if _, err := statement.ExecContext(ctx, value); err != nil {
			t.Fatal(err)
		}
		_, rows := bulkRows(t, b, "SELECT v FROM parameter_v")
		if !reflect.DeepEqual(rows, [][]string{{fmt.Sprint(value)}}) {
			t.Fatalf("parameter view: %v", rows)
		}
	}
	if _, err := a.ExecContext(ctx, "CREATE TEMP TABLE parameter_snapshot AS SELECT ? AS v", "snapshot"); err != nil {
		t.Fatal(err)
	}
	output := filepath.Join(root, "parameter.csv")
	uri, err := FileURI(output)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := a.ExecContext(ctx, "COPY (SELECT CAST(? AS INTEGER) AS id) TO ? (FORMAT CSV, HEADER ?)", int64(42), uri, true); err != nil {
		t.Fatal(err)
	}
	data, err := os.ReadFile(output)
	if err != nil {
		t.Fatal(err)
	}
	if string(data) != "id\n42\n" {
		t.Fatalf("COPY: %q", data)
	}
	if _, err := a.ExecContext(ctx, "SET TimeZone = ?", "America/New_York"); err != nil {
		t.Fatal(err)
	}
	if _, err := a.ExecContext(ctx, "SET threads = ?", int64(4)); err != nil {
		t.Fatal(err)
	}
	zoneQuery := "SELECT CAST(CAST('2024-01-02 03:04:05Z' AS TIMESTAMPTZ) AS VARCHAR) AS t"
	_, rows = bulkRows(t, a, zoneQuery)
	if !reflect.DeepEqual(rows, [][]string{{"2024-01-01 22:04:05-05"}}) {
		t.Fatalf("local TimeZone: %v", rows)
	}
	_, rows = bulkRows(t, b, zoneQuery)
	if !reflect.DeepEqual(rows, [][]string{{"2024-01-02 03:04:05+00"}}) {
		t.Fatalf("TimeZone leaked: %v", rows)
	}
	_, rows = bulkRows(t, a, "SELECT CAST(CAST('2024-01-02 03:04:05' AS TIMESTAMPTZ) AS VARCHAR) AS t")
	if !reflect.DeepEqual(rows, [][]string{{"2024-01-02 03:04:05-05"}}) {
		t.Fatalf("naive TimeZone: %v", rows)
	}
	for _, q := range []string{"RESET TimeZone", "RESET threads"} {
		if _, err := a.ExecContext(ctx, q); err != nil {
			t.Fatal(err)
		}
	}
	if _, err := a.ExecContext(ctx, "SET threads = 0"); err == nil {
		t.Fatal("invalid threads accepted")
	}
	if _, err := a.ExecContext(ctx, "SET TimeZone = 'not/a/zone'"); err == nil {
		t.Fatal("invalid timezone accepted")
	}
	if err := statement.Close(); err != nil {
		t.Fatal(err)
	}
	if err := a.Close(); err != nil {
		t.Fatal(err)
	}
	native.materialized.mu.Lock()
	_, exists := native.materialized.items[token]
	native.materialized.mu.Unlock()
	if exists {
		t.Fatal("TEMP snapshot survived physical connection close")
	}
	_, rows = bulkRows(t, b, "SELECT n FROM shared_v")
	if !reflect.DeepEqual(rows, [][]string{{"1"}}) {
		t.Fatal(rows)
	}
	if err := b.Close(); err != nil {
		t.Fatal(err)
	}
	if err := db.Close(); err != nil {
		t.Fatal(err)
	}
	if used := native.MemoryUsed(); used != 0 {
		t.Fatalf("connection cleanup leaked %d", used)
	}
}

func TestBulkCSVFormatsCandidatesAndConstantArguments(t *testing.T) {
	root := t.TempDir()
	fixtures := map[string]string{"dates.csv": "d,ts\n31/01/2024,31/01/2024 12:13:14\n01/02/2024,01/02/2024 01:02:03\n", "integers.csv": "n\n1\n2\n", "mixed.csv": "d\n31/01/2024\n2024-02-01\n"}
	for name, data := range fixtures {
		if err := os.WriteFile(filepath.Join(root, name), []byte(data), 0600); err != nil {
			t.Fatal(err)
		}
	}
	db, err := OpenSQLDB(Config{MemoryLimitBytes: 8 << 20, LocalFiles: &LocalFileConfig{RootDir: root}, TempDir: t.TempDir(), ManifestDir: t.TempDir()})
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	uri := func(name string) string {
		u, err := FileURI(filepath.Join(root, name))
		if err != nil {
			t.Fatal(err)
		}
		return strings.ReplaceAll(u, "'", "''")
	}
	query := fmt.Sprintf("SELECT CAST(d AS VARCHAR) AS d,CAST(ts AS VARCHAR) AS ts FROM read_csv('%s',header=true,dateformat='%%d/%%m/%%Y',timestampformat='%%d/%%m/%%Y %%H:%%M:%%S') ORDER BY d", uri("dates.csv"))
	_, rows := bulkRows(t, db, query)
	want := [][]string{{"2024-01-31", "2024-01-31 12:13:14"}, {"2024-02-01", "2024-02-01 01:02:03"}}
	if !reflect.DeepEqual(rows, want) {
		t.Fatalf("formats: %v", rows)
	}
	types, rows := bulkRows(t, db, fmt.Sprintf("SELECT n FROM read_csv('%s',header=true,auto_type_candidates=['SMALLINT','INTEGER']) ORDER BY n", uri("integers.csv")))
	if !reflect.DeepEqual(types, []string{"SMALLINT"}) || !reflect.DeepEqual(rows, [][]string{{"1"}, {"2"}}) {
		t.Fatalf("candidates: %v %v", types, rows)
	}
	types, _ = bulkRows(t, db, fmt.Sprintf("SELECT n FROM read_csv('%s',header=true,auto_type_candidates=['VARCHAR'])", uri("integers.csv")))
	if !reflect.DeepEqual(types, []string{"VARCHAR"}) {
		t.Fatal(types)
	}
	types, rows = bulkRows(t, db, fmt.Sprintf("SELECT d FROM read_csv('%s',header=true)", uri("mixed.csv")))
	if !reflect.DeepEqual(types, []string{"VARCHAR"}) || !reflect.DeepEqual(rows, [][]string{{"31/01/2024"}, {"2024-02-01"}}) {
		t.Fatalf("mixed formats: %v %v", types, rows)
	}
	base := uri("integers.csv")
	_, rows = bulkRows(t, db, "SELECT n FROM read_csv(? || ?,header=true) ORDER BY n", strings.TrimSuffix(base, ".csv"), ".csv")
	if !reflect.DeepEqual(rows, [][]string{{"1"}, {"2"}}) {
		t.Fatalf("constant path: %v", rows)
	}
}
