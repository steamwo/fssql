package gosql

import (
	"context"
	"os"
	"path/filepath"
	"sync/atomic"
	"testing"
)

func TestMigrationCTASAndViewReplacement(t *testing.T) {
	db, err := OpenSQLDB(Config{TableFunctions: []TableFunction{migrationSource()}, TempDir: t.TempDir(), ManifestDir: t.TempDir(), SpillMemoryBudgetBytes: 1})
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	ctx := context.Background()
	for _, sql := range []string{"CREATE TEMP TABLE frozen AS SELECT * FROM migration_rows()", "CREATE TEMP VIEW output AS SELECT u FROM frozen", "CREATE OR REPLACE TEMP VIEW output AS SELECT note FROM frozen"} {
		if _, err = db.ExecContext(ctx, sql); err != nil {
			t.Fatalf("%s: %v", sql, err)
		}
	}
	var n int64
	if err = db.QueryRow("SELECT count(*) FROM frozen").Scan(&n); err != nil || n != 3 {
		t.Fatalf("count=%d err=%v", n, err)
	}
	for _, q := range []string{"SELECT count(*) FROM frozen WHERE note IS NULL", "SELECT count(*) FROM frozen WHERE note='x'"} {
		if err = db.QueryRow(q).Scan(&n); err != nil || n != 1 {
			t.Fatalf("snapshot filter %s: count=%d err=%v", q, n, err)
		}
	}
	if _, err = db.Exec("CREATE OR REPLACE VIEW output AS SELECT missing FROM frozen"); err == nil {
		t.Fatal("invalid replacement accepted")
	}
	var value string
	if err = db.QueryRow("SELECT note FROM output LIMIT 1").Scan(&value); err != nil || value != "x" {
		t.Fatalf("old view lost: %s %v", value, err)
	}
	if _, err = db.Exec("CREATE TEMP TABLE frozen AS SELECT 1"); err == nil {
		t.Fatal("duplicate CTAS accepted")
	}
	if _, err = db.Exec("CREATE OR REPLACE TEMP VIEW frozen AS SELECT 1"); err == nil {
		t.Fatal("table replaced by view")
	}
}

func TestMigrationNullSafeComparisonEvaluatesOnce(t *testing.T) {
	var calls atomic.Int32
	fn := ScalarFunction{Name: "next_null", Result: Type{ID: "BIGINT"}, NullHandling: ScalarNullPropagate, Eval: func([]ScalarValue) (ScalarValue, error) {
		calls.Add(1)
		return ScalarValue{Type: Type{ID: "BIGINT"}, Valid: false}, nil
	}}
	db, err := OpenSQLDB(Config{ScalarFunctions: []ScalarFunction{fn}, TempDir: t.TempDir(), ManifestDir: t.TempDir()})
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	var a, b, c, d bool
	if err = db.QueryRow("SELECT next_null() IS NOT DISTINCT FROM next_null(),NULL IS NOT DISTINCT FROM NULL,1 IS DISTINCT FROM NULL,1 IS DISTINCT FROM 1").Scan(&a, &b, &c, &d); err != nil {
		t.Fatal(err)
	}
	if !a || !b || !c || d || calls.Load() != 2 {
		t.Fatalf("values %v %v %v %v calls %d", a, b, c, d, calls.Load())
	}
	rows, err := db.Query("SELECT 1 IS NOT DISTINCT FROM NULL,1 IS DISTINCT FROM NULL")
	if err != nil {
		t.Fatal(err)
	}
	defer rows.Close()
	cols, err := rows.ColumnTypes()
	if err != nil {
		t.Fatal(err)
	}
	for _, col := range cols {
		if nullable, ok := col.Nullable(); !ok || nullable {
			t.Fatalf("NULL-safe predicate nullable=%v known=%v", nullable, ok)
		}
	}
}

func TestMigrationUnsupportedScanners(t *testing.T) {
	db, err := Open(Config{TempDir: t.TempDir(), ManifestDir: t.TempDir()})
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	for _, sql := range []string{"ATTACH 'postgres:fixture' AS external", "SELECT * FROM postgres_scan('fixture','public','items')", "SELECT * FROM mysql_scan('fixture','public','items')"} {
		if err = db.Exec(context.Background(), sql); err == nil {
			t.Fatalf("unsupported scanner accepted: %s", sql)
		}
	}
}

func TestMigrationReaderAliasesAndForceQuote(t *testing.T) {
	root := t.TempDir()
	csv := filepath.Join(root, "input.csv")
	ndjson := filepath.Join(root, "input.ndjson")
	if err := os.WriteFile(csv, []byte("id,note\n1,hello\n"), 0600); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(ndjson, []byte("{\"id\":1}\n"), 0600); err != nil {
		t.Fatal(err)
	}
	uri, _ := FileURI(csv)
	jsonURI, _ := FileURI(ndjson)
	db, err := OpenSQLDB(Config{LocalFiles: &LocalFileConfig{RootDir: root}, TempDir: t.TempDir(), ManifestDir: t.TempDir()})
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	for _, q := range []string{"SELECT count(*) FROM read_csv_auto('" + uri + "',header=true,columns={'id':'BIGINT','note':'VARCHAR'})", "SELECT count(*) FROM read_json_auto('" + jsonURI + "',format='newline_delimited')"} {
		var n int64
		if err = db.QueryRow(q).Scan(&n); err != nil || n != 1 {
			t.Fatalf("%s: %d %v", q, n, err)
		}
	}
	stmt, err := db.Prepare("SELECT count(*) FROM read_csv_auto(?, header=?, columns=?)")
	if err != nil {
		t.Fatal(err)
	}
	defer stmt.Close()
	var n int64
	if err = stmt.QueryRow(uri, true, "id BIGINT,note VARCHAR").Scan(&n); err != nil || n != 1 {
		t.Fatalf("parameterized alias: %d %v", n, err)
	}
	out := filepath.Join(root, "out.csv")
	outURI, _ := FileURI(out)
	if _, err = db.Exec("COPY (SELECT 1 AS id,'x' AS note) TO '" + outURI + "' (FORMAT CSV, FORCE_QUOTE *)"); err != nil {
		t.Fatal(err)
	}
	data, err := os.ReadFile(out)
	if err != nil {
		t.Fatal(err)
	}
	if string(data) != "\"id\",\"note\"\n\"1\",\"x\"\n" {
		t.Fatalf("CSV %q", data)
	}
}
