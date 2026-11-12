package gosql

import (
	"bytes"
	"context"
	"database/sql"
	"database/sql/driver"
	"errors"
	"fmt"
	"math"
	"os"
	"path/filepath"
	"reflect"
	"strings"
	"sync"
	"testing"
	"time"
)

func TestDatabaseSQLParameterValues(t *testing.T) {
	db, err := OpenSQLDB(Config{TempDir: t.TempDir(), ManifestDir: t.TempDir()})
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	instant := time.Date(2026, 1, 2, 3, 4, 5, 123456789, time.FixedZone("offset", 8*3600))
	payload := "x'); SELECT 999; -- ? $name\x00你好"
	blob := []byte{0, '\'', 0xff, 0x80}
	var text string
	var number int64
	var floating float64
	var boolean bool
	var binary []byte
	var timestamp time.Time
	var null any
	err = db.QueryRowContext(context.Background(), "SELECT ?, ?, ?, ?, ?, ?, ?", payload, int64(math.MinInt64), 1.2345e100, true, blob, instant, nil).Scan(&text, &number, &floating, &boolean, &binary, &timestamp, &null)
	if err != nil {
		t.Fatal(err)
	}
	if text != payload || number != math.MinInt64 || floating != 1.2345e100 || !boolean || !bytes.Equal(binary, blob) || !timestamp.Equal(instant) || null != nil {
		t.Fatalf("unexpected bound values: %q %d %g %v %x %v %v", text, number, floating, boolean, binary, timestamp, null)
	}
	var isNull bool
	var typedNull sql.NullInt64
	if err := db.QueryRowContext(context.Background(), "SELECT ? IS NULL, CAST(? AS BIGINT)", nil, nil).Scan(&isNull, &typedNull); err != nil || !isNull || typedNull.Valid {
		t.Fatalf("NULL binding: %v %v %v", isNull, typedNull, err)
	}
	var nullArithmetic, nullComparison any
	if err := db.QueryRowContext(context.Background(), "SELECT ? + 1, ? = 1", nil, nil).Scan(&nullArithmetic, &nullComparison); err != nil || nullArithmetic != nil || nullComparison != nil {
		t.Fatalf("contextual NULL: %v %v %v", nullArithmetic, nullComparison, err)
	}
	var integer int64
	var decimal, huge string
	err = db.QueryRowContext(context.Background(), "SELECT $2 + $1, CAST($3 AS DECIMAL(38,2)), CAST($4 AS HUGEINT)", int64(3), int64(4), "123456789012345678901234567890.12", "170141183460469231731687303715884105727").Scan(&integer, &decimal, &huge)
	if err != nil || integer != 7 || decimal != "123456789012345678901234567890.12" || huge != "170141183460469231731687303715884105727" {
		t.Fatalf("numbered binding: %d %s %s %v", integer, decimal, huge, err)
	}
	if err := db.QueryRowContext(context.Background(), "SELECT $value + $value, $label", sql.Named("label", payload), sql.Named("value", int64(7))).Scan(&integer, &text); err != nil || integer != 14 || text != payload {
		t.Fatalf("named binding: %d %q %v", integer, text, err)
	}
	if err := db.QueryRowContext(context.Background(), "SELECT '? $ignored', ? -- ? $ignored\n /* ? */", payload).Scan(&text, &payload); err != nil || text != "? $ignored" {
		t.Fatalf("quoted/comment placeholders: %q %v", text, err)
	}
}

func TestDatabaseSQLPreparedStatementReuse(t *testing.T) {
	db, err := OpenSQLDB(Config{TableFunctions: []TableFunction{groupedRowsFunction()}, TempDir: t.TempDir(), ManifestDir: t.TempDir()})
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	stmt, err := db.PrepareContext(context.Background(), "SELECT count(*), (SELECT 7) FROM (SELECT id FROM grouped_rows()) AS d WHERE id > ?")
	if err != nil {
		t.Fatal(err)
	}
	defer stmt.Close()
	for _, threshold := range []int64{0, 3, 5, 1} {
		var count, constant int64
		if err := stmt.QueryRowContext(context.Background(), threshold).Scan(&count, &constant); err != nil || count != 5-threshold || constant != 7 {
			t.Fatalf("threshold=%d count=%d scalar=%d err=%v", threshold, count, constant, err)
		}
	}
	if _, err := stmt.ExecContext(context.Background(), 3); err != nil {
		t.Fatal(err)
	}
	var wg sync.WaitGroup
	failures := make(chan error, 8)
	for i := 0; i < 8; i++ {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			threshold := int64(i % 6)
			var count, constant int64
			if err := stmt.QueryRowContext(context.Background(), threshold).Scan(&count, &constant); err != nil {
				failures <- err
			} else if count != 5-threshold || constant != 7 {
				failures <- fmt.Errorf("concurrent result: %d %d", count, constant)
			}
		}(i)
	}
	wg.Wait()
	close(failures)
	for err := range failures {
		t.Error(err)
	}
	if err := stmt.Close(); err != nil {
		t.Fatal(err)
	}
	var count int64
	if err := stmt.QueryRowContext(context.Background(), 0).Scan(&count); err == nil {
		t.Fatal("closed statement succeeded")
	}
	named, err := db.PrepareContext(context.Background(), "SELECT $n + $n")
	if err != nil {
		t.Fatal(err)
	}
	defer named.Close()
	if err := named.QueryRowContext(context.Background(), sql.Named("n", 3)).Scan(&count); err != nil || count != 6 {
		t.Fatalf("named statement: %d %v", count, err)
	}
	limited, err := db.PrepareContext(context.Background(), "SELECT id FROM grouped_rows() ORDER BY id LIMIT ? OFFSET ?")
	if err != nil {
		t.Fatal(err)
	}
	defer limited.Close()
	for _, offset := range []int{0, 2} {
		rows, err := limited.QueryContext(context.Background(), 2, offset)
		if err != nil {
			t.Fatal(err)
		}
		var ids []int64
		for rows.Next() {
			var id int64
			if err := rows.Scan(&id); err != nil {
				t.Fatal(err)
			}
			ids = append(ids, id)
		}
		err = rows.Err()
		rows.Close()
		if err != nil || !reflect.DeepEqual(ids, []int64{int64(offset + 1), int64(offset + 2)}) {
			t.Fatalf("limit parameters: %v %v", ids, err)
		}
	}
}

func TestDatabaseSQLParameterizedCSVSourceAndOptions(t *testing.T) {
	root := t.TempDir()
	path := filepath.Join(root, "quoted's.csv")
	if err := os.WriteFile(path, []byte("id\n1\n2\n3\n"), 0600); err != nil {
		t.Fatal(err)
	}
	uri, err := FileURI(path)
	if err != nil {
		t.Fatal(err)
	}
	db, err := OpenSQLDB(Config{LocalFiles: &LocalFileConfig{RootDir: root}, TempDir: t.TempDir(), ManifestDir: t.TempDir()})
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	stmt, err := db.PrepareContext(context.Background(), "SELECT count(*) FROM read_csv(?, header=?, columns=?) WHERE id > ?")
	if err != nil {
		t.Fatal(err)
	}
	defer stmt.Close()
	for _, threshold := range []int{1, 2} {
		var count int64
		if err := stmt.QueryRowContext(context.Background(), uri, true, "id BIGINT", threshold).Scan(&count); err != nil || count != int64(3-threshold) {
			t.Fatalf("CSV parameter count=%d err=%v", count, err)
		}
	}
}

func TestDatabaseSQLParameterValidationAndTransactions(t *testing.T) {
	db, err := OpenSQLDB(Config{TempDir: t.TempDir(), ManifestDir: t.TempDir()})
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	cases := []struct {
		sql  string
		args []any
	}{
		{"SELECT ?", nil}, {"SELECT 1", []any{1}}, {"SELECT ?", []any{1, 2}},
		{"SELECT $name", []any{sql.Named("wrong", 1)}}, {"SELECT $name", []any{1}},
		{"SELECT $a, $b", []any{sql.Named("a", 1), sql.Named("a", 2)}},
		{"SELECT ?", []any{sql.Named("a", 1)}}, {"SELECT ?, $1", []any{1}},
		{"SELECT $0", []any{1}}, {"SELECT $2", []any{1, 2}},
		{"SELECT ?", []any{time.Date(1000, 1, 1, 0, 0, 0, 0, time.UTC)}},
	}
	for _, tc := range cases {
		var value any
		if err := db.QueryRowContext(context.Background(), tc.sql, tc.args...).Scan(&value); err == nil {
			t.Errorf("accepted invalid binding: %s", tc.sql)
		}
	}
	if _, err := db.PrepareContext(context.Background(), "SELECT ("); err == nil {
		t.Fatal("prepared invalid SQL")
	}
	for _, options := range []*sql.TxOptions{nil, {ReadOnly: true}, {Isolation: sql.LevelSerializable}} {
		if _, err := db.BeginTx(context.Background(), options); err == nil || !strings.Contains(err.Error(), "transactions are not supported") {
			t.Fatalf("transaction error: %v", err)
		}
	}
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	if _, err := db.PrepareContext(ctx, "SELECT ?"); !errors.Is(err, context.Canceled) {
		t.Fatalf("prepare cancellation: %v", err)
	}
	stmt, err := db.PrepareContext(context.Background(), "SELECT ?")
	if err != nil {
		t.Fatal(err)
	}
	defer stmt.Close()
	var value any
	if err := stmt.QueryRowContext(ctx, 1).Scan(&value); !errors.Is(err, context.Canceled) {
		t.Fatalf("query cancellation: %v", err)
	}
}

func TestDatabaseSQLPreparedEarlyCloseReleasesMemory(t *testing.T) {
	native := groupedRowsDB(t, 1<<20)
	conn := &sqlConn{db: native}
	statement, err := conn.PrepareContext(context.Background(), "SELECT id FROM grouped_rows() WHERE id > ?")
	if err != nil {
		t.Fatal(err)
	}
	defer statement.Close()
	stmt := statement.(driver.StmtQueryContext)
	for i := 0; i < 3; i++ {
		rows, err := stmt.QueryContext(context.Background(), []driver.NamedValue{{Ordinal: 1, Value: int64(i)}})
		if err != nil {
			t.Fatal(err)
		}
		if err := rows.Next(make([]driver.Value, 1)); err != nil {
			t.Fatal(err)
		}
		if err := rows.Close(); err != nil {
			t.Fatal(err)
		}
		if used := native.MemoryUsed(); used != 0 {
			t.Fatalf("managed memory leak: %d", used)
		}
	}
	if _, err := conn.execTemplate(context.Background(), "SELECT ?", statement.(*sqlStmt).template, []driver.NamedValue{{Ordinal: 1, Value: struct{}{}}}); err == nil {
		t.Fatal("unsupported driver value succeeded")
	}
	if err := statement.Close(); err != nil {
		t.Fatal(err)
	}
	if _, err := stmt.QueryContext(context.Background(), []driver.NamedValue{{Ordinal: 1, Value: int64(0)}}); err == nil {
		t.Fatal("closed driver statement succeeded")
	}
}
