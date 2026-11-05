package gosql

import (
	"context"
	"errors"
	"testing"

	"github.com/steamwo/fssql/internal/core/memory"
	"github.com/steamwo/fssql/internal/core/types"
	"github.com/steamwo/fssql/internal/core/vector"
)

func TestNativeQueryChunksUsesBorrowedOwnChunkAndReleasesMemory(t *testing.T) {
	db, err := Open(Config{MemoryLimitBytes: 8 << 20})
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	var calls, rows int
	columns, err := db.QueryChunks(context.Background(), "SELECT 42 AS answer, 'ok' AS label, true AS flag", func(chunk *Chunk) error {
		calls++
		if chunk.RowCount() != 1 || chunk.ColumnCount() != 3 {
			t.Fatalf("chunk rows=%d columns=%d", chunk.RowCount(), chunk.ColumnCount())
		}
		answer, ok, err := chunk.Int64(0, 0)
		if err != nil || !ok || answer != 42 {
			t.Fatalf("answer=%d ok=%v err=%v", answer, ok, err)
		}
		label, ok, err := chunk.String(0, 1)
		if err != nil || !ok || label != "ok" {
			t.Fatalf("label=%q ok=%v err=%v", label, ok, err)
		}
		flag, ok, err := chunk.Bool(0, 2)
		if err != nil || !ok || !flag {
			t.Fatalf("flag=%v ok=%v err=%v", flag, ok, err)
		}
		rows += chunk.RowCount()
		return nil
	})
	if err != nil {
		t.Fatal(err)
	}
	if calls != 1 || rows != 1 {
		t.Fatalf("calls=%d rows=%d", calls, rows)
	}
	if len(columns) != 3 || columns[0].Name != "answer" || columns[0].Type.ID != "INTEGER" || columns[1].Type.ID != "VARCHAR" || columns[2].Type.ID != "BOOLEAN" {
		t.Fatalf("columns=%+v", columns)
	}
	if used := db.MemoryUsed(); used != 0 {
		t.Fatalf("managed memory leak=%d", used)
	}
}

func TestNativeCallbackFailureReleasesQueryOwnership(t *testing.T) {
	db, err := Open(Config{MemoryLimitBytes: 8 << 20})
	if err != nil {
		t.Fatal(err)
	}
	want := errors.New("stop consumer")
	_, err = db.QueryChunks(context.Background(), "SELECT 1", func(*Chunk) error { return want })
	if !errors.Is(err, want) {
		t.Fatalf("error=%v", err)
	}
	if used := db.MemoryUsed(); used != 0 {
		t.Fatalf("managed memory leak after callback failure=%d", used)
	}
}

func TestNativeControlStatementAndViewUseSameDB(t *testing.T) {
	db, err := Open(Config{})
	if err != nil {
		t.Fatal(err)
	}
	if err := db.Exec(context.Background(), "CREATE VIEW v AS SELECT 7 AS value"); err != nil {
		t.Fatal(err)
	}
	var got int64
	_, err = db.QueryChunks(context.Background(), "SELECT value FROM v", func(chunk *Chunk) error {
		value, ok, err := chunk.Int64(0, 0)
		if err != nil || !ok {
			return err
		}
		got = value
		return nil
	})
	if err != nil {
		t.Fatal(err)
	}
	if got != 7 {
		t.Fatalf("value=%d", got)
	}
}

func TestNativeCloseRejectsNewQueries(t *testing.T) {
	db, err := Open(Config{})
	if err != nil {
		t.Fatal(err)
	}
	if err := db.Close(); err != nil {
		t.Fatal(err)
	}
	if _, err := db.QueryChunks(context.Background(), "SELECT 1", func(*Chunk) error { return nil }); err == nil {
		t.Fatal("expected closed DB error")
	}
}

func TestDatabaseSQLCompatibilityQueryAndMetadata(t *testing.T) {
	db, err := OpenSQLDB(Config{MemoryLimitBytes: 8 << 20})
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	rows, err := db.QueryContext(context.Background(), "SELECT 11 AS id, 'hello' AS name")
	if err != nil {
		t.Fatal(err)
	}
	defer rows.Close()
	columnTypes, err := rows.ColumnTypes()
	if err != nil {
		t.Fatal(err)
	}
	if len(columnTypes) != 2 || columnTypes[0].DatabaseTypeName() != "INTEGER" || columnTypes[1].DatabaseTypeName() != "VARCHAR" {
		t.Fatalf("column types=%v", columnTypes)
	}
	if !rows.Next() {
		t.Fatalf("Next: %v", rows.Err())
	}
	var id int64
	var name string
	if err := rows.Scan(&id, &name); err != nil {
		t.Fatal(err)
	}
	if id != 11 || name != "hello" {
		t.Fatalf("id=%d name=%q", id, name)
	}
	if rows.Next() {
		t.Fatal("unexpected second row")
	}
	if err := rows.Err(); err != nil {
		t.Fatal(err)
	}
}

func TestDatabaseSQLNullConversionUsesDriverNil(t *testing.T) {
	manager := memory.NewManager(1 << 20)
	scope := manager.NewScope()
	bm := memory.NewBufferManager(nil)
	v, err := vector.NewFlat(scope, bm, types.LogicalType{ID: types.Varchar}, 1)
	if err != nil {
		t.Fatal(err)
	}
	v.SetNull(0)
	value, err := sqlDriverValue(v, 0)
	v.Release()
	if err != nil {
		t.Fatal(err)
	}
	if value != nil {
		t.Fatalf("NULL driver value=%v", value)
	}
	if err := scope.Close(); err != nil {
		t.Fatal(err)
	}
	if manager.Used() != 0 {
		t.Fatalf("managed memory leak=%d", manager.Used())
	}
}

func TestDatabaseSQLPositionalParameter(t *testing.T) {
	db, err := OpenSQLDB(Config{})
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	var value int64
	if err := db.QueryRowContext(context.Background(), "SELECT ?", 1).Scan(&value); err != nil || value != 1 {
		t.Fatalf("parameter result=%d err=%v", value, err)
	}
}

func TestDatabaseSQLCanceledContextDoesNotLeak(t *testing.T) {
	db, err := OpenSQLDB(Config{})
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	if _, err := db.QueryContext(ctx, "SELECT 1"); !errors.Is(err, context.Canceled) {
		t.Fatalf("error=%v", err)
	}
}

func BenchmarkNativeQueryChunks(b *testing.B) {
	db, err := Open(Config{MemoryLimitBytes: 64 << 20})
	if err != nil {
		b.Fatal(err)
	}
	defer db.Close()
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		_, err := db.QueryChunks(context.Background(), "SELECT 1 + 2 AS value", func(chunk *Chunk) error {
			_, _, err := chunk.Int64(0, 0)
			return err
		})
		if err != nil {
			b.Fatal(err)
		}
	}
}
