package gosql

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"testing"
)

func TestNativeQueryParametersAndStats(t *testing.T) {
	db, err := Open(Config{TempDir: t.TempDir(), ManifestDir: t.TempDir()})
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	ctx := context.Background()
	want := "Zhao's data"
	callback := func(chunk *Chunk) error {
		value, valid, err := chunk.String(0, 0)
		if err != nil || !valid || value != want {
			return fmt.Errorf("value=%q valid=%v err=%v, want %q", value, valid, err, want)
		}
		return nil
	}
	if _, err := db.QueryChunksWithArgs(ctx, "SELECT $value AS label", callback, sql.Named("value", want)); err != nil {
		t.Fatal(err)
	}
	_, stats, err := db.QueryChunksWithStats(ctx, "SELECT ? AS label", callback, want)
	if err != nil || stats.Rows != 1 || stats.Chunks != 1 || stats.PeakManagedBytes <= 0 || db.MemoryUsed() != 0 {
		t.Fatalf("stats=%+v memory=%d err=%v", stats, db.MemoryUsed(), err)
	}
	stop := errors.New("stop receiving")
	_, stats, err = db.QueryChunksWithStats(ctx, "SELECT $1", func(*Chunk) error { return stop }, 7)
	if !errors.Is(err, stop) || stats.Rows != 1 || db.MemoryUsed() != 0 {
		t.Fatalf("stats=%+v memory=%d err=%v", stats, db.MemoryUsed(), err)
	}
}

func TestQueryStatsFileReads(t *testing.T) {
	root := t.TempDir()
	for _, name := range []string{"a.csv", "b.csv"} {
		if err := os.WriteFile(filepath.Join(root, name), []byte("7\n8\n"), 0o600); err != nil {
			t.Fatal(err)
		}
	}
	uri, err := FileURI(filepath.Join(root, "*.csv"))
	if err != nil {
		t.Fatal(err)
	}
	db, err := Open(Config{LocalFiles: &LocalFileConfig{RootDir: root}, TempDir: t.TempDir(), ManifestDir: t.TempDir()})
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	_, stats, err := db.QueryChunksWithStats(context.Background(), "SELECT * FROM read_csv(?)", func(*Chunk) error { return nil }, uri)
	if err != nil || stats.Rows != 4 || stats.FilesMatched != 2 || stats.FilesRead != 2 || stats.ReadRequests < 2 || stats.BytesRead < 8 || db.MemoryUsed() != 0 {
		t.Fatalf("stats=%+v memory=%d err=%v", stats, db.MemoryUsed(), err)
	}
}

func TestCopyToWithArgs(t *testing.T) {
	root := t.TempDir()
	db, err := Open(Config{LocalFiles: &LocalFileConfig{RootDir: root}, TempDir: t.TempDir(), ManifestDir: t.TempDir()})
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	path := filepath.Join(root, "values.csv")
	uri, err := FileURI(path)
	if err != nil {
		t.Fatal(err)
	}
	options := CopyOptions{Format: CopyFormatCSV}
	ctx := context.Background()
	if _, err := db.CopyToWithArgs(ctx, "SELECT ? AS id", uri, options, 1, 2); err == nil || !strings.Contains(err.Error(), "got 2 arguments, want 1") {
		t.Fatalf("wrong parameter count error=%v", err)
	}
	result, err := db.CopyToWithArgs(ctx, "SELECT ? AS id", uri, options, 7)
	if err != nil || result.Rows != 1 {
		t.Fatalf("result=%+v err=%v", result, err)
	}
	data, err := os.ReadFile(path)
	if err != nil || string(data) != "id\n7\n" {
		t.Fatalf("data=%q err=%v", data, err)
	}
}
