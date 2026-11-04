package vector

import (
	"strings"
	"testing"

	"github.com/steamwo/fssql/internal/core/memory"
	"github.com/steamwo/fssql/internal/core/types"
)

func TestStringBatchWriterInlineAndHeapValues(t *testing.T) {
	manager := memory.NewManager(1 << 20)
	scope := manager.NewScope()
	bm := memory.NewBufferManager(nil)
	v, err := NewFlat(scope, bm, types.LogicalType{ID: types.Varchar}, 4)
	if err != nil {
		t.Fatal(err)
	}
	values := []string{"", "abcdefghijkl", "abcdefghijklm", strings.Repeat("x", 100)}
	writer, err := v.NewStringBatchWriter()
	if err != nil {
		t.Fatal(err)
	}
	for row, value := range values {
		if err := writer.Append(row, []byte(value)); err != nil {
			t.Fatal(err)
		}
	}
	writer.Commit()
	for row, want := range values {
		got, valid := v.String(row)
		if !valid || got != want {
			t.Fatalf("row %d value=%q valid=%v want=%q", row, got, valid, want)
		}
	}
	if v.heap.Used() == 0 {
		t.Fatal("expected external heap bytes")
	}

	second, err := v.NewStringBatchWriter()
	if err != nil {
		t.Fatal(err)
	}
	if err := second.Append(0, []byte(strings.Repeat("y", 64))); err != nil {
		t.Fatal(err)
	}
	second.Commit()
	if got, _ := v.String(0); got != strings.Repeat("y", 64) {
		t.Fatalf("second batch value=%q", got)
	}
	if err := second.Append(0, []byte("closed")); err == nil {
		t.Fatal("closed writer accepted a value")
	}
	v.Release()
	if err := scope.Close(); err != nil {
		t.Fatal(err)
	}
	if manager.Used() != 0 {
		t.Fatalf("managed leak=%d", manager.Used())
	}
}
