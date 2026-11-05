package vector

import (
	"errors"
	"strings"
	"testing"

	"github.com/steamwo/fssql/internal/core/memory"
	"github.com/steamwo/fssql/internal/core/types"
)

func TestStringBatchAppendPartsOwnsInlineAndHeapValues(t *testing.T) {
	manager := memory.NewManager(1 << 20)
	scope := manager.NewScope()
	buffers := memory.NewBufferManager(nil)
	v, err := NewFlat(scope, buffers, types.LogicalType{ID: types.Varchar}, 4)
	if err != nil {
		t.Fatal(err)
	}
	writer, err := v.NewStringBatchWriter()
	if err != nil {
		t.Fatal(err)
	}
	left := []byte("β")
	right := []byte("😊")
	if err := writer.AppendParts(0, left, right); err != nil {
		t.Fatal(err)
	}
	left[0] = 'x'
	right[0] = 'y'
	if err := writer.AppendParts(1, []byte(strings.Repeat("a", 2048)), []byte(strings.Repeat("b", 2048))); err != nil {
		t.Fatal(err)
	}
	if err := writer.AppendParts(2, nil, nil); err != nil {
		t.Fatal(err)
	}
	if err := writer.Append(3, []byte("single")); err != nil {
		t.Fatal(err)
	}
	writer.Commit()
	for row, want := range []string{"β😊", strings.Repeat("a", 2048) + strings.Repeat("b", 2048), "", "single"} {
		got, valid := v.Bytes(row)
		if !valid || string(got) != want {
			t.Fatalf("row=%d value=%q/%v", row, got, valid)
		}
	}
	if err := writer.AppendParts(0, nil, nil); err == nil {
		t.Fatal("closed writer accepted append")
	}
	v.Release()
	if scope.Used() != 0 {
		t.Fatalf("managed leak=%d", scope.Used())
	}
}

func TestStringBatchAppendPartsRespectsMemoryLimit(t *testing.T) {
	manager := memory.NewManager(256)
	scope := manager.NewScope()
	buffers := memory.NewBufferManager(nil)
	v, err := NewFlat(scope, buffers, types.LogicalType{ID: types.Varchar}, 1)
	if err != nil {
		t.Fatal(err)
	}
	writer, err := v.NewStringBatchWriter()
	if err != nil {
		t.Fatal(err)
	}
	err = writer.AppendParts(0, []byte(strings.Repeat("a", 200)), []byte(strings.Repeat("b", 200)))
	if !errors.Is(err, memory.ErrLimitExceeded) {
		t.Fatalf("budget err=%v", err)
	}
	writer.Commit()
	v.Release()
	if scope.Used() != 0 {
		t.Fatalf("managed leak=%d", scope.Used())
	}
}
