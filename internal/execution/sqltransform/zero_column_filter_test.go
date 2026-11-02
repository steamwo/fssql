package sqltransform

import (
	"context"
	"testing"

	"github.com/steamwo/fssql/internal/core/memory"
	"github.com/steamwo/fssql/internal/core/vector"
	"github.com/steamwo/fssql/internal/execution/expression"
)

func TestMaterializeSelectionPreservesZeroColumnCardinality(t *testing.T) {
	manager := memory.NewManager(1 << 20)
	scope := manager.NewScope()
	buffers := memory.NewBufferManager(nil)
	executor := expression.NewExecutor(scope, buffers)
	input := &vector.DataChunk{Cardinality: 1}

	selected, err := materializeSelection(context.Background(), executor, input, []int{0})
	if err != nil {
		t.Fatal(err)
	}
	if len(selected.Columns) != 0 || selected.Cardinality != 1 {
		selected.Release()
		t.Fatalf("columns=%d cardinality=%d", len(selected.Columns), selected.Cardinality)
	}
	selected.Release()

	empty, err := materializeSelection(context.Background(), executor, input, nil)
	if err != nil {
		t.Fatal(err)
	}
	if len(empty.Columns) != 0 || empty.Cardinality != 0 {
		empty.Release()
		t.Fatalf("empty columns=%d cardinality=%d", len(empty.Columns), empty.Cardinality)
	}
	empty.Release()

	if err := scope.Close(); err != nil {
		t.Fatal(err)
	}
	if used := manager.Used(); used != 0 {
		t.Fatalf("managed memory leak=%d", used)
	}
}
