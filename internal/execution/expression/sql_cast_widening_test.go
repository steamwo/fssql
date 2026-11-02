package expression

import (
	"testing"

	"github.com/steamwo/fssql/internal/core/memory"
	"github.com/steamwo/fssql/internal/core/types"
	"github.com/steamwo/fssql/internal/core/vector"
	"github.com/steamwo/fssql/internal/sql/binder"
)

func TestExecuteSQLIntegerToBigIntWideningCast(t *testing.T) {
	manager := memory.NewManager(1 << 20)
	scope := manager.NewScope()
	buffers := memory.NewBufferManager(nil)
	input, err := vector.NewFlat(scope, buffers, types.LogicalType{ID: types.Integer}, 2)
	if err != nil {
		t.Fatal(err)
	}
	input.SetInt32(0, 42)
	input.SetNull(1)
	chunk, err := vector.NewDataChunk(input)
	if err != nil {
		input.Release()
		t.Fatal(err)
	}
	defer func() {
		chunk.Release()
		if err := scope.Close(); err != nil {
			t.Fatal(err)
		}
		if used := manager.Used(); used != 0 {
			t.Fatalf("managed memory leak=%d", used)
		}
	}()

	expr := &binder.BoundExpression{
		Kind: binder.BoundCast,
		Type: types.LogicalType{ID: types.BigInt},
		Children: []*binder.BoundExpression{{
			Kind: binder.BoundColumn, Type: types.LogicalType{ID: types.Integer}, ColumnIndex: 0,
		}},
	}
	out, err := NewExecutor(scope, buffers).ExecuteSQL(expr, chunk)
	if err != nil {
		t.Fatal(err)
	}
	defer out.Release()
	value, ok := out.Int64(0)
	if !ok || value != 42 {
		t.Fatalf("value=%d ok=%v", value, ok)
	}
	if out.IsValid(1) {
		t.Fatal("NULL widening cast must remain NULL")
	}
}
