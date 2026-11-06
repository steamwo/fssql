package expression

import (
	"testing"
	"time"

	"github.com/steamwo/fssql/internal/core/memory"
	"github.com/steamwo/fssql/internal/core/types"
	"github.com/steamwo/fssql/internal/core/vector"
	"github.com/steamwo/fssql/internal/function"
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

func TestExecuteSQLConstantTemporalCast(t *testing.T) {
	executor, chunk, cleanup := sqlExecutorFixture(t, 4)
	defer cleanup()
	catalog := function.NewQueryCatalog()
	timestampCast, ok := catalog.Lookup("__fssql_cast_timestamp")
	if !ok {
		t.Fatal("timestamp cast function missing")
	}
	text := &binder.BoundExpression{Kind: binder.BoundLiteral, Type: types.LogicalType{ID: types.Varchar}, Literal: "2024-07-01 00:00:00"}
	want := time.Date(2024, 7, 1, 0, 0, 0, 0, time.UTC).UnixMicro()
	for _, expr := range []*binder.BoundExpression{
		{Kind: binder.BoundFunction, FunctionKind: function.Scalar, FunctionID: timestampCast.ID, Type: types.LogicalType{ID: types.TimestampType}, Children: []*binder.BoundExpression{text}},
		{Kind: binder.BoundCast, Type: types.LogicalType{ID: types.TimestampType}, Children: []*binder.BoundExpression{text}},
	} {
		out, err := executor.ExecuteSQL(expr, chunk)
		if err != nil {
			t.Fatal(err)
		}
		if out.Kind != vector.Constant || out.Type.ID != types.TimestampType {
			t.Fatalf("kind=%v type=%v", out.Kind, out.Type.ID)
		}
		for row := 0; row < chunk.Cardinality; row++ {
			value, valid := out.Int64(row)
			if !valid || value != want {
				t.Fatalf("row=%d value=%d valid=%v want=%d", row, value, valid, want)
			}
		}
		out.Release()
	}
}
