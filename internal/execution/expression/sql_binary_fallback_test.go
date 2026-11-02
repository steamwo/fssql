package expression

import (
	"testing"

	"github.com/steamwo/fssql/internal/core/memory"
	"github.com/steamwo/fssql/internal/core/types"
	"github.com/steamwo/fssql/internal/core/vector"
	"github.com/steamwo/fssql/internal/sql/binder"
)

func TestExecuteSQLBooleanBinaryFallbackThreeValuedLogic(t *testing.T) {
	manager := memory.NewManager(1 << 20)
	scope := manager.NewScope()
	buffers := memory.NewBufferManager(nil)
	left, err := vector.NewFlat(scope, buffers, types.LogicalType{ID: types.Boolean}, 3)
	if err != nil {
		t.Fatal(err)
	}
	right, err := vector.NewFlat(scope, buffers, types.LogicalType{ID: types.Boolean}, 3)
	if err != nil {
		left.Release()
		t.Fatal(err)
	}
	left.SetBool(0, true)
	left.SetBool(1, false)
	left.SetNull(2)
	right.SetNull(0)
	right.SetNull(1)
	right.SetBool(2, true)
	chunk, err := vector.NewDataChunk(left, right)
	if err != nil {
		left.Release()
		right.Release()
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
	col := func(index int) *binder.BoundExpression {
		return &binder.BoundExpression{Kind: binder.BoundColumn, Type: types.LogicalType{ID: types.Boolean}, ColumnIndex: index}
	}
	for _, tc := range []struct {
		op        string
		wantValid []bool
		want      []bool
	}{
		{op: "AND", wantValid: []bool{false, true, false}, want: []bool{false, false, false}},
		{op: "OR", wantValid: []bool{true, false, true}, want: []bool{true, false, true}},
	} {
		expr := &binder.BoundExpression{Kind: binder.BoundBinary, Type: types.LogicalType{ID: types.Boolean}, Op: tc.op, Children: []*binder.BoundExpression{col(0), col(1)}}
		out, err := NewExecutor(scope, buffers).executeSQLBooleanBinary(expr, chunk)
		if err != nil {
			t.Fatalf("%s: %v", tc.op, err)
		}
		for row := range tc.wantValid {
			value, valid := out.Bool(row)
			if valid != tc.wantValid[row] || (valid && value != tc.want[row]) {
				out.Release()
				t.Fatalf("%s row=%d value=%v valid=%v", tc.op, row, value, valid)
			}
		}
		out.Release()
	}
}

func TestExecuteSQLBigIntBinaryFallbackComparison(t *testing.T) {
	manager := memory.NewManager(1 << 20)
	scope := manager.NewScope()
	buffers := memory.NewBufferManager(nil)
	left, err := vector.NewFlat(scope, buffers, types.LogicalType{ID: types.BigInt}, 3)
	if err != nil {
		t.Fatal(err)
	}
	right, err := vector.NewFlat(scope, buffers, types.LogicalType{ID: types.BigInt}, 3)
	if err != nil {
		left.Release()
		t.Fatal(err)
	}
	left.SetInt64(0, 1)
	left.SetInt64(1, 2)
	left.SetNull(2)
	right.SetInt64(0, 1)
	right.SetInt64(1, 1)
	right.SetInt64(2, 1)
	chunk, err := vector.NewDataChunk(left, right)
	if err != nil {
		left.Release()
		right.Release()
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
	col := func(index int) *binder.BoundExpression {
		return &binder.BoundExpression{Kind: binder.BoundColumn, Type: types.LogicalType{ID: types.BigInt}, ColumnIndex: index}
	}
	expr := &binder.BoundExpression{Kind: binder.BoundBinary, Type: types.LogicalType{ID: types.Boolean}, Op: "=", Children: []*binder.BoundExpression{col(0), col(1)}}
	out, err := NewExecutor(scope, buffers).executeSQLBigIntBinary(expr, chunk)
	if err != nil {
		t.Fatal(err)
	}
	defer out.Release()
	for row, want := range []bool{true, false} {
		value, valid := out.Bool(row)
		if !valid || value != want {
			t.Fatalf("row=%d value=%v valid=%v want=%v", row, value, valid, want)
		}
	}
	if out.IsValid(2) {
		t.Fatal("NULL BIGINT comparison must remain NULL")
	}
}
