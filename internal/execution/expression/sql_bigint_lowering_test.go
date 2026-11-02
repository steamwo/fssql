package expression

import (
	"math"
	"testing"

	"github.com/steamwo/fssql/internal/core/memory"
	"github.com/steamwo/fssql/internal/core/types"
	"github.com/steamwo/fssql/internal/core/vector"
	"github.com/steamwo/fssql/internal/sql/binder"
)

// TestLowerSQLBigIntCompositeComparisons 验证四种缺失比较只组合现有 Equal/LessThan/Not kernel。
func TestLowerSQLBigIntCompositeComparisons(t *testing.T) {
	left := &binder.BoundExpression{Kind: binder.BoundColumn, Type: types.LogicalType{ID: types.BigInt}, ColumnIndex: 0}
	right := &binder.BoundExpression{Kind: binder.BoundColumn, Type: types.LogicalType{ID: types.BigInt}, ColumnIndex: 1}
	for _, tc := range []struct {
		op       string
		rootKind Kind
		child    Kind
	}{
		{op: "<>", rootKind: Not, child: Equal},
		{op: "<=", rootKind: Not, child: LessThan},
		{op: ">", rootKind: LessThan},
		{op: ">=", rootKind: Not, child: LessThan},
	} {
		got, err := LowerSQLExpression(&binder.BoundExpression{Kind: binder.BoundBinary, Type: types.LogicalType{ID: types.Boolean}, Op: tc.op, Children: []*binder.BoundExpression{left, right}})
		if err != nil {
			t.Fatalf("op=%s: %v", tc.op, err)
		}
		if got.Kind != tc.rootKind {
			t.Fatalf("op=%s root=%v want=%v", tc.op, got.Kind, tc.rootKind)
		}
		if tc.child != 0 && (len(got.Children) != 1 || got.Children[0].Kind != tc.child) {
			t.Fatalf("op=%s lowered=%+v", tc.op, got)
		}
	}
}

// TestLowerSQLBigIntUnaryComposition 验证一元正号为 identity，一元负号复用 Subtract kernel。
func TestLowerSQLBigIntUnaryComposition(t *testing.T) {
	child := &binder.BoundExpression{Kind: binder.BoundColumn, Type: types.LogicalType{ID: types.BigInt}, ColumnIndex: 0}
	plus, err := LowerSQLExpression(&binder.BoundExpression{Kind: binder.BoundUnary, Type: types.LogicalType{ID: types.BigInt}, Op: "+", Children: []*binder.BoundExpression{child}})
	if err != nil {
		t.Fatal(err)
	}
	if plus.Kind != Column || plus.ColumnIndex != 0 {
		t.Fatalf("plus=%+v", plus)
	}
	minus, err := LowerSQLExpression(&binder.BoundExpression{Kind: binder.BoundUnary, Type: types.LogicalType{ID: types.BigInt}, Op: "-", Children: []*binder.BoundExpression{child}})
	if err != nil {
		t.Fatal(err)
	}
	if minus.Kind != Subtract || len(minus.Children) != 2 || minus.Children[0].Kind != ConstantInt || minus.Children[0].IntValue != 0 {
		t.Fatalf("minus=%+v", minus)
	}
}

// TestExecuteSQLBigIntCompositeComparisons 验证组合 lowering 在真实 managed vectors 上保持比较结果。
func TestExecuteSQLBigIntCompositeComparisons(t *testing.T) {
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
	left.SetInt64(0, 5)
	left.SetInt64(1, 5)
	left.SetNull(2)
	right.SetInt64(0, 4)
	right.SetInt64(1, 5)
	right.SetInt64(2, 5)
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
	}()
	executor := NewExecutor(scope, buffers)
	col := func(index int) *binder.BoundExpression {
		return &binder.BoundExpression{Kind: binder.BoundColumn, Type: types.LogicalType{ID: types.BigInt}, ColumnIndex: index}
	}
	for _, tc := range []struct {
		op   string
		want []bool
	}{{">", []bool{true, false}}, {">=", []bool{true, true}}, {"<>", []bool{true, false}}, {"<=", []bool{false, true}}} {
		expr := &binder.BoundExpression{Kind: binder.BoundBinary, Type: types.LogicalType{ID: types.Boolean}, Op: tc.op, Children: []*binder.BoundExpression{col(0), col(1)}}
		out, err := executor.ExecuteSQL(expr, chunk)
		if err != nil {
			t.Fatalf("op=%s: %v", tc.op, err)
		}
		for row, want := range tc.want {
			value, ok := out.Bool(row)
			if !ok || value != want {
				out.Release()
				t.Fatalf("op=%s row=%d value=%v ok=%v", tc.op, row, value, ok)
			}
		}
		if out.IsValid(2) {
			out.Release()
			t.Fatalf("op=%s NULL comparison must remain NULL", tc.op)
		}
		out.Release()
	}
}

// TestExecuteSQLBigIntUnaryMinOverflow 验证 0-MinInt64 继续由 semantics.SubInt64 报 overflow。
func TestExecuteSQLBigIntUnaryMinOverflow(t *testing.T) {
	manager := memory.NewManager(1 << 20)
	scope := manager.NewScope()
	buffers := memory.NewBufferManager(nil)
	input, err := vector.NewFlat(scope, buffers, types.LogicalType{ID: types.BigInt}, 1)
	if err != nil {
		t.Fatal(err)
	}
	input.SetInt64(0, math.MinInt64)
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
	}()
	expr := &binder.BoundExpression{Kind: binder.BoundUnary, Type: types.LogicalType{ID: types.BigInt}, Op: "-", Children: []*binder.BoundExpression{{Kind: binder.BoundColumn, Type: types.LogicalType{ID: types.BigInt}, ColumnIndex: 0}}}
	if out, err := NewExecutor(scope, buffers).ExecuteSQL(expr, chunk); err == nil {
		if out != nil {
			out.Release()
		}
		t.Fatal("expected BIGINT unary overflow")
	}
}
