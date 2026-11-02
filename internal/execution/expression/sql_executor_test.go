package expression

import (
	"strings"
	"testing"

	"github.com/steamwo/fssql/internal/core/memory"
	"github.com/steamwo/fssql/internal/core/types"
	"github.com/steamwo/fssql/internal/core/vector"
	"github.com/steamwo/fssql/internal/sql/binder"
)

// sqlExecutorFixture 创建受 MemoryManager 管理的表达式执行测试环境。
func sqlExecutorFixture(t *testing.T, rows int) (*Executor, *vector.DataChunk, func()) {
	t.Helper()
	manager := memory.NewManager(1 << 20)
	scope := manager.NewScope()
	buffers := memory.NewBufferManager(nil)
	dummy, err := vector.NewFlat(scope, buffers, types.LogicalType{ID: types.Boolean}, rows)
	if err != nil {
		t.Fatal(err)
	}
	for i := 0; i < rows; i++ {
		dummy.SetBool(i, true)
	}
	chunk, err := vector.NewDataChunk(dummy)
	if err != nil {
		dummy.Release()
		t.Fatal(err)
	}
	cleanup := func() {
		chunk.Release()
		if err := scope.Close(); err != nil {
			t.Fatal(err)
		}
	}
	return NewExecutor(scope, buffers), chunk, cleanup
}

// TestExecuteSQLIntegerLiteral 验证 Binder 的 INTEGER literal 不再被迫改写成 BIGINT 才能执行。
func TestExecuteSQLIntegerLiteral(t *testing.T) {
	executor, chunk, cleanup := sqlExecutorFixture(t, 3)
	defer cleanup()
	out, err := executor.ExecuteSQL(&binder.BoundExpression{Kind: binder.BoundLiteral, Type: types.LogicalType{ID: types.Integer}, Literal: "42"}, chunk)
	if err != nil {
		t.Fatal(err)
	}
	defer out.Release()
	for row := 0; row < 3; row++ {
		value, ok := out.Int32(row)
		if !ok || value != 42 {
			t.Fatalf("row=%d value=%d ok=%v", row, value, ok)
		}
	}
}

// TestExecuteSQLIntegerArithmeticAndComparison 覆盖 INTEGER arithmetic 与六种 comparison 中的代表路径。
func TestExecuteSQLIntegerArithmeticAndComparison(t *testing.T) {
	executor, chunk, cleanup := sqlExecutorFixture(t, 2)
	defer cleanup()
	add := &binder.BoundExpression{
		Kind: binder.BoundBinary, Type: types.LogicalType{ID: types.Integer}, Op: "+",
		Children: []*binder.BoundExpression{
			{Kind: binder.BoundLiteral, Type: types.LogicalType{ID: types.Integer}, Literal: "7"},
			{Kind: binder.BoundLiteral, Type: types.LogicalType{ID: types.Integer}, Literal: "5"},
		},
	}
	value, err := executor.ExecuteSQL(add, chunk)
	if err != nil {
		t.Fatal(err)
	}
	v, ok := value.Int32(0)
	value.Release()
	if !ok || v != 12 {
		t.Fatalf("value=%d ok=%v", v, ok)
	}
	cmp := &binder.BoundExpression{Kind: binder.BoundBinary, Type: types.LogicalType{ID: types.Boolean}, Op: ">=", Children: add.Children}
	predicate, err := executor.ExecuteSQL(cmp, chunk)
	if err != nil {
		t.Fatal(err)
	}
	defer predicate.Release()
	got, ok := predicate.Bool(0)
	if !ok || !got {
		t.Fatalf("predicate=%v ok=%v", got, ok)
	}
}

// TestExecuteSQLIntegerOverflowAndModuloZero 确保 overflow/zero divisor 显式失败，不发生 wraparound 或 NULL 降级。
func TestExecuteSQLIntegerOverflowAndModuloZero(t *testing.T) {
	executor, chunk, cleanup := sqlExecutorFixture(t, 1)
	defer cleanup()
	cases := []*binder.BoundExpression{
		{Kind: binder.BoundBinary, Type: types.LogicalType{ID: types.Integer}, Op: "+", Children: []*binder.BoundExpression{{Kind: binder.BoundLiteral, Type: types.LogicalType{ID: types.Integer}, Literal: "2147483647"}, {Kind: binder.BoundLiteral, Type: types.LogicalType{ID: types.Integer}, Literal: "1"}}},
		{Kind: binder.BoundBinary, Type: types.LogicalType{ID: types.Integer}, Op: "%", Children: []*binder.BoundExpression{{Kind: binder.BoundLiteral, Type: types.LogicalType{ID: types.Integer}, Literal: "5"}, {Kind: binder.BoundLiteral, Type: types.LogicalType{ID: types.Integer}, Literal: "0"}}},
	}
	for i, expr := range cases {
		if out, err := executor.ExecuteSQL(expr, chunk); err == nil {
			if out != nil {
				out.Release()
			}
			t.Fatalf("case %d expected error", i)
		}
	}
}

// TestExecuteSQLVarcharComparisonAndNull 验证 VARCHAR literal 比较及 typed SQL NULL 传播。
func TestExecuteSQLVarcharComparisonAndNull(t *testing.T) {
	executor, chunk, cleanup := sqlExecutorFixture(t, 2)
	defer cleanup()
	left := &binder.BoundExpression{Kind: binder.BoundLiteral, Type: types.LogicalType{ID: types.Varchar}, Literal: "alpha"}
	right := &binder.BoundExpression{Kind: binder.BoundLiteral, Type: types.LogicalType{ID: types.Varchar}, Literal: "beta"}
	cmp := &binder.BoundExpression{Kind: binder.BoundBinary, Type: types.LogicalType{ID: types.Boolean}, Op: "<", Children: []*binder.BoundExpression{left, right}}
	out, err := executor.ExecuteSQL(cmp, chunk)
	if err != nil {
		t.Fatal(err)
	}
	value, ok := out.Bool(0)
	out.Release()
	if !ok || !value {
		t.Fatalf("value=%v ok=%v", value, ok)
	}

	null := &binder.BoundExpression{Kind: binder.BoundNull, Type: types.LogicalType{ID: types.Varchar}, Nullable: true}
	nullCmp := &binder.BoundExpression{Kind: binder.BoundBinary, Type: types.LogicalType{ID: types.Boolean}, Op: "=", Children: []*binder.BoundExpression{left, null}}
	out, err = executor.ExecuteSQL(nullCmp, chunk)
	if err != nil {
		t.Fatal(err)
	}
	defer out.Release()
	if out.IsValid(0) {
		t.Fatal("comparison with SQL NULL must produce NULL")
	}
}

// TestExecuteSQLUnaryIntegerOverflow 验证 INTEGER 最小值取负不会 wraparound。
func TestExecuteSQLUnaryIntegerOverflow(t *testing.T) {
	executor, chunk, cleanup := sqlExecutorFixture(t, 1)
	defer cleanup()
	expr := &binder.BoundExpression{Kind: binder.BoundUnary, Type: types.LogicalType{ID: types.Integer}, Op: "-", Children: []*binder.BoundExpression{{Kind: binder.BoundLiteral, Type: types.LogicalType{ID: types.Integer}, Literal: "-2147483648"}}}
	_, err := executor.ExecuteSQL(expr, chunk)
	if err == nil || !strings.Contains(err.Error(), "overflow") {
		t.Fatalf("err=%v", err)
	}
}
