package expression

import (
	"math"
	"testing"

	"github.com/steamwo/fssql/internal/core/memory"
	"github.com/steamwo/fssql/internal/core/types"
	"github.com/steamwo/fssql/internal/core/vector"
	"github.com/steamwo/fssql/internal/sql/binder"
)

// TestExecuteSQLIntegerDivisionUsesDouble 验证 DuckDB-compatible 1/2 经 INTEGER→DOUBLE cast 后返回 0.5。
func TestExecuteSQLIntegerDivisionUsesDouble(t *testing.T) {
	executor, chunk, cleanup := sqlExecutorFixture(t, 2)
	defer cleanup()
	integer := func(v string) *binder.BoundExpression {
		return &binder.BoundExpression{Kind: binder.BoundLiteral, Type: types.LogicalType{ID: types.Integer}, Literal: v}
	}
	cast := func(child *binder.BoundExpression) *binder.BoundExpression {
		return &binder.BoundExpression{Kind: binder.BoundCast, Type: types.LogicalType{ID: types.Double}, Children: []*binder.BoundExpression{child}}
	}
	expr := &binder.BoundExpression{Kind: binder.BoundBinary, Type: types.LogicalType{ID: types.Double}, Op: "/", Children: []*binder.BoundExpression{cast(integer("1")), cast(integer("2"))}}
	out, err := executor.ExecuteSQL(expr, chunk)
	if err != nil {
		t.Fatal(err)
	}
	defer out.Release()
	for row := 0; row < 2; row++ {
		value, ok := out.Float64(row)
		if !ok || value != 0.5 {
			t.Fatalf("row=%d value=%v ok=%v", row, value, ok)
		}
	}
}

// TestExecuteSQLDoubleDivisionByZeroUsesIEEE 验证默认 IEEE 浮点除零语义：1/0=+Inf，0/0=NaN。
func TestExecuteSQLDoubleDivisionByZeroUsesIEEE(t *testing.T) {
	executor, chunk, cleanup := sqlExecutorFixture(t, 1)
	defer cleanup()
	literal := func(v string) *binder.BoundExpression {
		return &binder.BoundExpression{Kind: binder.BoundLiteral, Type: types.LogicalType{ID: types.Double}, Literal: v}
	}
	for _, tc := range []struct {
		left string
		want func(float64) bool
	}{
		{left: "1", want: func(v float64) bool { return math.IsInf(v, 1) }},
		{left: "0", want: math.IsNaN},
	} {
		expr := &binder.BoundExpression{Kind: binder.BoundBinary, Type: types.LogicalType{ID: types.Double}, Op: "/", Children: []*binder.BoundExpression{literal(tc.left), literal("0")}}
		out, err := executor.ExecuteSQL(expr, chunk)
		if err != nil {
			t.Fatal(err)
		}
		value, ok := out.Float64(0)
		out.Release()
		if !ok || !tc.want(value) {
			t.Fatalf("%s/0 value=%v ok=%v", tc.left, value, ok)
		}
	}
}

// TestExecuteSQLDoubleNaNTotalOrder 验证 DuckDB 特例：NaN=NaN 且 NaN 大于任意普通 DOUBLE。
func TestExecuteSQLDoubleNaNTotalOrder(t *testing.T) {
	manager := memory.NewManager(1 << 20)
	scope := manager.NewScope()
	buffers := memory.NewBufferManager(nil)
	left, err := vector.NewFlat(scope, buffers, types.LogicalType{ID: types.Double}, 2)
	if err != nil {
		t.Fatal(err)
	}
	right, err := vector.NewFlat(scope, buffers, types.LogicalType{ID: types.Double}, 2)
	if err != nil {
		left.Release()
		t.Fatal(err)
	}
	left.SetFloat64(0, math.NaN())
	left.SetFloat64(1, math.NaN())
	right.SetFloat64(0, math.NaN())
	right.SetFloat64(1, 3)
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
		return &binder.BoundExpression{Kind: binder.BoundColumn, Type: types.LogicalType{ID: types.Double}, ColumnIndex: index}
	}
	eq := &binder.BoundExpression{Kind: binder.BoundBinary, Type: types.LogicalType{ID: types.Boolean}, Op: "=", Children: []*binder.BoundExpression{col(0), col(1)}}
	out, err := executor.ExecuteSQL(eq, chunk)
	if err != nil {
		t.Fatal(err)
	}
	value, ok := out.Bool(0)
	out.Release()
	if !ok || !value {
		t.Fatalf("NaN=NaN value=%v ok=%v", value, ok)
	}
	gt := &binder.BoundExpression{Kind: binder.BoundBinary, Type: types.LogicalType{ID: types.Boolean}, Op: ">", Children: []*binder.BoundExpression{col(0), col(1)}}
	out, err = executor.ExecuteSQL(gt, chunk)
	if err != nil {
		t.Fatal(err)
	}
	defer out.Release()
	value, ok = out.Bool(1)
	if !ok || !value {
		t.Fatalf("NaN>3 value=%v ok=%v", value, ok)
	}
}

// TestExecuteSQLDoubleArithmeticAndUnary 覆盖 DOUBLE +,-,* 与一元负号。
func TestExecuteSQLDoubleArithmeticAndUnary(t *testing.T) {
	executor, chunk, cleanup := sqlExecutorFixture(t, 1)
	defer cleanup()
	literal := func(v string) *binder.BoundExpression {
		return &binder.BoundExpression{Kind: binder.BoundLiteral, Type: types.LogicalType{ID: types.Double}, Literal: v}
	}
	for _, tc := range []struct {
		op   string
		want float64
	}{{"+", 5.5}, {"-", 1.5}, {"*", 7.0}} {
		expr := &binder.BoundExpression{Kind: binder.BoundBinary, Type: types.LogicalType{ID: types.Double}, Op: tc.op, Children: []*binder.BoundExpression{literal("3.5"), literal("2")}}
		out, err := executor.ExecuteSQL(expr, chunk)
		if err != nil {
			t.Fatal(err)
		}
		value, ok := out.Float64(0)
		out.Release()
		if !ok || value != tc.want {
			t.Fatalf("op=%s value=%v ok=%v", tc.op, value, ok)
		}
	}
	neg := &binder.BoundExpression{Kind: binder.BoundUnary, Type: types.LogicalType{ID: types.Double}, Op: "-", Children: []*binder.BoundExpression{literal("1.25")}}
	out, err := executor.ExecuteSQL(neg, chunk)
	if err != nil {
		t.Fatal(err)
	}
	defer out.Release()
	value, ok := out.Float64(0)
	if !ok || value != -1.25 {
		t.Fatalf("value=%v ok=%v", value, ok)
	}
}

// TestExecuteSQLDoubleModuloRemainsFailClosed 防止未经 differential 验证的浮点 modulo 被猜测实现。
func TestExecuteSQLDoubleModuloRemainsFailClosed(t *testing.T) {
	executor, chunk, cleanup := sqlExecutorFixture(t, 1)
	defer cleanup()
	literal := func(v string) *binder.BoundExpression {
		return &binder.BoundLiteralExpr
		_ = v
	}
	_ = literal
	expr := &binder.BoundExpression{Kind: binder.BoundBinary, Type: types.LogicalType{ID: types.Double}, Op: "%", Children: []*binder.BoundExpression{
		{Kind: binder.BoundLiteral, Type: types.LogicalType{ID: types.Double}, Literal: "5.5"},
		{Kind: binder.BoundLiteral, Type: types.LogicalType{ID: types.Double}, Literal: "2"},
	}}
	if out, err := executor.ExecuteSQL(expr, chunk); err == nil {
		if out != nil {
			out.Release()
		}
		t.Fatal("expected DOUBLE modulo to remain explicit unsupported")
	}
}
