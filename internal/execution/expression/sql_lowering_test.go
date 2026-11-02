package expression

import (
	"strings"
	"testing"

	"github.com/steamwo/fssql/internal/core/types"
	"github.com/steamwo/fssql/internal/function"
	"github.com/steamwo/fssql/internal/sql/binder"
)

// TestLowerSQLExpressionColumnAndBoolean 验证无损 column/BOOLEAN lowering。
func TestLowerSQLExpressionColumnAndBoolean(t *testing.T) {
	column, err := LowerSQLExpression(&binder.BoundExpression{Kind: binder.BoundColumn, Type: types.LogicalType{ID: types.Varchar}, ColumnIndex: 2})
	if err != nil {
		t.Fatal(err)
	}
	if column.Kind != Column || column.ColumnIndex != 2 || column.Type.ID != types.Varchar {
		t.Fatalf("column=%+v", column)
	}
	literal, err := LowerSQLExpression(&binder.BoundExpression{Kind: binder.BoundLiteral, Type: types.LogicalType{ID: types.Boolean}, Literal: "true"})
	if err != nil {
		t.Fatal(err)
	}
	if literal.Kind != ConstantBool || !literal.BoolValue {
		t.Fatalf("literal=%+v", literal)
	}
}

// TestLowerSQLExpressionBigIntTree 验证现有 BIGINT arithmetic/comparison kernel 的结构化 lowering。
func TestLowerSQLExpressionBigIntTree(t *testing.T) {
	left := &binder.BoundExpression{Kind: binder.BoundColumn, Type: types.LogicalType{ID: types.BigInt}, ColumnIndex: 0}
	right := &binder.BoundExpression{Kind: binder.BoundLiteral, Type: types.LogicalType{ID: types.BigInt}, Literal: "5"}
	add := &binder.BoundExpression{Kind: binder.BoundBinary, Type: types.LogicalType{ID: types.BigInt}, Op: "+", Children: []*binder.BoundExpression{left, right}}
	cmp := &binder.BoundExpression{Kind: binder.BoundBinary, Type: types.LogicalType{ID: types.Boolean}, Op: "<", Children: []*binder.BoundExpression{add, &binder.BoundExpression{Kind: binder.BoundLiteral, Type: types.LogicalType{ID: types.BigInt}, Literal: "10"}}}
	got, err := LowerSQLExpression(cmp)
	if err != nil {
		t.Fatal(err)
	}
	if got.Kind != LessThan || len(got.Children) != 2 || got.Children[0].Kind != Add || got.Children[0].Children[1].Kind != ConstantInt {
		t.Fatalf("lowered=%+v", got)
	}
}

// TestLowerSQLExpressionIsNotNull 验证 IS NOT NULL 不需要第二套 kernel，而是精确组合 NOT(IS NULL)。
func TestLowerSQLExpressionIsNotNull(t *testing.T) {
	sql := &binder.BoundExpression{
		Kind: binder.BoundIsNull,
		Type: types.LogicalType{ID: types.Boolean},
		Op:   "IS NOT NULL",
		Children: []*binder.BoundExpression{{
			Kind: binder.BoundColumn, Type: types.LogicalType{ID: types.Varchar}, ColumnIndex: 0,
		}},
	}
	got, err := LowerSQLExpression(sql)
	if err != nil {
		t.Fatal(err)
	}
	if got.Kind != Not || len(got.Children) != 1 || got.Children[0].Kind != IsNull {
		t.Fatalf("lowered=%+v", got)
	}
}

// TestLowerSQLExpressionCastBoundary 验证只开放当前执行器已有的 BIGINT↔DOUBLE cast。
func TestLowerSQLExpressionCastBoundary(t *testing.T) {
	good := &binder.BoundExpression{
		Kind: binder.BoundCast, Type: types.LogicalType{ID: types.Double},
		Children: []*binder.BoundExpression{{Kind: binder.BoundColumn, Type: types.LogicalType{ID: types.BigInt}, ColumnIndex: 0}},
	}
	if _, err := LowerSQLExpression(good); err != nil {
		t.Fatal(err)
	}
	bad := &binder.BoundExpression{
		Kind: binder.BoundCast, Type: types.LogicalType{ID: types.BigInt},
		Children: []*binder.BoundExpression{{Kind: binder.BoundColumn, Type: types.LogicalType{ID: types.Integer}, ColumnIndex: 0}},
	}
	if _, err := LowerSQLExpression(bad); err == nil {
		t.Fatal("expected unsupported cast")
	}
}

// TestLowerSQLExpressionFailsClosedOnMissingStableKernels 记录仍不属于 legacy lowering 的执行边界，防止 unsupported SQL 被偷偷降级。
func TestLowerSQLExpressionFailsClosedOnMissingStableKernels(t *testing.T) {
	cases := []*binder.BoundExpression{
		{Kind: binder.BoundLiteral, Type: types.LogicalType{ID: types.Integer}, Literal: "1"},
		{Kind: binder.BoundLiteral, Type: types.LogicalType{ID: types.Varchar}, Literal: "x"},
		{Kind: binder.BoundBinary, Type: types.LogicalType{ID: types.Double}, Op: "/", Children: []*binder.BoundExpression{{Kind: binder.BoundColumn, Type: types.LogicalType{ID: types.Double}, ColumnIndex: 0}, {Kind: binder.BoundColumn, Type: types.LogicalType{ID: types.Double}, ColumnIndex: 1}}},
		{Kind: binder.BoundFunction, FunctionID: function.SumID, FunctionKind: function.Aggregate, Type: types.LogicalType{ID: types.HugeInt}},
		{Kind: binder.BoundSubquery, Type: types.LogicalType{ID: types.BigInt}},
	}
	for i, expr := range cases {
		if _, err := LowerSQLExpression(expr); err == nil {
			t.Fatalf("case %d expected explicit failure", i)
		}
	}
}

// TestLowerSQLExpressionDeepCopiesType 验证 lowering 不与 Binder IR 共享 nested type metadata。
func TestLowerSQLExpressionDeepCopiesType(t *testing.T) {
	child := types.LogicalType{ID: types.BigInt}
	list := types.LogicalType{ID: types.List, Child: &child}
	sql := &binder.BoundExpression{Kind: binder.BoundColumn, Type: list, ColumnIndex: 0}
	got, err := LowerSQLExpression(sql)
	if err != nil {
		t.Fatal(err)
	}
	got.Type.Child.ID = types.Varchar
	if sql.Type.Child.ID != types.BigInt {
		t.Fatalf("nested type alias leaked: %v", sql.Type.Child.ID)
	}
}

// TestLowerSQLExpressionDiagnosticIsSpecific 防止 unsupported kernel 被包装成模糊 generic error。
func TestLowerSQLExpressionDiagnosticIsSpecific(t *testing.T) {
	_, err := LowerSQLExpression(&binder.BoundExpression{Kind: binder.BoundUnary, Type: types.LogicalType{ID: types.Integer}, Op: "-", Children: []*binder.BoundExpression{{Kind: binder.BoundColumn, Type: types.LogicalType{ID: types.Integer}, ColumnIndex: 0}}})
	if err == nil || !strings.Contains(err.Error(), "requires BIGINT kernel") {
		t.Fatalf("err=%v", err)
	}
}
