package planner

import (
	"context"
	"strings"
	"testing"

	"github.com/steamwo/fssql/internal/core/stats"
	"github.com/steamwo/fssql/internal/core/types"
	"github.com/steamwo/fssql/internal/function"
	"github.com/steamwo/fssql/internal/sql/binder"
)

func limitPlannerQuery() *binder.BoundQuery {
	column := binder.RelationColumn{Name: "id", Type: types.LogicalType{ID: types.BigInt}}
	expr := &binder.BoundExpression{Kind: binder.BoundColumn, Type: column.Type, ColumnIndex: 0}
	return &binder.BoundQuery{
		Source: &binder.BoundRelation{
			Kind: binder.RelationTableFunction, ID: "tablefunc:read_csv:fp", Columns: []binder.RelationColumn{column},
			FunctionID: function.ReadCSVID, Source: "mem://rows.csv", Fingerprint: stats.DatasetFingerprint("fp"),
		},
		Projection: []binder.BoundSelectItem{{Expression: expr, Output: column}}, Output: []binder.RelationColumn{column},
	}
}

// TestPlanWithLimitPreservesLimitZero 验证 LIMIT 0 不再与“无 LIMIT”混淆。
func TestPlanWithLimitPreservesLimitZero(t *testing.T) {
	query := limitPlannerQuery()
	query.Limit = &binder.BoundExpression{Kind: binder.BoundLiteral, Type: types.LogicalType{ID: types.Integer}, Literal: "0"}
	plan, err := NewQueryPlanner(nil, nil).PlanWithLimit(context.Background(), query)
	if err != nil {
		t.Fatal(err)
	}
	if plan.Kind != LogicalLimit || plan.Limit != 0 || plan.Offset != 0 || len(plan.Children) != 1 {
		t.Fatalf("plan=%+v", plan)
	}
}

// TestPlanWithLimitOffsetOnlyUsesUnlimitedSentinel 验证 OFFSET-only 与 LIMIT 0 使用不同内部表示。
func TestPlanWithLimitOffsetOnlyUsesUnlimitedSentinel(t *testing.T) {
	query := limitPlannerQuery()
	query.Offset = &binder.BoundExpression{Kind: binder.BoundLiteral, Type: types.LogicalType{ID: types.Integer}, Literal: "3"}
	plan, err := NewQueryPlanner(nil, nil).PlanWithLimit(context.Background(), query)
	if err != nil {
		t.Fatal(err)
	}
	if plan.Kind != LogicalLimit || plan.Limit != UnlimitedRows || plan.Offset != 3 {
		t.Fatalf("plan=%+v", plan)
	}
}

// TestFoldRowCountExpressionEvaluatesTypedIntegerConstants 覆盖 local constant arithmetic 与逐节点 typed overflow 规则。
func TestFoldRowCountExpressionEvaluatesTypedIntegerConstants(t *testing.T) {
	integer := func(value string) *binder.BoundExpression {
		return &binder.BoundExpression{Kind: binder.BoundLiteral, Type: types.LogicalType{ID: types.Integer}, Literal: value}
	}
	add := &binder.BoundExpression{Kind: binder.BoundBinary, Type: types.LogicalType{ID: types.Integer}, Op: "+", Children: []*binder.BoundExpression{integer("2"), integer("3")}}
	mul := &binder.BoundExpression{Kind: binder.BoundBinary, Type: types.LogicalType{ID: types.Integer}, Op: "*", Children: []*binder.BoundExpression{add, integer("4")}}
	got, err := foldRowCountExpression(mul, "LIMIT")
	if err != nil || got != 20 {
		t.Fatalf("got=%d err=%v", got, err)
	}

	negative := &binder.BoundExpression{Kind: binder.BoundUnary, Type: types.LogicalType{ID: types.Integer}, Op: "-", Children: []*binder.BoundExpression{integer("1")}}
	if _, err := foldRowCountExpression(negative, "LIMIT"); err == nil || !strings.Contains(err.Error(), "cannot be negative") {
		t.Fatalf("negative err=%v", err)
	}
	overflow := &binder.BoundExpression{Kind: binder.BoundLiteral, Type: types.LogicalType{ID: types.Integer}, Literal: "2147483648"}
	if _, err := foldRowCountExpression(overflow, "LIMIT"); err == nil || !strings.Contains(err.Error(), "overflow") {
		t.Fatalf("overflow err=%v", err)
	}
}

// TestPlanWithLimitRejectsHugeintBeyondUint64 验证 Planner 不截断 DuckDB LIMIT/OFFSET 的 UINT64 conversion domain。
func TestPlanWithLimitRejectsHugeintBeyondUint64(t *testing.T) {
	query := limitPlannerQuery()
	query.Limit = &binder.BoundExpression{Kind: binder.BoundLiteral, Type: types.LogicalType{ID: types.HugeInt}, Literal: "18446744073709551616"}
	_, err := NewQueryPlanner(nil, nil).PlanWithLimit(context.Background(), query)
	if err == nil || !strings.Contains(err.Error(), "UINT64") {
		t.Fatalf("err=%v", err)
	}
}
