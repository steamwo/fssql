package planner

import (
	"testing"

	"github.com/steamwo/fssql/internal/core/types"
	"github.com/steamwo/fssql/internal/sql/binder"
)

func joinKeyColumn(index int) *binder.BoundExpression {
	return &binder.BoundExpression{Kind: binder.BoundColumn, Type: types.LogicalType{ID: types.BigInt}, ColumnIndex: index}
}

func joinKeyBinary(op string, left, right *binder.BoundExpression) *binder.BoundExpression {
	return &binder.BoundExpression{Kind: binder.BoundBinary, Type: types.LogicalType{ID: types.Boolean}, Op: op, Children: []*binder.BoundExpression{left, right}}
}

// TestExtractHashJoinKeysRemapsRightColumns 验证 combined Scope 的 right index 被重映射成 right child 自身序号。
func TestExtractHashJoinKeysRemapsRightColumns(t *testing.T) {
	predicate := joinKeyBinary("AND",
		joinKeyBinary("=", joinKeyColumn(0), joinKeyColumn(3)),
		joinKeyBinary("=", joinKeyColumn(4), joinKeyColumn(1)),
	)
	keys, err := ExtractHashJoinKeys(predicate, 2, 3)
	if err != nil {
		t.Fatal(err)
	}
	if len(keys.Left) != 2 || keys.Left[0] != 0 || keys.Right[0] != 1 || keys.Left[1] != 1 || keys.Right[1] != 2 {
		t.Fatalf("keys=%+v", keys)
	}
}

// TestExtractHashJoinKeysRejectsResidualAndSameSide 验证不能把 residual/non-equi/same-side 条件错误当成 hash key。
func TestExtractHashJoinKeysRejectsResidualAndSameSide(t *testing.T) {
	cases := []*binder.BoundExpression{
		joinKeyBinary("<", joinKeyColumn(0), joinKeyColumn(2)),
		joinKeyBinary("=", joinKeyColumn(0), joinKeyColumn(1)),
		joinKeyBinary("AND", joinKeyBinary("=", joinKeyColumn(0), joinKeyColumn(2)), joinKeyBinary(">", joinKeyColumn(1), joinKeyColumn(3))),
		joinKeyBinary("=", &binder.BoundExpression{Kind: binder.BoundBinary, Type: types.LogicalType{ID: types.BigInt}, Op: "+", Children: []*binder.BoundExpression{joinKeyColumn(0), joinKeyColumn(0)}}, joinKeyColumn(2)),
	}
	for i, predicate := range cases {
		if _, err := ExtractHashJoinKeys(predicate, 2, 2); err == nil {
			t.Fatalf("case %d should fail", i)
		}
	}
}

// TestExtractHashJoinKeysRejectsOutOfRangeColumn 验证损坏的 Binder column index 不会进入 execution。
func TestExtractHashJoinKeysRejectsOutOfRangeColumn(t *testing.T) {
	if _, err := ExtractHashJoinKeys(joinKeyBinary("=", joinKeyColumn(0), joinKeyColumn(99)), 1, 1); err == nil {
		t.Fatal("expected out-of-range key rejection")
	}
}

func TestDomainHashJoinNullSafeKeys(t *testing.T) {
	a := domainNullSafeEqual(joinKeyColumn(0), joinKeyColumn(2))
	b := domainNullSafeEqual(joinKeyColumn(1), joinKeyColumn(3))
	keys, err := ExtractHashJoinKeys(phase5And(a, b), 2, 2)
	if err != nil || !keys.NullsEqual || len(keys.Left) != 2 {
		t.Fatalf("keys=%+v err=%v", keys, err)
	}
	if _, err := ExtractHashJoinKeys(phase5And(a, joinKeyBinary("=", joinKeyColumn(1), joinKeyColumn(3))), 2, 2); err == nil {
		t.Fatal("mixed equality modes must fail closed")
	}
	// IS NOT NULL in an otherwise similar OR expression is not key equality.
	b.Children[1].Children[0].Op = "IS NOT NULL"
	if _, err := ExtractHashJoinKeys(b, 2, 2); err == nil {
		t.Fatal("non-equivalent OR must not become a hash key")
	}
}
