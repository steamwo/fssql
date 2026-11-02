package expression

import (
	"testing"

	"github.com/steamwo/fssql/internal/core/types"
	"github.com/steamwo/fssql/internal/sql/binder"
)

// TestExecuteSQLBooleanFallbackFromIntegerComparison exercises the complete
// ExecuteSQL fallback path used by WHERE predicates such as `3 > 2 AND TRUE`.
func TestExecuteSQLBooleanFallbackFromIntegerComparison(t *testing.T) {
	executor, chunk, cleanup := sqlExecutorFixture(t, 1)
	defer cleanup()
	integer := func(value string) *binder.BoundExpression {
		return &binder.BoundExpression{Kind: binder.BoundLiteral, Type: types.LogicalType{ID: types.Integer}, Literal: value}
	}
	comparison := &binder.BoundExpression{
		Kind: binder.BoundBinary, Type: types.LogicalType{ID: types.Boolean}, Op: ">",
		Children: []*binder.BoundExpression{integer("3"), integer("2")},
	}
	truth := &binder.BoundExpression{Kind: binder.BoundLiteral, Type: types.LogicalType{ID: types.Boolean}, Literal: "true"}
	predicate := &binder.BoundExpression{
		Kind: binder.BoundBinary, Type: types.LogicalType{ID: types.Boolean}, Op: "AND",
		Children: []*binder.BoundExpression{comparison, truth},
	}
	out, err := executor.ExecuteSQL(predicate, chunk)
	if err != nil {
		t.Fatal(err)
	}
	defer out.Release()
	value, valid := out.Bool(0)
	if !valid || !value {
		t.Fatalf("predicate value=%v valid=%v", value, valid)
	}
}

// TestExecuteSQLIntegerToBigIntWidening covers Binder-inserted widening used
// when INTEGER literals are compared with BIGINT aggregate outputs.
func TestExecuteSQLIntegerToBigIntWidening(t *testing.T) {
	executor, chunk, cleanup := sqlExecutorFixture(t, 2)
	defer cleanup()
	expr := &binder.BoundExpression{
		Kind: binder.BoundCast, Type: types.LogicalType{ID: types.BigInt},
		Children: []*binder.BoundExpression{{Kind: binder.BoundLiteral, Type: types.LogicalType{ID: types.Integer}, Literal: "1"}},
	}
	out, err := executor.ExecuteSQL(expr, chunk)
	if err != nil {
		t.Fatal(err)
	}
	defer out.Release()
	for row := 0; row < 2; row++ {
		value, valid := out.Int64(row)
		if !valid || value != 1 {
			t.Fatalf("row=%d value=%d valid=%v", row, value, valid)
		}
	}
}
