package sqltransform

import (
	"context"
	"testing"

	"github.com/steamwo/fssql/internal/core/types"
	"github.com/steamwo/fssql/internal/core/vector"
	"github.com/steamwo/fssql/internal/sql/binder"
)

func TestBatchProjectOwnsDuplicateColumnsAndConstantResult(t *testing.T) {
	m, _, executor, input := sqlTransformFixture(t, 3, 7)
	column := &binder.BoundExpression{Kind: binder.BoundColumn, ColumnIndex: 0, Type: types.LogicalType{ID: types.BigInt}}
	literal := &binder.BoundExpression{Kind: binder.BoundLiteral, Literal: "42", Type: types.LogicalType{ID: types.BigInt}}
	project, err := NewProject(executor, []*binder.BoundExpression{column, column, literal})
	if err != nil {
		t.Fatal(err)
	}
	out, err := project.Transform(context.Background(), input)
	if err != nil {
		t.Fatal(err)
	}
	input.Release()
	out.Columns[0].SetInt64(0, 99)
	if x, valid := out.Columns[1].Int64(0); !valid || x != 3 {
		t.Fatal("duplicate projection aliases storage")
	}
	if x, valid := out.Columns[2].Int64(1); !valid || x != 42 {
		t.Fatal("constant result lost ownership")
	}
	out.Release()
	if m.Used() != 0 {
		t.Fatalf("managed leak=%d", m.Used())
	}
}

func TestSelectionKeepsUnreadColumnsConstantAndIndependent(t *testing.T) {
	m, scope, executor, input := sqlTransformFixture(t, 1, 2, 3)
	null, err := vector.NewConstant(scope, executor.Buffers, types.LogicalType{ID: types.Varchar}, 3)
	if err != nil {
		t.Fatal(err)
	}
	null.SetNull(0)
	input.Columns = append(input.Columns, null)
	out, err := materializeSelection(context.Background(), executor, input, []int{2, 0})
	if err != nil {
		t.Fatal(err)
	}
	input.Release()
	if out.Columns[1].Kind != vector.Constant || out.Columns[1].IsValid(1) {
		t.Fatal("unread NULL column expanded or corrupted")
	}
	if x, valid := out.Columns[0].Int64(0); !valid || x != 3 {
		t.Fatal("selection did not retain independent data")
	}
	out.Release()
	if m.Used() != 0 {
		t.Fatalf("managed leak=%d", m.Used())
	}
}
