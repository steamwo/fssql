package sqltransform

import (
	"context"
	"errors"
	"testing"

	"github.com/steamwo/fssql/internal/core/memory"
	"github.com/steamwo/fssql/internal/core/types"
	"github.com/steamwo/fssql/internal/core/vector"
	"github.com/steamwo/fssql/internal/execution/expression"
	"github.com/steamwo/fssql/internal/sql/binder"
)

func ownedColumn(index int) *binder.BoundExpression {
	return &binder.BoundExpression{Kind: binder.BoundColumn, ColumnIndex: index, Type: types.LogicalType{ID: types.BigInt}}
}
func ownedPredicate() *binder.BoundExpression {
	return &binder.BoundExpression{Kind: binder.BoundBinary, Type: types.LogicalType{ID: types.Boolean}, Op: ">", Children: []*binder.BoundExpression{ownedColumn(0), {Kind: binder.BoundLiteral, Type: types.LogicalType{ID: types.BigInt}, Literal: "1"}}}
}

func TestOwnedProjectMovesColumnsAndCopiesDuplicates(t *testing.T) {
	m, scope, e, in := sqlTransformFixture(t, 1, 2, 3)
	original := in.Columns[0]
	p, _ := NewProject(e, []*binder.BoundExpression{ownedColumn(0), ownedColumn(0)})
	out, err := p.TransformOwned(context.Background(), in)
	if err != nil {
		t.Fatal(err)
	}
	if out.Columns[0] != original || in.Columns[0] != nil {
		t.Fatal("projection did not transfer exclusive input")
	}
	in.Release()
	out.Columns[1].SetInt64(0, 99)
	if x, _ := out.Columns[0].Int64(0); x != 1 {
		t.Fatal("duplicate projection aliases storage")
	}
	out.Release()
	if err := scope.Close(); err != nil {
		t.Fatal(err)
	}
	if m.Used() != 0 {
		t.Fatalf("leak=%d", m.Used())
	}
}

func TestOwnedFilterRetainsNestedDictionaryAndConstant(t *testing.T) {
	m, scope, e, in := sqlTransformFixture(t, 1, 2, 3)
	constant, _ := vector.NewConstant(scope, e.Buffers, types.LogicalType{ID: types.Varchar}, 3)
	_ = constant.SetString(0, "constant payload")
	in.Columns = append(in.Columns, constant)
	f, _ := NewFilter(e, ownedPredicate())
	out, err := f.TransformOwned(context.Background(), in)
	if err != nil {
		t.Fatal(err)
	}
	in.Release()
	if out.Cardinality != 2 || out.Columns[0].Kind != vector.Dictionary || out.Columns[1].Kind != vector.Constant || out.Columns[1].Count() != 2 {
		t.Fatal("selection was flattened or constant expanded")
	}
	second, err := f.TransformOwned(context.Background(), out)
	if err != nil {
		t.Fatal(err)
	}
	// The all-pass fast path returns the same owned chunk.
	if second != out {
		t.Fatal("all-pass filter allocated replacement")
	}
	for row, want := range []int64{2, 3} {
		if x, ok := second.Columns[0].Int64(row); !ok || x != want {
			t.Fatal("dictionary lost values")
		}
	}
	second.Release()
	if err := scope.Close(); err != nil {
		t.Fatal(err)
	}
	if m.Used() != 0 {
		t.Fatalf("leak=%d", m.Used())
	}
}

func TestOwnedTransformsRollbackAllocationFailure(t *testing.T) {
	for _, project := range []bool{false, true} {
		m, scope, e, in := sqlTransformFixture(t, 1, 2, 3)
		other, _ := vector.NewFlat(scope, e.Buffers, types.LogicalType{ID: types.BigInt}, 3)
		in.Columns = append(in.Columns, other)
		remaining := int64(10)
		if project {
			remaining = 0
		}
		hold, err := scope.Reserve(memory.TagMetadata, m.Limit()-m.Used()-remaining)
		if err != nil {
			t.Fatal(err)
		}
		if project {
			p, _ := NewProject(e, []*binder.BoundExpression{ownedColumn(0), ownedColumn(0)})
			_, err = p.TransformOwned(context.Background(), in)
		} else {
			f, _ := NewFilter(e, ownedPredicate())
			_, err = f.TransformOwned(context.Background(), in)
		}
		if !errors.Is(err, memory.ErrLimitExceeded) {
			t.Fatalf("project=%v err=%v", project, err)
		}
		hold.Release()
		if in.Columns[0] == nil {
			t.Fatal("failed transform detached input")
		}
		if x, ok := in.Columns[0].Int64(0); !ok || x != 1 {
			t.Fatal("failed transform released input")
		}
		in.Release()
		if err := scope.Close(); err != nil {
			t.Fatal(err)
		}
	}
}

func BenchmarkOwnedProjectVersusCopy(b *testing.B) {
	for _, owned := range []bool{false, true} {
		b.Run(map[bool]string{false: "copy", true: "move"}[owned], func(b *testing.B) {
			m := memory.NewManager(8 << 20)
			scope := m.NewScope()
			bm := memory.NewBufferManager(nil)
			// Input recreation is shared between modes; payload formatting is absent.
			e := expression.NewExecutor(scope, bm)
			p, _ := NewProject(e, []*binder.BoundExpression{ownedColumn(0)})
			b.ReportAllocs()
			b.ResetTimer()
			for i := 0; i < b.N; i++ {
				v, _ := vector.NewFlat(scope, bm, types.LogicalType{ID: types.BigInt}, 2048)
				in, _ := vector.NewDataChunk(v)
				var out *vector.DataChunk
				var err error
				if owned {
					out, err = p.TransformOwned(context.Background(), in)
				} else {
					out, err = p.Transform(context.Background(), in)
				}
				if err != nil {
					b.Fatal(err)
				}
				in.Release()
				out.Release()
			}
			b.StopTimer()
			if err := scope.Close(); err != nil {
				b.Fatal(err)
			}
		})
	}
}
