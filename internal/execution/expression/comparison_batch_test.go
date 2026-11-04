package expression

import (
	"math"
	"reflect"
	"testing"

	"github.com/steamwo/fssql/internal/core/memory"
	"github.com/steamwo/fssql/internal/core/types"
	"github.com/steamwo/fssql/internal/core/vector"
	"github.com/steamwo/fssql/internal/sql/binder"
)

func TestBatchComparisonSelectionMatchesBooleanOutput(t *testing.T) {
	for _, typ := range []types.LogicalType{{ID: types.Double}, types.BlobType(), types.UIntegerType(), types.TimestampNSType()} {
		m := memory.NewManager(1 << 20)
		scope := m.NewScope()
		bm := memory.NewBufferManager(nil)
		e := NewExecutor(scope, bm)
		left, _ := vector.NewFlat(scope, bm, typ, 4)
		right, _ := vector.NewFlat(scope, bm, typ, 4)
		for row := 0; row < 4; row++ {
			switch typ.ID {
			case types.Double:
				left.SetFloat64(row, []float64{math.NaN(), math.Copysign(0, -1), -3, 7}[row])
				right.SetFloat64(row, []float64{math.NaN(), 0, 2, 7}[row])
			case types.Varchar:
				_ = left.SetStringBytes(row, []byte{byte(row), 0, 255})
				_ = right.SetStringBytes(row, []byte{byte(3 - row), 0, 255})
			case types.Integer:
				left.SetUint32(row, math.MaxUint32-uint32(row))
				right.SetUint32(row, math.MaxUint32-uint32(3-row))
			case types.TimestampType:
				left.SetInt64(row, 1730000000123456789+int64(row))
				right.SetInt64(row, 1730000000123456792-int64(row))
			}
		}
		left.SetNull(3)
		child, _ := vector.NewDictionary(left, vector.SelectionFrom([]uint32{2, 1, 0, 3}))
		outer, _ := vector.NewDictionary(child, vector.SelectionFrom([]uint32{2, 1, 0, 3}))
		chunk, _ := vector.NewDataChunk(outer, right)
		column := func(i int) *binder.BoundExpression {
			return &binder.BoundExpression{Kind: binder.BoundColumn, ColumnIndex: i, Type: typ}
		}
		for _, op := range []string{"=", "!=", "<>", "<", "<=", ">", ">="} {
			expr := &binder.BoundExpression{Kind: binder.BoundBinary, Type: types.LogicalType{ID: types.Boolean}, Op: op, Children: []*binder.BoundExpression{column(0), column(1)}}
			out, err := e.ExecuteSQL(expr, chunk)
			if err != nil {
				t.Fatal(err)
			}
			selected, handled, err := e.SelectSQL(expr, chunk, nil)
			if err != nil || !handled {
				t.Fatalf("handled=%v err=%v", handled, err)
			}
			var want []int
			for row := 0; row < 4; row++ {
				value, valid := out.Bool(row)
				if value && valid {
					want = append(want, row)
				}
			}
			if !reflect.DeepEqual(want, selected) {
				t.Fatalf("type=%v op=%s want=%v got=%v", typ, op, want, selected)
			}
			if out.IsValid(3) {
				t.Fatal("NULL comparison became valid")
			}
			out.Release()
		}
		chunk.Release()
		child.Release()
		left.Release()
		if err := scope.Close(); err != nil {
			t.Fatal(err)
		}
	}
}

func BenchmarkBatchPredicateSelection(b *testing.B) {
	m := memory.NewManager(1 << 20)
	scope := m.NewScope()
	bm := memory.NewBufferManager(nil)
	e := NewExecutor(scope, bm)
	v, _ := vector.NewFlat(scope, bm, types.LogicalType{ID: types.BigInt}, 2048)
	for i := 0; i < 2048; i++ {
		v.SetInt64(i, int64(i))
	}
	chunk, _ := vector.NewDataChunk(v)
	defer func() {
		chunk.Release()
		if err := scope.Close(); err != nil {
			b.Fatal(err)
		}
	}()
	expr := &binder.BoundExpression{Kind: binder.BoundBinary, Type: types.LogicalType{ID: types.Boolean}, Op: ">=", Children: []*binder.BoundExpression{{Kind: binder.BoundColumn, Type: v.Type, ColumnIndex: 0}, {Kind: binder.BoundLiteral, Type: v.Type, Literal: "1024"}}}
	for _, selection := range []bool{false, true} {
		b.Run(map[bool]string{false: "boolean_vector", true: "direct_selection"}[selection], func(b *testing.B) {
			rows := make([]int, 0, 2048)
			b.ReportAllocs()
			b.SetBytes(2048 * 8)
			b.ResetTimer()
			for i := 0; i < b.N; i++ {
				if selection {
					selected, handled, err := e.SelectSQL(expr, chunk, rows[:0])
					if err != nil || !handled || len(selected) != 1024 {
						b.Fatalf("selection=%d handled=%v err=%v", len(selected), handled, err)
					}
				} else {
					out, err := e.ExecuteSQL(expr, chunk)
					if err != nil {
						b.Fatal(err)
					}
					out.Release()
				}
			}
		})
	}
}
