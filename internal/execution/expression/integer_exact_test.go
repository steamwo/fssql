package expression

import (
	"fmt"
	"testing"

	"github.com/steamwo/fssql/internal/core/memory"
	"github.com/steamwo/fssql/internal/core/types"
	"github.com/steamwo/fssql/internal/core/vector"
	"github.com/steamwo/fssql/internal/sql/binder"
)

func TestExactIntegerSQLVectorRepresentations(t *testing.T) {
	for _, typ := range []types.LogicalType{{ID: types.TinyInt}, {ID: types.SmallInt}, {ID: types.HugeInt}, types.UTinyIntType(), types.UBigIntType()} {
		for _, mode := range []string{"flat", "constant", "dictionary"} {
			t.Run(fmt.Sprintf("%d_unsigned_%t_%s", typ.ID, typ.Unsigned, mode), func(t *testing.T) {
				manager := memory.NewManager(1 << 20)
				scope := manager.NewScope()
				buffers := memory.NewBufferManager(nil)
				var left *vector.Vector
				var err error
				if mode == "constant" {
					left, err = vector.NewConstant(scope, buffers, typ, 3)
				} else {
					left, err = vector.NewFlat(scope, buffers, typ, 3)
				}
				if err != nil {
					t.Fatal(err)
				}
				value := func(n int64) ScalarValue {
					return ScalarValue{Type: typ, Valid: true, Int64: n, Uint64: uint64(n), Int128: types.Int128FromInt64(n)}
				}
				if err := writeScalarValue(left, 0, typ, value(2)); err != nil {
					t.Fatal(err)
				}
				if mode != "constant" {
					if err := writeScalarValue(left, 1, typ, value(3)); err != nil {
						t.Fatal(err)
					}
					left.SetNull(2)
				}
				var base *vector.Vector
				if mode == "dictionary" {
					base = left
					left, err = vector.NewDictionary(base, vector.SelectionFrom([]uint32{2, 0, 1}))
					if err != nil {
						t.Fatal(err)
					}
				}
				right, err := vector.NewConstant(scope, buffers, typ, 3)
				if err != nil {
					t.Fatal(err)
				}
				if err := writeScalarValue(right, 0, typ, value(4)); err != nil {
					t.Fatal(err)
				}
				chunk, err := vector.NewDataChunk(left, right)
				if err != nil {
					t.Fatal(err)
				}
				expr := &binder.BoundExpression{Kind: binder.BoundBinary, Type: typ, Op: "+", Children: []*binder.BoundExpression{{Kind: binder.BoundColumn, Type: typ, ColumnIndex: 0}, {Kind: binder.BoundColumn, Type: typ, ColumnIndex: 1}}}
				out, err := NewExecutor(scope, buffers).ExecuteSQL(expr, chunk)
				if err != nil {
					t.Fatal(err)
				}
				want := []int64{6, 7, -1}
				if mode == "constant" {
					want = []int64{6, 6, 6}
				}
				if mode == "dictionary" {
					want = []int64{-1, 6, 7}
				}
				for row, expected := range want {
					if expected == -1 {
						if out.IsValid(row) {
							t.Fatalf("row %d must be NULL", row)
						}
						continue
					}
					actual, err := scalarFromVector(out, row)
					if err != nil {
						t.Fatal(err)
					}
					switch {
					case typ.IsUnsignedInteger():
						if actual.Uint64 != uint64(expected) {
							t.Fatalf("row %d: %d", row, actual.Uint64)
						}
					case typ.ID == types.HugeInt:
						if actual.Int128 != types.Int128FromInt64(expected) {
							t.Fatalf("row %d: %v", row, actual.Int128)
						}
					default:
						if actual.Int64 != expected {
							t.Fatalf("row %d: %d", row, actual.Int64)
						}
					}
				}
				out.Release()
				chunk.Release()
				if base != nil {
					base.Release()
				}
				if err := scope.Close(); err != nil {
					t.Fatal(err)
				}
				if manager.Used() != 0 {
					t.Fatalf("managed memory leak: %d", manager.Used())
				}
			})
		}
	}
}
