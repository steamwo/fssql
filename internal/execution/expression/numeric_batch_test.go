package expression

import (
	"math"
	"testing"

	"github.com/steamwo/fssql/internal/core/memory"
	"github.com/steamwo/fssql/internal/core/types"
	"github.com/steamwo/fssql/internal/core/vector"
)

func TestInt64ArithmeticBatchRepresentationsAndOverflow(t *testing.T) {
	for _, kind := range []Kind{Add, Subtract, Multiply} {
		for _, representation := range []string{"flat", "constant", "dictionary"} {
			manager := memory.NewManager(1 << 20)
			scope := manager.NewScope()
			buffers := memory.NewBufferManager(nil)
			typ := types.LogicalType{ID: types.BigInt}
			left, err := vector.NewFlat(scope, buffers, typ, 3)
			if err != nil {
				t.Fatal(err)
			}
			left.SetInt64(0, 4)
			left.SetInt64(1, -2)
			left.SetInt64(2, 7)
			right, err := vector.NewFlat(scope, buffers, typ, 3)
			if err != nil {
				t.Fatal(err)
			}
			right.SetInt64(0, 3)
			right.SetInt64(1, 5)
			right.SetInt64(2, -1)
			var child *vector.Vector
			wantRight := []int64{3, 5, -1}
			if representation == "constant" {
				right.Release()
				right, err = vector.NewConstant(scope, buffers, typ, 3)
				if err != nil {
					t.Fatal(err)
				}
				right.SetInt64(0, 3)
				wantRight = []int64{3, 3, 3}
			} else if representation == "dictionary" {
				child = right
				right, err = vector.NewDictionary(child, vector.SelectionFrom([]uint32{2, 0, 1}))
				if err != nil {
					t.Fatal(err)
				}
				wantRight = []int64{-1, 3, 5}
			}
			chunk, err := vector.NewDataChunk(left, right)
			if err != nil {
				t.Fatal(err)
			}
			executor := NewExecutor(scope, buffers)
			expr := &BoundExpression{Kind: kind, Type: typ, Children: []*BoundExpression{{Kind: Column, Type: typ, ColumnIndex: 0}, {Kind: Column, Type: typ, ColumnIndex: 1}}}
			for _, nullable := range []bool{false, true} {
				if nullable {
					left.SetNull(1)
				}
				out, err := executor.Execute(expr, chunk)
				if err != nil {
					t.Fatal(err)
				}
				for row, a := range []int64{4, -2, 7} {
					value, valid := out.Int64(row)
					if nullable && row == 1 {
						if valid {
							t.Fatal("NULL lost")
						}
						continue
					}
					want := a + wantRight[row]
					if kind == Subtract {
						want = a - wantRight[row]
					}
					if kind == Multiply {
						want = a * wantRight[row]
					}
					if !valid || value != want {
						t.Fatalf("kind=%d repr=%s row=%d got=%d/%v want=%d", kind, representation, row, value, valid, want)
					}
				}
				out.Release()
			}
			left.SetInt64(0, math.MaxInt64)
			if kind == Subtract {
				left.SetInt64(0, math.MinInt64)
			}
			if child != nil {
				child.SetInt64(2, 3)
			} else {
				right.SetInt64(0, 3)
			}
			out, err := executor.Execute(expr, chunk)
			if out != nil {
				out.Release()
			}
			if err == nil {
				t.Fatalf("kind=%d repr=%s overflow accepted", kind, representation)
			}
			chunk.Release()
			if child != nil {
				child.Release()
			}
			if scope.Used() != 0 {
				t.Fatalf("managed leak=%d", scope.Used())
			}
		}
	}
}
