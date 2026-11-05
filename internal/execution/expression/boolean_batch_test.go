package expression

import (
	"testing"

	"github.com/steamwo/fssql/internal/core/memory"
	"github.com/steamwo/fssql/internal/core/types"
	"github.com/steamwo/fssql/internal/core/vector"
	"github.com/steamwo/fssql/internal/sql/binder"
)

func TestBooleanBatchThreeValuedLogicAcrossRepresentations(t *testing.T) {
	and := []int{0, 0, 0, 0, 1, 2, 0, 2, 2}
	or := []int{0, 1, 2, 1, 1, 1, 2, 1, 2}
	for _, leftLayout := range []string{"flat", "dictionary", "constant_false", "constant_true", "constant_null"} {
		for _, rightLayout := range []string{"flat", "dictionary", "constant_false", "constant_true", "constant_null"} {
			manager := memory.NewManager(1 << 20)
			scope := manager.NewScope()
			buffers := memory.NewBufferManager(nil)
			typ := types.LogicalType{ID: types.Boolean}
			makeInput := func(layout string, left bool) (*vector.Vector, *vector.Vector) {
				if layout == "constant_false" || layout == "constant_true" || layout == "constant_null" {
					v, err := vector.NewConstant(scope, buffers, typ, 9)
					if err != nil {
						t.Fatal(err)
					}
					v.SetBool(0, layout == "constant_true")
					if layout == "constant_null" {
						v.SetNull(0)
					}
					return v, nil
				}
				base, err := vector.NewFlat(scope, buffers, typ, 9)
				if err != nil {
					t.Fatal(err)
				}
				for row := 0; row < 9; row++ {
					state := row % 3
					if left {
						state = row / 3
					}
					base.SetBool(row, state == 1)
					if state == 2 {
						base.SetNull(row)
					}
				}
				if layout == "flat" {
					return base, nil
				}
				v, err := vector.NewDictionary(base, vector.SelectionFrom([]uint32{8, 7, 6, 5, 4, 3, 2, 1, 0}))
				if err != nil {
					t.Fatal(err)
				}
				return v, base
			}
			left, leftBase := makeInput(leftLayout, true)
			right, rightBase := makeInput(rightLayout, false)
			chunk, err := vector.NewDataChunk(left, right)
			if err != nil {
				t.Fatal(err)
			}
			executor := NewExecutor(scope, buffers)
			for _, tc := range []struct {
				op    string
				kind  Kind
				truth []int
			}{{"AND", And, and}, {"OR", Or, or}} {
				for _, sqlFallback := range []bool{false, true} {
					var out *vector.Vector
					if sqlFallback {
						expr := &binder.BoundExpression{Kind: binder.BoundBinary, Type: typ, Op: tc.op, Children: []*binder.BoundExpression{{Kind: binder.BoundColumn, Type: typ, ColumnIndex: 0}, {Kind: binder.BoundColumn, Type: typ, ColumnIndex: 1}}}
						out, err = executor.executeSQLBooleanBinary(expr, chunk)
					} else {
						expr := &BoundExpression{Kind: tc.kind, Type: typ, Children: []*BoundExpression{{Kind: Column, Type: typ, ColumnIndex: 0}, {Kind: Column, Type: typ, ColumnIndex: 1}}}
						out, err = executor.Execute(expr, chunk)
					}
					if err != nil {
						t.Fatal(err)
					}
					for row := 0; row < 9; row++ {
						state := func(v *vector.Vector) int {
							value, valid := v.Bool(row)
							if !valid {
								return 2
							}
							if value {
								return 1
							}
							return 0
						}
						want := tc.truth[state(left)*3+state(right)]
						value, valid := out.Bool(row)
						if valid != (want != 2) || valid && value != (want == 1) {
							t.Fatalf("%s %s/%s fallback=%v row=%d value=%v/%v want=%d", tc.op, leftLayout, rightLayout, sqlFallback, row, value, valid, want)
						}
					}
					out.Release()
				}
			}
			chunk.Release()
			if leftBase != nil {
				leftBase.Release()
			}
			if rightBase != nil {
				rightBase.Release()
			}
			if scope.Used() != 0 {
				t.Fatalf("managed leak=%d", scope.Used())
			}
		}
	}
}
