package expression

import (
	"testing"

	"github.com/steamwo/fssql/internal/core/memory"
	"github.com/steamwo/fssql/internal/core/types"
	"github.com/steamwo/fssql/internal/core/vector"
	"github.com/steamwo/fssql/internal/function"
	"github.com/steamwo/fssql/internal/sql/binder"
)

func stringBatchInput(t *testing.T, scope *memory.Scope, buffers *memory.BufferManager, values []string, layout string) (*vector.Vector, []*vector.Vector) {
	t.Helper()
	typ := types.LogicalType{ID: types.Varchar}
	if layout == "constant" {
		v, err := vector.NewConstant(scope, buffers, typ, len(values))
		if err != nil {
			t.Fatal(err)
		}
		if err := v.SetString(0, values[0]); err != nil {
			t.Fatal(err)
		}
		return v, nil
	}
	base, err := vector.NewFlat(scope, buffers, typ, len(values))
	if err != nil {
		t.Fatal(err)
	}
	for row, value := range values {
		if err := base.SetString(row, value); err != nil {
			t.Fatal(err)
		}
	}
	base.SetNull(len(values) - 1)
	if layout == "flat" {
		return base, nil
	}
	selection := make([]uint32, len(values))
	for row := range selection {
		selection[row] = uint32(len(values) - 1 - row)
	}
	inner, err := vector.NewDictionary(base, vector.SelectionFrom(selection))
	if err != nil {
		t.Fatal(err)
	}
	for row := range selection {
		selection[row] = uint32((row + 1) % len(values))
	}
	outer, err := vector.NewDictionary(inner, vector.SelectionFrom(selection))
	if err != nil {
		t.Fatal(err)
	}
	return outer, []*vector.Vector{inner, base}
}

func stringBatchExpression(id function.ID, binary bool) *binder.BoundExpression {
	text := types.LogicalType{ID: types.Varchar}
	result := types.LogicalType{ID: types.BigInt}
	if id == function.ConcatID {
		result = text
	}
	expr := &binder.BoundExpression{Kind: binder.BoundFunction, Type: result, FunctionID: id, FunctionKind: function.Scalar, Children: []*binder.BoundExpression{{Kind: binder.BoundColumn, Type: text, ColumnIndex: 0}}}
	if binary {
		expr.Children = append(expr.Children, &binder.BoundExpression{Kind: binder.BoundColumn, Type: text, ColumnIndex: 1})
	}
	return expr
}

func TestStringBatchMatchesScalarSemanticsAndOwnsResults(t *testing.T) {
	for _, tc := range []struct {
		name   string
		id     function.ID
		binary bool
	}{{"length", function.LengthID, false}, {"strpos", function.StrposID, true}, {"concat", function.ConcatID, true}} {
		for _, layout := range []string{"flat", "constant", "nested_dictionary"} {
			t.Run(tc.name+"/"+layout, func(t *testing.T) {
				manager := memory.NewManager(1 << 20)
				scope := manager.NewScope()
				buffers := memory.NewBufferManager(nil)
				left, leftChildren := stringBatchInput(t, scope, buffers, []string{"aβ😊-a long UTF-8 input", "", string([]byte{254, 'x'}), "a\uFFFDx", "abcdef", "NULL"}, layout)
				right, rightChildren := stringBatchInput(t, scope, buffers, []string{"β", "", string([]byte{255}), string([]byte{255}), "zzz", "NULL"}, layout)
				if layout == "flat" {
					right.SetNull(4)
				}
				if layout == "nested_dictionary" {
					rightChildren[1].SetNull(4)
				}
				chunk, err := vector.NewDataChunk(left, right)
				if err != nil {
					t.Fatal(err)
				}
				expr := stringBatchExpression(tc.id, tc.binary)
				want := make([]ScalarValue, chunk.Cardinality)
				for row := range want {
					args := make([]ScalarValue, len(expr.Children))
					for column := range args {
						args[column], err = scalarFromVector(chunk.Columns[column], row)
						if err != nil {
							t.Fatal(err)
						}
					}
					want[row], err = evalBuiltinScalar(tc.id, expr.Type, args)
					if err != nil {
						t.Fatal(err)
					}
				}
				out, err := NewExecutor(scope, buffers).ExecuteSQL(expr, chunk)
				if err != nil {
					t.Fatal(err)
				}
				chunk.Release()
				for _, child := range append(leftChildren, rightChildren...) {
					child.Release()
				}
				for row, expected := range want {
					if out.IsValid(row) != expected.Valid {
						t.Fatalf("row=%d validity mismatch", row)
					}
					if !expected.Valid {
						continue
					}
					if tc.id == function.ConcatID {
						value, _ := out.Bytes(row)
						if string(value) != string(expected.Bytes) {
							t.Fatalf("row=%d got=%q want=%q", row, value, expected.Bytes)
						}
					} else {
						value, _ := out.Int64(row)
						if value != expected.Int64 {
							t.Fatalf("row=%d got=%d want=%d", row, value, expected.Int64)
						}
					}
				}
				out.Release()
				if scope.Used() != 0 {
					t.Fatalf("managed leak=%d", scope.Used())
				}
			})
		}
	}
}

func TestStringBatchPreservesRegisteredOverrideAndArgumentOwnership(t *testing.T) {
	manager := memory.NewManager(1 << 20)
	scope := manager.NewScope()
	buffers := memory.NewBufferManager(nil)
	input, children := stringBatchInput(t, scope, buffers, []string{"input-one", "input-two", "NULL"}, "flat")
	chunk, err := vector.NewDataChunk(input)
	if err != nil {
		t.Fatal(err)
	}
	executor := NewExecutor(scope, buffers)
	var retained [][]ScalarValue
	executor.Scalars = ScalarRegistry{function.LengthID: {NullHandling: ScalarNullPropagate, Eval: func(args []ScalarValue) (ScalarValue, error) {
		retained = append(retained, args)
		args[0].Bytes[0] = 'X'
		return ScalarValue{Type: types.LogicalType{ID: types.BigInt}, Valid: true, Int64: 99}, nil
	}}}
	out, err := executor.ExecuteSQL(stringBatchExpression(function.LengthID, false), chunk)
	if err != nil {
		t.Fatal(err)
	}
	if len(retained) != 2 {
		t.Fatalf("override calls=%d", len(retained))
	}
	for row, want := range []string{"input-one", "input-two"} {
		value, _ := input.Bytes(row)
		if string(value) != want {
			t.Fatalf("UDF mutated input row=%d", row)
		}
		value64, valid := out.Int64(row)
		if !valid || value64 != 99 {
			t.Fatal("override bypassed")
		}
	}
	retained[0][0].Bytes[1] = 'Z'
	if retained[1][0].Bytes[1] != 'n' {
		t.Fatal("UDF arguments share storage between rows")
	}
	if out.IsValid(2) {
		t.Fatal("NULL propagation changed")
	}
	out.Release()
	chunk.Release()
	for _, child := range children {
		child.Release()
	}
	if scope.Used() != 0 {
		t.Fatalf("managed leak=%d", scope.Used())
	}
}

func TestBuiltinStringAllocationsDoNotScalePerRow(t *testing.T) {
	for _, id := range []function.ID{function.LengthID, function.StrposID, function.ConcatID} {
		measure := func(rows int) float64 {
			manager := memory.NewManager(8 << 20)
			scope := manager.NewScope()
			buffers := memory.NewBufferManager(nil)
			left, err := vector.NewFlat(scope, buffers, types.LogicalType{ID: types.Varchar}, rows)
			if err != nil {
				t.Fatal(err)
			}
			for row := 0; row < rows; row++ {
				if err := left.SetString(row, "prefix-β-middle-😊-a string longer than inline storage"); err != nil {
					t.Fatal(err)
				}
			}
			right, err := vector.NewConstant(scope, buffers, left.Type, rows)
			if err != nil {
				t.Fatal(err)
			}
			if err := right.SetString(0, "β"); err != nil {
				t.Fatal(err)
			}
			chunk, err := vector.NewDataChunk(left, right)
			if err != nil {
				t.Fatal(err)
			}
			executor := NewExecutor(scope, buffers)
			expr := stringBatchExpression(id, id != function.LengthID)
			allocations := testing.AllocsPerRun(10, func() {
				out, err := executor.ExecuteSQL(expr, chunk)
				if err != nil {
					panic(err)
				}
				out.Release()
			})
			chunk.Release()
			if scope.Used() != 0 {
				t.Fatalf("managed leak=%d", scope.Used())
			}
			return allocations
		}
		small, large := measure(64), measure(2048)
		if large > small+20 {
			t.Fatalf("function=%d allocations small=%.1f large=%.1f", id, small, large)
		}
	}
}
