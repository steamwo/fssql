package expression

import (
	"errors"
	"strings"
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

// stringBatchExpression 构造真实 ExecuteSQL 批路径使用的字符串函数表达式。
func stringBatchExpression(id function.ID, binary bool) *binder.BoundExpression {
	text := types.LogicalType{ID: types.Varchar}
	result := types.LogicalType{ID: types.BigInt}
	if id == function.ConcatID || id == function.LowerID {
		result = text
	}
	expr := &binder.BoundExpression{Kind: binder.BoundFunction, Type: result, FunctionID: id, FunctionKind: function.Scalar, Children: []*binder.BoundExpression{{Kind: binder.BoundColumn, Type: text, ColumnIndex: 0}}}
	if binary {
		expr.Children = append(expr.Children, &binder.BoundExpression{Kind: binder.BoundColumn, Type: text, ColumnIndex: 1})
	}
	return expr
}

// TestStringBatchMatchesScalarSemanticsAndOwnsResults 对照标量实现验证字符串批处理语义和结果所有权。
func TestStringBatchMatchesScalarSemanticsAndOwnsResults(t *testing.T) {
	for _, tc := range []struct {
		name   string
		id     function.ID
		binary bool
	}{{"length", function.LengthID, false}, {"strpos", function.StrposID, true}, {"concat", function.ConcatID, true}, {"lower", function.LowerID, false}} {
		for _, layout := range []string{"flat", "constant", "nested_dictionary"} {
			t.Run(tc.name+"/"+layout, func(t *testing.T) {
				manager := memory.NewManager(1 << 20)
				scope := manager.NewScope()
				buffers := memory.NewBufferManager(nil)
				left, leftChildren := stringBatchInput(t, scope, buffers, []string{"aβ😊-İKȺ-a long UTF-8 input", "", string([]byte{254, 'x'}), "a\uFFFDx", "abcdef", "NULL"}, layout)
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
				if !out.Type.Same(expr.Type) {
					t.Fatalf("output type=%v want=%v", out.Type, expr.Type)
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
					if tc.id == function.ConcatID || tc.id == function.LowerID {
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

// TestLowerBatchReleasesOutputOnLimit 验证 LOWER 输出堆增长失败后不保留受管缓冲。
func TestLowerBatchReleasesOutputOnLimit(t *testing.T) {
	const rows = 8
	// 输入占用 704 字节；960 允许输出向量及初始堆分配，但拒绝首次堆增长。
	manager := memory.NewManager(960)
	scope := manager.NewScope()
	buffers := memory.NewBufferManager(nil)
	input, err := vector.NewFlat(scope, buffers, types.LogicalType{ID: types.Varchar}, rows)
	if err != nil {
		t.Fatal(err)
	}
	for row := 0; row < rows; row++ {
		if err := input.SetString(row, "ABCDEFGHIJKLMNOPQRSTUVWXYZ-abcdefghijklmnopqrstuvwxyz"); err != nil {
			t.Fatal(err)
		}
	}
	chunk, err := vector.NewDataChunk(input)
	if err != nil {
		t.Fatal(err)
	}
	inputManaged := manager.Used()
	out, err := NewExecutor(scope, buffers).ExecuteSQL(stringBatchExpression(function.LowerID, false), chunk)
	if out != nil {
		out.Release()
		t.Fatal("LIMIT 错误返回了非空输出")
	}
	if !errors.Is(err, memory.ErrLimitExceeded) {
		t.Fatalf("error=%v want=%v", err, memory.ErrLimitExceeded)
	}
	if used := manager.Used(); used != inputManaged {
		t.Fatalf("输出未释放：managed=%d input=%d", used, inputManaged)
	}
	chunk.Release()
	if managedAfter := manager.Used(); managedAfter != 0 {
		t.Fatalf("managed_after=%d", managedAfter)
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

// TestConcatBatchExactReservationAndBudget 验证仅为非 NULL 外部堆值预留，并检查精确限额边界。
func TestConcatBatchExactReservationAndBudget(t *testing.T) {
	const rows = 8
	leftValues := []string{"a", strings.Repeat("a", 12), strings.Repeat("b", 12), strings.Repeat("c", 12), strings.Repeat("d", 8), strings.Repeat("e", 6), strings.Repeat("f", 12), ""}
	rightValues := []string{"b", strings.Repeat("x", 12), strings.Repeat("y", 12), strings.Repeat("z", 12), strings.Repeat("w", 8), strings.Repeat("v", 7), strings.Repeat("g", 12), ""}
	const heapBytes = int64(24 + 24 + 16 + 13)
	const inputReservation = int64(2 * (rows*24 + 64))
	for _, tc := range []struct {
		name      string
		shortfall int64
		wantErr   bool
	}{{"exact", 0, false}, {"limit", 1, true}} {
		t.Run(tc.name, func(t *testing.T) {
			limit := inputReservation + rows*24 + heapBytes - tc.shortfall
			manager := memory.NewManager(limit)
			scope := manager.NewScope()
			buffers := memory.NewBufferManager(nil)
			typ := types.LogicalType{ID: types.Varchar}
			left, err := vector.NewFlat(scope, buffers, typ, rows)
			if err != nil {
				t.Fatal(err)
			}
			right, err := vector.NewFlat(scope, buffers, typ, rows)
			if err != nil {
				t.Fatal(err)
			}
			for row := 0; row < rows; row++ {
				if err := left.SetString(row, leftValues[row]); err != nil {
					t.Fatal(err)
				}
				if err := right.SetString(row, rightValues[row]); err != nil {
					t.Fatal(err)
				}
			}
			left.SetNull(2)
			right.SetNull(3)
			chunk, err := vector.NewDataChunk(left, right)
			if err != nil {
				t.Fatal(err)
			}
			inputManaged := manager.Used()
			if inputManaged != inputReservation {
				t.Fatalf("input managed=%d want=%d", inputManaged, inputReservation)
			}
			inputHeap := scope.UsedByTag(memory.TagStringHeap)
			out, err := NewExecutor(scope, buffers).ExecuteSQL(stringBatchExpression(function.ConcatID, true), chunk)
			if tc.wantErr {
				if out != nil {
					out.Release()
					t.Fatal("limit error returned an output vector")
				}
				if !errors.Is(err, memory.ErrLimitExceeded) {
					t.Fatalf("error=%v want=%v", err, memory.ErrLimitExceeded)
				}
				if got := manager.Used(); got != inputManaged {
					t.Fatalf("managed_after=%d want input=%d", got, inputManaged)
				}
				chunk.Release()
				if got := manager.Used(); got != 0 {
					t.Fatalf("managed leak=%d", got)
				}
				return
			}
			if err != nil {
				t.Fatal(err)
			}
			for row := 0; row < rows; row++ {
				valid := row != 2 && row != 3
				if out.IsValid(row) != valid {
					t.Fatalf("row=%d validity=%v want=%v", row, out.IsValid(row), valid)
				}
				if valid {
					value, _ := out.Bytes(row)
					if want := leftValues[row] + rightValues[row]; string(value) != want {
						t.Fatalf("row=%d value=%q want=%q", row, value, want)
					}
				}
			}
			if got := scope.UsedByTag(memory.TagStringHeap); got != inputHeap+heapBytes {
				t.Fatalf("string heap=%d want=%d", got, inputHeap+heapBytes)
			}
			if got := manager.Peak(); got != limit {
				t.Fatalf("managed peak=%d want exact limit=%d", got, limit)
			}
			out.Release()
			if got := manager.Used(); got != inputManaged {
				t.Fatalf("managed_after=%d want input=%d", got, inputManaged)
			}
			chunk.Release()
			if got := manager.Used(); got != 0 {
				t.Fatalf("managed leak=%d", got)
			}
		})
	}
}

// TestStringBatchEmpty 验证空批在 CONCAT 与 LOWER 专用路径中无需堆增长。
func TestStringBatchEmpty(t *testing.T) {
	for _, tc := range []struct {
		name   string
		id     function.ID
		binary bool
	}{{"concat", function.ConcatID, true}, {"lower", function.LowerID, false}} {
		t.Run(tc.name, func(t *testing.T) {
			manager := memory.NewManager(1024)
			scope := manager.NewScope()
			buffers := memory.NewBufferManager(nil)
			typ := types.LogicalType{ID: types.Varchar}
			left, err := vector.NewFlat(scope, buffers, typ, 0)
			if err != nil {
				t.Fatal(err)
			}
			right, err := vector.NewFlat(scope, buffers, typ, 0)
			if err != nil {
				t.Fatal(err)
			}
			chunk, err := vector.NewDataChunk(left, right)
			if err != nil {
				t.Fatal(err)
			}
			out, err := NewExecutor(scope, buffers).ExecuteSQL(stringBatchExpression(tc.id, tc.binary), chunk)
			if err != nil {
				t.Fatal(err)
			}
			if out.Count() != 0 {
				t.Fatalf("output rows=%d", out.Count())
			}
			out.Release()
			chunk.Release()
			if got := manager.Used(); got != 0 {
				t.Fatalf("managed leak=%d", got)
			}
		})
	}
}
