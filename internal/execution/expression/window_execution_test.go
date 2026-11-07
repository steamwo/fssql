package expression

import (
	"fmt"
	"math"
	"testing"

	"github.com/steamwo/fssql/internal/core/memory"
	"github.com/steamwo/fssql/internal/core/types"
	"github.com/steamwo/fssql/internal/core/vector"
	"github.com/steamwo/fssql/internal/function"
	"github.com/steamwo/fssql/internal/sql/binder"
)

// windowRowNumberExpr 构造直接经过窗口执行器的分区 ROW_NUMBER 表达式。
func windowRowNumberExpr(partition ...*binder.BoundExpression) *binder.BoundExpression {
	children := []*binder.BoundExpression{{
		Kind:    binder.BoundLiteral,
		Type:    types.LogicalType{ID: types.Varchar},
		Literal: fmt.Sprintf("p=%d;o=0;d=;n=;f=none", len(partition)),
	}}
	children = append(children, partition...)
	return &binder.BoundExpression{
		Kind:         binder.BoundFunction,
		Type:         types.LogicalType{ID: types.BigInt},
		FunctionID:   function.WindowRowNumberID,
		FunctionKind: function.Scalar,
		Children:     children,
	}
}

// windowTestEnvironment 创建窗口执行测试环境，并在测试结束时释放全局状态。
func windowTestEnvironment(t testing.TB) (*memory.Manager, *Executor, *memory.Scope, *memory.BufferManager) {
	t.Helper()
	manager := memory.NewManager(64 << 20)
	scope := manager.NewScope()
	buffers := memory.NewBufferManager(nil)
	executor := NewExecutor(scope, buffers)
	t.Cleanup(func() {
		ReleaseWindowState(executor)
		if err := scope.Close(); err != nil {
			t.Fatal(err)
		}
		if manager.Used() != 0 {
			t.Fatalf("managed memory leaked: %d", manager.Used())
		}
	})
	return manager, executor, scope, buffers
}

// windowColumn 构造窗口分区列表达式。
func windowColumn(index int, typ types.LogicalType) *binder.BoundExpression {
	return &binder.BoundExpression{Kind: binder.BoundColumn, Type: typ, ColumnIndex: index}
}

// windowStringVector 创建包含指定字符串的扁平向量。
func windowStringVector(t testing.TB, scope *memory.Scope, buffers *memory.BufferManager, values []string) *vector.Vector {
	t.Helper()
	input, err := vector.NewFlat(scope, buffers, types.LogicalType{ID: types.Varchar}, len(values))
	if err != nil {
		t.Fatal(err)
	}
	for row, value := range values {
		if err := input.SetString(row, value); err != nil {
			input.Release()
			t.Fatal(err)
		}
	}
	return input
}

// assertWindowRowNumbers 通过完整 ExecuteSQL 路径校验分区行号。
func assertWindowRowNumbers(t testing.TB, executor *Executor, expr *binder.BoundExpression, chunk *vector.DataChunk, want []int64) {
	t.Helper()
	out, err := executor.ExecuteSQL(expr, chunk)
	if err != nil {
		t.Fatal(err)
	}
	defer out.Release()
	if out.Count() != len(want) {
		t.Fatalf("row count=%d want=%d", out.Count(), len(want))
	}
	for row, expected := range want {
		value, ok := out.Int64(row)
		if !ok || value != expected {
			t.Fatalf("row=%d row_number=%d valid=%v want=%d", row, value, ok, expected)
		}
	}
}

// TestWindowPartitionOwnsSavedStringKey 验证保存键不会被后续 scratch 写入覆盖。
func TestWindowPartitionOwnsSavedStringKey(t *testing.T) {
	_, executor, scope, buffers := windowTestEnvironment(t)
	values := []string{
		"partition-alpha-0123456789", "partition-alpha-0123456789",
		"partition-beta-0123456789", "partition-beta-0123456789",
		"partition-gamma-0123456789",
	}
	input := windowStringVector(t, scope, buffers, values)
	chunk, err := vector.NewDataChunk(input)
	if err != nil {
		t.Fatal(err)
	}
	defer chunk.Release()
	expr := windowRowNumberExpr(windowColumn(0, types.LogicalType{ID: types.Varchar}))
	assertWindowRowNumbers(t, executor, expr, chunk, []int64{1, 2, 1, 2, 1})

	state := windowStateFor(executor, expr)
	if len(state.partition) != 1 || string(state.partition[0].Bytes) != values[len(values)-1] {
		t.Fatalf("saved partition=%q want=%q", state.partition[0].Bytes, values[len(values)-1])
	}
	if _, err := state.partitionKey(chunk.Columns, 0); err != nil {
		t.Fatal(err)
	}
	if got := string(state.partition[0].Bytes); got != values[len(values)-1] {
		t.Fatalf("saved partition overwritten by scratch: %q", got)
	}
	for i := range state.keyScratch {
		state.keyScratch[i].Bytes = nil
	}
}

// TestWindowPartitionAcross2048Boundary 验证跨 chunk 续分区、切换分区及 NULL 分区。
func TestWindowPartitionAcross2048Boundary(t *testing.T) {
	_, executor, scope, buffers := windowTestEnvironment(t)
	const partitionA = "cross-chunk-alpha-0123456789"
	const partitionB = "cross-chunk-beta-0123456789"
	firstValues := make([]string, vector.DefaultVectorSize)
	for row := range firstValues {
		firstValues[row] = partitionA
	}
	first := windowStringVector(t, scope, buffers, firstValues)
	firstChunk, err := vector.NewDataChunk(first)
	if err != nil {
		t.Fatal(err)
	}
	defer firstChunk.Release()
	second := windowStringVector(t, scope, buffers, []string{partitionA, partitionB, partitionB})
	secondChunk, err := vector.NewDataChunk(second)
	if err != nil {
		t.Fatal(err)
	}
	defer secondChunk.Release()
	third := windowStringVector(t, scope, buffers, []string{partitionB, "", partitionA, partitionA})
	third.SetNull(1)
	thirdChunk, err := vector.NewDataChunk(third)
	if err != nil {
		t.Fatal(err)
	}
	defer thirdChunk.Release()

	expr := windowRowNumberExpr(windowColumn(0, types.LogicalType{ID: types.Varchar}))
	firstWant := make([]int64, vector.DefaultVectorSize)
	for row := range firstWant {
		firstWant[row] = int64(row + 1)
	}
	assertWindowRowNumbers(t, executor, expr, firstChunk, firstWant)
	assertWindowRowNumbers(t, executor, expr, secondChunk, []int64{vector.DefaultVectorSize + 1, 1, 2})
	assertWindowRowNumbers(t, executor, expr, thirdChunk, []int64{3, 1, 1, 2})
}

// TestWindowPartitionDictionaryAndConstant 验证字典和常量向量的借用键语义。
func TestWindowPartitionDictionaryAndConstant(t *testing.T) {
	t.Run("dictionary", func(t *testing.T) {
		_, executor, scope, buffers := windowTestEnvironment(t)
		base := windowStringVector(t, scope, buffers, []string{
			"dictionary-alpha-0123456789", "dictionary-beta-0123456789", "dictionary-gamma-0123456789",
		})
		defer base.Release()
		dictionary, err := vector.NewDictionary(base, vector.SelectionFrom([]uint32{0, 0, 1, 2, 0}))
		if err != nil {
			t.Fatal(err)
		}
		chunk, err := vector.NewDataChunk(dictionary)
		if err != nil {
			t.Fatal(err)
		}
		defer chunk.Release()
		expr := windowRowNumberExpr(windowColumn(0, types.LogicalType{ID: types.Varchar}))
		assertWindowRowNumbers(t, executor, expr, chunk, []int64{1, 2, 1, 1, 1})
	})

	t.Run("constant", func(t *testing.T) {
		_, executor, scope, buffers := windowTestEnvironment(t)
		input, err := vector.NewConstant(scope, buffers, types.LogicalType{ID: types.Varchar}, 4)
		if err != nil {
			t.Fatal(err)
		}
		if err := input.SetString(0, "constant-partition-0123456789"); err != nil {
			t.Fatal(err)
		}
		chunk, err := vector.NewDataChunk(input)
		if err != nil {
			t.Fatal(err)
		}
		defer chunk.Release()
		expr := windowRowNumberExpr(windowColumn(0, types.LogicalType{ID: types.Varchar}))
		assertWindowRowNumbers(t, executor, expr, chunk, []int64{1, 2, 3, 4})
	})
}

// TestWindowPartitionMultiColumnAndMultiWindow 验证多列键、NaN 与多个窗口状态互不影响。
func TestWindowPartitionMultiColumnAndMultiWindow(t *testing.T) {
	_, executor, scope, buffers := windowTestEnvironment(t)
	textType := types.LogicalType{ID: types.Varchar}
	decimalType := types.LogicalType{ID: types.Decimal, Decimal: types.DecimalInfo{Width: 18, Scale: 0}}
	doubleType := types.LogicalType{ID: types.Double}
	text := windowStringVector(t, scope, buffers, []string{"a", "a", "a", "b", "b"})
	decimal, err := vector.NewFlat(scope, buffers, decimalType, 5)
	if err != nil {
		t.Fatal(err)
	}
	for row, value := range []int64{1, 1, 2, 2, 2} {
		decimal.SetInt64(row, value)
	}
	floating, err := vector.NewFlat(scope, buffers, doubleType, 5)
	if err != nil {
		t.Fatal(err)
	}
	for row := 0; row < 4; row++ {
		floating.SetFloat64(row, math.NaN())
	}
	floating.SetNull(4)
	chunk, err := vector.NewDataChunk(text, decimal, floating)
	if err != nil {
		t.Fatal(err)
	}
	defer chunk.Release()

	multiColumn := windowRowNumberExpr(windowColumn(0, textType), windowColumn(1, decimalType))
	textOnly := windowRowNumberExpr(windowColumn(0, textType))
	nanAndNull := windowRowNumberExpr(windowColumn(2, doubleType))
	assertWindowRowNumbers(t, executor, multiColumn, chunk, []int64{1, 2, 1, 1, 2})
	assertWindowRowNumbers(t, executor, textOnly, chunk, []int64{1, 2, 3, 1, 2})
	assertWindowRowNumbers(t, executor, nanAndNull, chunk, []int64{1, 2, 3, 4, 1})
	assertWindowRowNumbers(t, executor, multiColumn, chunk, []int64{1, 2, 1, 1, 2})
}

// TestWindowPartitionDecimalWidths 验证窄、宽 Decimal 分区键保持既有比较语义。
func TestWindowPartitionDecimalWidths(t *testing.T) {
	for _, width := range []uint8{18, 19} {
		t.Run(fmt.Sprintf("width_%d", width), func(t *testing.T) {
			_, executor, scope, buffers := windowTestEnvironment(t)
			decimalType := types.LogicalType{ID: types.Decimal, Decimal: types.DecimalInfo{Width: width, Scale: 0}}
			input, err := vector.NewFlat(scope, buffers, decimalType, 5)
			if err != nil {
				t.Fatal(err)
			}
			for row, value := range []int64{1, 1, 2, 2, 1} {
				if width <= 18 {
					input.SetInt64(row, value)
				} else {
					input.SetInt128(row, types.Int128FromInt64(value))
				}
			}
			chunk, err := vector.NewDataChunk(input)
			if err != nil {
				t.Fatal(err)
			}
			defer chunk.Release()
			expr := windowRowNumberExpr(windowColumn(0, decimalType))
			assertWindowRowNumbers(t, executor, expr, chunk, []int64{1, 2, 1, 2, 1})
		})
	}
}

// TestWindowPartitionKeyErrorCleanup 验证部分键已借用后出错仍释放结果并清除引用。
func TestWindowPartitionKeyErrorCleanup(t *testing.T) {
	manager, executor, scope, buffers := windowTestEnvironment(t)
	textType := types.LogicalType{ID: types.Varchar}
	text := windowStringVector(t, scope, buffers, []string{"before-error-0123456789"})
	childType := types.LogicalType{ID: types.BigInt}
	listType := types.LogicalType{ID: types.List, Child: &childType}
	list, err := vector.NewFlat(scope, buffers, listType, 1)
	if err != nil {
		t.Fatal(err)
	}
	chunk, err := vector.NewDataChunk(text, list)
	if err != nil {
		t.Fatal(err)
	}
	expr := windowRowNumberExpr(windowColumn(0, textType), windowColumn(1, listType))
	out, err := executor.ExecuteSQL(expr, chunk)
	if err == nil {
		if out != nil {
			out.Release()
		}
		t.Fatal("unsupported partition key unexpectedly succeeded")
	}
	if out != nil {
		out.Release()
		t.Fatal("failed window returned an output vector")
	}
	state := windowStateFor(executor, expr)
	for i := range state.keyScratch {
		if state.keyScratch[i].Bytes != nil {
			t.Fatal("borrowed bytes retained after window error")
		}
	}
	chunk.Release()
	if manager.Used() != 0 {
		t.Fatalf("managed memory retained after error: %d", manager.Used())
	}
}

// TestWindowBorrowingPreservesScalarOwnership 验证窗口借用不改变通用标量值的字节所有权。
func TestWindowBorrowingPreservesScalarOwnership(t *testing.T) {
	_, _, scope, buffers := windowTestEnvironment(t)
	input := windowStringVector(t, scope, buffers, []string{"owned-scalar-0123456789"})
	defer input.Release()
	value, err := scalarFromVector(input, 0)
	if err != nil {
		t.Fatal(err)
	}
	value.Bytes[0] = 'X'
	got, ok := input.Bytes(0)
	if !ok || string(got) != "owned-scalar-0123456789" {
		t.Fatalf("scalarFromVector aliases vector bytes: %q valid=%v", got, ok)
	}
}

// BenchmarkWindowRowNumberVarcharPartition 测量包含分区列求值和结果物化的窗口执行分配。
func BenchmarkWindowRowNumberVarcharPartition(b *testing.B) {
	const rowsPerPartition = 16
	manager := memory.NewManager(64 << 20)
	scope := manager.NewScope()
	buffers := memory.NewBufferManager(nil)
	input, err := vector.NewFlat(scope, buffers, types.LogicalType{ID: types.Varchar}, vector.DefaultVectorSize)
	if err != nil {
		b.Fatal(err)
	}
	for row := 0; row < vector.DefaultVectorSize; row++ {
		value := fmt.Sprintf("partition-%04d-0123456789abcdef", row/rowsPerPartition)
		if err := input.SetString(row, value); err != nil {
			b.Fatal(err)
		}
	}
	chunk, err := vector.NewDataChunk(input)
	if err != nil {
		b.Fatal(err)
	}
	executor := NewExecutor(scope, buffers)
	expr := windowRowNumberExpr(&binder.BoundExpression{
		Kind:        binder.BoundColumn,
		Type:        types.LogicalType{ID: types.Varchar},
		ColumnIndex: 0,
	})
	defer func() {
		ReleaseWindowState(executor)
		chunk.Release()
		if err := scope.Close(); err != nil {
			b.Fatal(err)
		}
		if manager.Used() != 0 {
			b.Fatalf("managed memory leaked: %d", manager.Used())
		}
	}()

	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		out, err := executor.ExecuteSQL(expr, chunk)
		if err != nil {
			b.Fatal(err)
		}
		out.Release()
	}
}
