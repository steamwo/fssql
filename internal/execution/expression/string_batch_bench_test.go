package expression

import (
	"testing"

	"github.com/steamwo/fssql/internal/core/memory"
	"github.com/steamwo/fssql/internal/core/types"
	"github.com/steamwo/fssql/internal/core/vector"
	"github.com/steamwo/fssql/internal/function"
	"github.com/steamwo/fssql/internal/sql/binder"
)

// BenchmarkBuiltinStringBatch2048 测量 2048 行真实 ExecuteSQL 字符串批路径的稳态开销。
func BenchmarkBuiltinStringBatch2048(b *testing.B) {
	for _, tc := range []struct {
		name   string
		id     function.ID
		binary bool
	}{{"length", function.LengthID, false}, {"strpos", function.StrposID, true}, {"concat", function.ConcatID, true}, {"lower", function.LowerID, false}} {
		b.Run(tc.name, func(b *testing.B) {
			manager := memory.NewManager(8 << 20)
			scope := manager.NewScope()
			buffers := memory.NewBufferManager(nil)
			text := types.LogicalType{ID: types.Varchar}
			left, err := vector.NewFlat(scope, buffers, text, 2048)
			if err != nil {
				b.Fatal(err)
			}
			for row := 0; row < 2048; row++ {
				if err := left.SetString(row, "prefix-β-middle-😊-a string longer than inline storage"); err != nil {
					b.Fatal(err)
				}
			}
			right, err := vector.NewConstant(scope, buffers, text, 2048)
			if err != nil {
				b.Fatal(err)
			}
			if err := right.SetString(0, "β"); err != nil {
				b.Fatal(err)
			}
			chunk, err := vector.NewDataChunk(left, right)
			if err != nil {
				b.Fatal(err)
			}
			result := types.LogicalType{ID: types.BigInt}
			if tc.id == function.ConcatID || tc.id == function.LowerID {
				result = text
			}
			expr := &binder.BoundExpression{Kind: binder.BoundFunction, Type: result, FunctionID: tc.id, FunctionKind: function.Scalar, Children: []*binder.BoundExpression{{Kind: binder.BoundColumn, Type: text, ColumnIndex: 0}}}
			if tc.binary {
				expr.Children = append(expr.Children, &binder.BoundExpression{Kind: binder.BoundColumn, Type: text, ColumnIndex: 1})
			}
			executor := NewExecutor(scope, buffers)
			b.ReportAllocs()
			b.ResetTimer()
			for iteration := 0; iteration < b.N; iteration++ {
				out, err := executor.ExecuteSQL(expr, chunk)
				if err != nil {
					b.Fatal(err)
				}
				out.Release()
			}
			b.StopTimer()
			b.ReportMetric(float64(manager.Peak()), "managed-peak-B")
			chunk.Release()
			if scope.Used() != 0 {
				b.Fatalf("managed leak=%d", scope.Used())
			}
		})
	}
}

func BenchmarkBooleanAndBatch2048(b *testing.B) {
	manager := memory.NewManager(1 << 20)
	scope := manager.NewScope()
	buffers := memory.NewBufferManager(nil)
	typ := types.LogicalType{ID: types.Boolean}
	left, err := vector.NewFlat(scope, buffers, typ, 2048)
	if err != nil {
		b.Fatal(err)
	}
	right, err := vector.NewFlat(scope, buffers, typ, 2048)
	if err != nil {
		b.Fatal(err)
	}
	for row := 0; row < 2048; row++ {
		left.SetBool(row, row%2 == 0)
		right.SetBool(row, row%3 == 0)
		if row%7 == 0 {
			left.SetNull(row)
		}
	}
	chunk, err := vector.NewDataChunk(left, right)
	if err != nil {
		b.Fatal(err)
	}
	expr := &BoundExpression{Kind: And, Type: typ, Children: []*BoundExpression{{Kind: Column, Type: typ, ColumnIndex: 0}, {Kind: Column, Type: typ, ColumnIndex: 1}}}
	executor := NewExecutor(scope, buffers)
	b.ReportAllocs()
	b.ResetTimer()
	for iteration := 0; iteration < b.N; iteration++ {
		out, err := executor.Execute(expr, chunk)
		if err != nil {
			b.Fatal(err)
		}
		out.Release()
	}
	b.StopTimer()
	chunk.Release()
	if scope.Used() != 0 {
		b.Fatalf("managed leak=%d", scope.Used())
	}
}
