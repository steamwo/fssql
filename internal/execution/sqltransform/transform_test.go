package sqltransform

import (
	"context"
	"testing"

	"github.com/steamwo/fssql/internal/core/memory"
	"github.com/steamwo/fssql/internal/core/types"
	"github.com/steamwo/fssql/internal/core/vector"
	"github.com/steamwo/fssql/internal/execution/expression"
	"github.com/steamwo/fssql/internal/sql/binder"
)

// sqlTransformFixture 创建 managed executor 与单列 BIGINT input；cleanup 要求调用方已释放所有结果 chunk。
func sqlTransformFixture(t *testing.T, values ...int64) (*memory.Manager, *memory.Scope, *expression.Executor, *vector.DataChunk) {
	t.Helper()
	manager := memory.NewManager(1 << 20)
	scope := manager.NewScope()
	buffers := memory.NewBufferManager(nil)
	column, err := vector.NewFlat(scope, buffers, types.LogicalType{ID: types.BigInt}, len(values))
	if err != nil {
		t.Fatal(err)
	}
	for row, value := range values {
		column.SetInt64(row, value)
	}
	chunk, err := vector.NewDataChunk(column)
	if err != nil {
		column.Release()
		t.Fatal(err)
	}
	return manager, scope, expression.NewExecutor(scope, buffers), chunk
}

// TestFilterProducesIndependentChunk 验证 WHERE selection 被复制到独立 backing storage，input 释放后结果仍可读取。
func TestFilterProducesIndependentChunk(t *testing.T) {
	manager, scope, executor, input := sqlTransformFixture(t, 1, 2, 3)
	column := &binder.BoundExpression{Kind: binder.BoundColumn, Type: types.LogicalType{ID: types.BigInt}, ColumnIndex: 0}
	one := &binder.BoundExpression{Kind: binder.BoundLiteral, Type: types.LogicalType{ID: types.BigInt}, Literal: "1"}
	predicate := &binder.BoundExpression{Kind: binder.BoundBinary, Type: types.LogicalType{ID: types.Boolean}, Op: ">", Children: []*binder.BoundExpression{column, one}}
	filter, err := NewFilter(executor, predicate)
	if err != nil {
		input.Release()
		t.Fatal(err)
	}
	output, err := filter.Transform(context.Background(), input)
	if err != nil {
		input.Release()
		t.Fatal(err)
	}
	input.Release()
	if output.Cardinality != 2 {
		output.Release()
		t.Fatalf("cardinality=%d", output.Cardinality)
	}
	for row, want := range []int64{2, 3} {
		got, ok := output.Columns[0].Int64(row)
		if !ok || got != want {
			output.Release()
			t.Fatalf("row=%d got=%d ok=%v want=%d", row, got, ok, want)
		}
	}
	output.Release()
	if manager.Used() != 0 {
		t.Fatalf("managed leak=%d", manager.Used())
	}
	if err := scope.Close(); err != nil {
		t.Fatal(err)
	}
}

// TestProjectMaterializesBorrowedColumn 验证纯列 projection 返回的 Dictionary Vector 不会跨 replacement ownership 泄漏。
func TestProjectMaterializesBorrowedColumn(t *testing.T) {
	manager, scope, executor, input := sqlTransformFixture(t, 4, 5)
	column := &binder.BoundExpression{Kind: binder.BoundColumn, Type: types.LogicalType{ID: types.BigInt}, ColumnIndex: 0}
	one := &binder.BoundExpression{Kind: binder.BoundLiteral, Type: types.LogicalType{ID: types.BigInt}, Literal: "1"}
	plusOne := &binder.BoundExpression{Kind: binder.BoundBinary, Type: types.LogicalType{ID: types.BigInt}, Op: "+", Children: []*binder.BoundExpression{column, one}}
	project, err := NewProject(executor, []*binder.BoundExpression{column, plusOne})
	if err != nil {
		input.Release()
		t.Fatal(err)
	}
	output, err := project.Transform(context.Background(), input)
	if err != nil {
		input.Release()
		t.Fatal(err)
	}
	input.Release()
	for row, want := range []int64{4, 5} {
		got, ok := output.Columns[0].Int64(row)
		if !ok || got != want {
			output.Release()
			t.Fatalf("column0 row=%d got=%d ok=%v want=%d", row, got, ok, want)
		}
		got, ok = output.Columns[1].Int64(row)
		if !ok || got != want+1 {
			output.Release()
			t.Fatalf("column1 row=%d got=%d ok=%v want=%d", row, got, ok, want+1)
		}
	}
	output.Release()
	if manager.Used() != 0 {
		t.Fatalf("managed leak=%d", manager.Used())
	}
	if err := scope.Close(); err != nil {
		t.Fatal(err)
	}
}

// TestProjectNestedCopyFailsClosed 验证尚无独立 CopyValue kernel 的 LIST 不会降级成 borrowed replacement。
func TestProjectNestedCopyFailsClosed(t *testing.T) {
	manager := memory.NewManager(1 << 20)
	scope := manager.NewScope()
	buffers := memory.NewBufferManager(nil)
	listType := types.NewList(types.LogicalType{ID: types.BigInt})
	column, err := vector.NewFlat(scope, buffers, listType, 1)
	if err != nil {
		t.Fatal(err)
	}
	column.SetListEntry(0, vector.ListEntry{Offset: 0, Length: 0})
	input, err := vector.NewDataChunk(column)
	if err != nil {
		column.Release()
		t.Fatal(err)
	}
	executor := expression.NewExecutor(scope, buffers)
	expr := &binder.BoundExpression{Kind: binder.BoundColumn, Type: listType, ColumnIndex: 0}
	project, err := NewProject(executor, []*binder.BoundExpression{expr})
	if err != nil {
		input.Release()
		t.Fatal(err)
	}
	if output, err := project.Transform(context.Background(), input); err == nil {
		if output != nil {
			output.Release()
		}
		input.Release()
		t.Fatal("expected LIST materialization to fail closed")
	}
	input.Release()
	if manager.Used() != 0 {
		t.Fatalf("managed leak=%d", manager.Used())
	}
	if err := scope.Close(); err != nil {
		t.Fatal(err)
	}
}
