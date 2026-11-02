package sqltransform

import (
	"context"
	"fmt"

	"github.com/steamwo/fssql/internal/core/vector"
	"github.com/steamwo/fssql/internal/execution/expression"
	"github.com/steamwo/fssql/internal/execution/pipeline"
	"github.com/steamwo/fssql/internal/sql/binder"
)

// Filter 把 Binder BOOLEAN predicate 作为 Push Pipeline replacement transform 执行。
type Filter struct {
	Executor  *expression.Executor    // Executor 复用统一 SQL expression kernel 与 query managed memory。
	Predicate *binder.BoundExpression // Predicate 必须已由 Binder 强类型解析为 BOOLEAN。
}

var _ pipeline.TransformOperator = (*Filter)(nil)

// NewFilter 创建 SQL filter transform；缺少 managed executor 或非 BOOLEAN predicate 时立即失败。
func NewFilter(executor *expression.Executor, predicate *binder.BoundExpression) (*Filter, error) {
	if executor == nil || executor.Scope == nil || executor.Buffers == nil {
		return nil, fmt.Errorf("sql filter requires managed expression executor")
	}
	if predicate == nil || predicate.Type.ID != 1 { // types.Boolean 的稳定枚举值由 core/types 定义。
		return nil, fmt.Errorf("sql filter requires BOOLEAN predicate")
	}
	return &Filter{Executor: executor, Predicate: predicate}, nil
}

// Transform 计算 WHERE selection，并把选中行复制到完全独立的 owned DataChunk。
func (f *Filter) Transform(ctx context.Context, in *vector.DataChunk) (*vector.DataChunk, error) {
	if f == nil || f.Executor == nil || f.Predicate == nil || in == nil {
		return nil, fmt.Errorf("sql filter requires initialized transform and input chunk")
	}
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	predicate, err := f.Executor.ExecuteSQL(f.Predicate, in)
	if err != nil {
		return nil, err
	}
	defer predicate.Release()
	rows := make([]int, 0, in.Cardinality)
	for row := 0; row < in.Cardinality; row++ {
		if err := ctx.Err(); err != nil {
			return nil, err
		}
		value, valid := predicate.Bool(row)
		if valid && value {
			rows = append(rows, row)
		}
	}
	return materializeSelection(ctx, f.Executor, in, rows)
}

// Project 执行已绑定 projection expressions，并把每列结果物化为独立 owned Vector。
type Project struct {
	Executor    *expression.Executor      // Executor 复用 SQL typed kernel 与 query managed memory。
	Expressions []*binder.BoundExpression // Expressions 按最终输出列顺序保存。
}

var _ pipeline.TransformOperator = (*Project)(nil)

// NewProject 创建 SQL projection transform；空 projection 或未初始化 expression 会显式失败。
func NewProject(executor *expression.Executor, expressions []*binder.BoundExpression) (*Project, error) {
	if executor == nil || executor.Scope == nil || executor.Buffers == nil {
		return nil, fmt.Errorf("sql project requires managed expression executor")
	}
	if len(expressions) == 0 {
		return nil, fmt.Errorf("sql project requires at least one expression")
	}
	out := make([]*binder.BoundExpression, len(expressions))
	copy(out, expressions)
	for i, expr := range out {
		if expr == nil {
			return nil, fmt.Errorf("sql project expression %d is nil", i)
		}
	}
	return &Project{Executor: executor, Expressions: out}, nil
}

// Transform 逐列执行 SQL expression，并复制到独立 flat vectors，避免 Dictionary Vector 借用 input backing storage。
func (p *Project) Transform(ctx context.Context, in *vector.DataChunk) (*vector.DataChunk, error) {
	if p == nil || p.Executor == nil || in == nil || len(p.Expressions) == 0 {
		return nil, fmt.Errorf("sql project requires initialized transform and input chunk")
	}
	columns := make([]*vector.Vector, 0, len(p.Expressions))
	for index, expr := range p.Expressions {
		if err := ctx.Err(); err != nil {
			releaseVectors(columns)
			return nil, err
		}
		value, err := p.Executor.ExecuteSQL(expr, in)
		if err != nil {
			releaseVectors(columns)
			return nil, fmt.Errorf("sql project expression %d: %w", index, err)
		}
		materialized, err := vector.NewFlat(p.Executor.Scope, p.Executor.Buffers, value.Type, in.Cardinality)
		if err != nil {
			value.Release()
			releaseVectors(columns)
			return nil, err
		}
		for row := 0; row < in.Cardinality; row++ {
			if err := ctx.Err(); err != nil {
				materialized.Release()
				value.Release()
				releaseVectors(columns)
				return nil, err
			}
			if err := vector.CopyValue(value, row, materialized, row); err != nil {
				materialized.Release()
				value.Release()
				releaseVectors(columns)
				return nil, fmt.Errorf("sql project expression %d row %d: %w", index, row, err)
			}
		}
		value.Release()
		columns = append(columns, materialized)
	}
	chunk, err := vector.NewDataChunk(columns...)
	if err != nil {
		releaseVectors(columns)
		return nil, err
	}
	return chunk, nil
}

// materializeSelection 根据 selection 将输入所有列复制为独立 flat vectors；复杂类型缺少 CopyValue kernel 时显式失败。
func materializeSelection(ctx context.Context, executor *expression.Executor, in *vector.DataChunk, rows []int) (*vector.DataChunk, error) {
	columns := make([]*vector.Vector, 0, len(in.Columns))
	for columnIndex, source := range in.Columns {
		if source == nil {
			releaseVectors(columns)
			return nil, fmt.Errorf("sql filter input column %d is nil", columnIndex)
		}
		out, err := vector.NewFlat(executor.Scope, executor.Buffers, source.Type, len(rows))
		if err != nil {
			releaseVectors(columns)
			return nil, err
		}
		for outputRow, inputRow := range rows {
			if err := ctx.Err(); err != nil {
				out.Release()
				releaseVectors(columns)
				return nil, err
			}
			if err := vector.CopyValue(source, inputRow, out, outputRow); err != nil {
				out.Release()
				releaseVectors(columns)
				return nil, fmt.Errorf("sql filter column %d row %d: %w", columnIndex, inputRow, err)
			}
		}
		columns = append(columns, out)
	}
	chunk, err := vector.NewDataChunk(columns...)
	if err != nil {
		releaseVectors(columns)
		return nil, err
	}
	return chunk, nil
}

// releaseVectors 统一释放部分构造结果，保证错误路径不遗留 managed reservations。
func releaseVectors(columns []*vector.Vector) {
	for _, column := range columns {
		if column != nil {
			column.Release()
		}
	}
}
