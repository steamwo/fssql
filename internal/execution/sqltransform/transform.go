package sqltransform

import (
	"context"
	"fmt"

	"github.com/steamwo/fssql/internal/core/types"
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
	if predicate == nil || predicate.Type.ID != types.Boolean {
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
	rows, err := f.selectRows(ctx, in)
	if err != nil { return nil, err }

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

// Transform materializes borrowed column references and transfers owned
// expression results. Every output is independent from input backing storage.
func (p *Project) Transform(ctx context.Context, in *vector.DataChunk) (*vector.DataChunk, error) {
	if p == nil || p.Executor == nil || in == nil || len(p.Expressions) == 0 {
		return nil, fmt.Errorf("sql project requires initialized transform and input chunk")
	}
	if in.Cardinality < 0 || in.Cardinality > vector.DefaultVectorSize {
		return nil, fmt.Errorf("sql project invalid chunk cardinality")
	}
	var identity [vector.DefaultVectorSize]int
	for i := 0; i < in.Cardinality; i++ {
		identity[i] = i
	}
	columns := make([]*vector.Vector, 0, len(p.Expressions))
	for index, expr := range p.Expressions {
		if err := ctx.Err(); err != nil {
			releaseVectors(columns)
			return nil, err
		}
		if expr.Kind == binder.BoundColumn {
			if expr.ColumnIndex < 0 || expr.ColumnIndex >= len(in.Columns) {
				releaseVectors(columns)
				return nil, fmt.Errorf("sql project column out of range")
			}
			materialized, err := vector.CopySelection(p.Executor.Scope, p.Executor.Buffers, in.Columns[expr.ColumnIndex], identity[:in.Cardinality])
			if err != nil {
				releaseVectors(columns)
				return nil, err
			}
			columns = append(columns, materialized)
			continue
		}
		value, err := p.Executor.ExecuteSQL(expr, in)
		if err != nil {
			releaseVectors(columns)
			return nil, fmt.Errorf("sql project expression %d: %w", index, err)
		}
		// Flat and constant expression results already own their storage.
		// Dictionary expressions borrow the input and must still materialize.
		if value.Kind != vector.Dictionary {
			columns = append(columns, value)
			continue
		}
		materialized, err := vector.CopySelection(p.Executor.Scope, p.Executor.Buffers, value, identity[:in.Cardinality])
		value.Release()
		if err != nil {
			releaseVectors(columns)
			return nil, fmt.Errorf("sql project expression %d: %w", index, err)
		}
		columns = append(columns, materialized)
	}
	chunk, err := vector.NewDataChunk(columns...)
	if err != nil {
		releaseVectors(columns)
		return nil, err
	}
	return chunk, nil
}

// materializeSelection owns selected data and preserves constant placeholders.
func materializeSelection(ctx context.Context, executor *expression.Executor, in *vector.DataChunk, rows []int) (*vector.DataChunk, error) {
	columns := make([]*vector.Vector, 0, len(in.Columns))
	for columnIndex, source := range in.Columns {
		if source == nil {
			releaseVectors(columns)
			return nil, fmt.Errorf("sql filter input column %d is nil", columnIndex)
		}
		if err := ctx.Err(); err != nil {
			releaseVectors(columns)
			return nil, err
		}
		out, err := vector.CopySelection(executor.Scope, executor.Buffers, source, rows)
		if err != nil {
			releaseVectors(columns)
			return nil, fmt.Errorf("sql filter column %d: %w", columnIndex, err)
		}
		columns = append(columns, out)
	}
	chunk, err := vector.NewDataChunk(columns...)
	if err != nil {
		releaseVectors(columns)
		return nil, err
	}
	// A relation may legally carry rows with zero physical columns (for example
	// SELECT literals before projection). NewDataChunk cannot infer cardinality
	// without a column, so preserve the filter selection count explicitly.
	if len(columns) == 0 {
		chunk.Cardinality = len(rows)
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
