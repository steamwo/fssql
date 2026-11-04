package sqltransform

import (
	"context"
	"fmt"

	"github.com/steamwo/fssql/internal/core/vector"
	"github.com/steamwo/fssql/internal/sql/binder"
)

// TransformOwned moves pure column projections, while duplicates remain
// independent. Computed projections retain the ordinary materializing contract.
func (p *Project) TransformOwned(ctx context.Context, in *vector.DataChunk) (*vector.DataChunk, error) {
	if p == nil || p.Executor == nil || in == nil || len(p.Expressions) == 0 {
		return p.Transform(ctx, in)
	}
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	for _, expr := range p.Expressions {
		if expr == nil || expr.Kind != binder.BoundColumn {
			return p.Transform(ctx, in)
		}
		if expr.ColumnIndex < 0 || expr.ColumnIndex >= len(in.Columns) || in.Columns[expr.ColumnIndex] == nil {
			return nil, fmt.Errorf("sql project column out of range")
		}
		if in.Columns[expr.ColumnIndex].Count() != in.Cardinality {
			return p.Transform(ctx, in)
		}
	}
	columns := make([]*vector.Vector, len(p.Expressions))
	moved := make([]bool, len(in.Columns))
	owned := make([]bool, len(columns))
	var identity [vector.DefaultVectorSize]int
	if in.Cardinality < 0 || in.Cardinality > len(identity) {
		return nil, fmt.Errorf("sql project invalid chunk cardinality")
	}
	for i := 0; i < in.Cardinality; i++ {
		identity[i] = i
	}
	cleanup := func() {
		for i, v := range columns {
			if owned[i] && v != nil {
				v.Release()
			}
		}
	}
	for i, expr := range p.Expressions {
		index := expr.ColumnIndex
		if !moved[index] {
			columns[i] = in.Columns[index]
			moved[index] = true
			continue
		}
		v, err := vector.CopySelection(p.Executor.Scope, p.Executor.Buffers, in.Columns[index], identity[:in.Cardinality])
		if err != nil {
			cleanup()
			return nil, err
		}
		columns[i] = v
		owned[i] = true
	}
	out, err := vector.NewDataChunk(columns...)
	if err != nil {
		cleanup()
		return nil, err
	}
	for index, move := range moved {
		if move {
			in.Columns[index] = nil
		}
	}
	return out, nil
}

// TransformOwned retains a selected input behind owned dictionaries, avoiding
// payload copies. Preparation is transactional: dictionaries borrow until every
// allocation succeeds, then input columns are detached in one commit.
func (f *Filter) TransformOwned(ctx context.Context, in *vector.DataChunk) (*vector.DataChunk, error) {
	if f == nil || f.Executor == nil || f.Predicate == nil || in == nil {
		return nil, fmt.Errorf("sql filter requires initialized transform and input chunk")
	}
	for i, source := range in.Columns {
		if source == nil {
			return nil, fmt.Errorf("sql filter input column %d is nil", i)
		}
	}
	rows, err := f.selectRows(ctx, in)
	if err != nil {
		return nil, err
	}
	if len(rows) == in.Cardinality {
		return in, nil
	}
	columns := make([]*vector.Vector, len(in.Columns))
	sel := vector.SelectionFromRows(rows)
	for i, source := range in.Columns {
		if source.Kind == vector.Constant {
			continue
		}
		// Hold no child ownership until all allocations succeed.
		v, err := vector.NewOwnedDictionary(f.Executor.Scope, source, sel)
		if err != nil {
			for _, v := range columns {
				if v != nil {
					v.DisownDictionary()
					v.Release()
				}
			}
			return nil, err
		}
		columns[i] = v
	}
	for i, source := range in.Columns {
		if source.Kind == vector.Constant {
			_ = source.SetConstantCount(len(rows))
			columns[i] = source
		}
		in.Columns[i] = nil
	}
	return &vector.DataChunk{Columns: columns, Cardinality: len(rows)}, nil
}

func (f *Filter) selectRows(ctx context.Context, in *vector.DataChunk) ([]int, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	rows := make([]int, 0, in.Cardinality)
	if selected, handled, err := f.Executor.SelectSQL(f.Predicate, in, rows); handled {
		return selected, err
	}
	predicate, err := f.Executor.ExecuteSQL(f.Predicate, in)
	if err != nil {
		return nil, err
	}
	defer predicate.Release()
	view, _ := predicate.BorrowFixedWidth()
	for row := 0; row < in.Cardinality; row++ {
		if row&255 == 0 {
			if err := ctx.Err(); err != nil {
				return nil, err
			}
		}
		data, valid := view.Value(row)
		if valid && data[0] != 0 {
			rows = append(rows, row)
		}
	}
	return rows, nil
}
