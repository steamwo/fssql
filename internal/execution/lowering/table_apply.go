package lowering

import (
	"context"
	"fmt"
	"github.com/steamwo/fssql/internal/core/vector"
	"github.com/steamwo/fssql/internal/execution/expression"
	"github.com/steamwo/fssql/internal/execution/pipeline"
	"github.com/steamwo/fssql/internal/planner"
	"math"
)

type tableApplySink struct {
	executor   *expression.Executor
	plan       *planner.PhysicalPlan
	downstream pipeline.Sink
}

func (s *tableApplySink) Finished() bool {
	if f, ok := s.downstream.(interface{ Finished() bool }); ok {
		return f.Finished()
	}
	return false
}
func (s *tableApplySink) Consume(ctx context.Context, input *vector.DataChunk) error {
	args := make([]*vector.Vector, len(s.plan.Projection))
	defer func() {
		for _, v := range args {
			if v != nil {
				v.Release()
			}
		}
	}()
	for i, e := range s.plan.Projection {
		v, err := s.executor.ExecuteSQL(e, input)
		if err != nil {
			return err
		}
		args[i] = v
	}
	for row := 0; row < input.Cardinality && !s.Finished(); row++ {
		if err := ctx.Err(); err != nil {
			return err
		}
		null := false
		for _, arg := range args {
			null = null || !arg.IsValid(row)
		}
		if null {
			continue
		}
		if s.plan.Scan.Function == "unnest" {
			entry, _ := args[0].ListEntry(row)
			base, _ := args[0].NestedRow(row)
			if len(base.Children()) != 1 {
				return fmt.Errorf("invalid LIST child")
			}
			for start := 0; start < int(entry.Length) && !s.Finished(); {
				if err := ctx.Err(); err != nil {
					return err
				}
				n := int(entry.Length) - start
				if n > vector.DefaultVectorSize {
					n = vector.DefaultVectorSize
				}
				out, err := s.output(input, row, n)
				if err != nil {
					return err
				}
				for i := 0; i < n; i++ {
					if err = vector.CopyValue(base.Children()[0], int(entry.Offset)+start+i, out.Columns[0], i); err != nil {
						out.Release()
						return err
					}
				}
				if err = s.emit(ctx, out); err != nil {
					return err
				}
				start += n
			}
			continue
		}
		start, end, step := int64(0), int64(0), int64(1)
		if len(args) == 1 {
			end, _ = args[0].Int64(row)
		} else {
			start, _ = args[0].Int64(row)
			end, _ = args[1].Int64(row)
			if len(args) == 3 {
				step, _ = args[2].Int64(row)
			}
		}
		if step == 0 {
			return fmt.Errorf("range step cannot be zero")
		}
		inclusive := s.plan.Scan.Function == "generate_series"
		within := func(v int64) bool {
			if step > 0 {
				return v < end || (inclusive && v == end)
			}
			return v > end || (inclusive && v == end)
		}
		for within(start) && !s.Finished() {
			if err := ctx.Err(); err != nil {
				return err
			}
			values := make([]int64, 0, vector.DefaultVectorSize)
			done := false
			for len(values) < vector.DefaultVectorSize && within(start) {
				values = append(values, start)
				if (step > 0 && start > math.MaxInt64-step) || (step < 0 && start < math.MinInt64-step) {
					done = true
					break
				}
				start += step
			}
			out, err := s.output(input, row, len(values))
			if err != nil {
				return err
			}
			for i, v := range values {
				out.Columns[0].SetInt64(i, v)
			}
			if err = s.emit(ctx, out); err != nil {
				return err
			}
			if done {
				break
			}
		}
	}
	return nil
}
func (s *tableApplySink) output(input *vector.DataChunk, row, n int) (*vector.DataChunk, error) {
	cols := make([]*vector.Vector, len(s.plan.Output))
	for i, c := range s.plan.Output {
		v, err := vector.NewFlat(s.executor.Scope, s.executor.Buffers, c.Type, n)
		if err != nil {
			for _, x := range cols {
				if x != nil {
					x.Release()
				}
			}
			return nil, err
		}
		cols[i] = v
	}
	out, err := vector.NewDataChunk(cols...)
	if err != nil {
		for _, x := range cols {
			x.Release()
		}
		return nil, err
	}
	for col := range input.Columns {
		for i := 0; i < n; i++ {
			if err := vector.CopyValue(input.Columns[col], row, out.Columns[col+1], i); err != nil {
				out.Release()
				return nil, err
			}
		}
	}
	return out, nil
}
func (s *tableApplySink) emit(ctx context.Context, out *vector.DataChunk) error {
	err := s.downstream.Consume(ctx, out)
	if err != nil || !pipeline.SinkTakesChunkOwnership(s.downstream) {
		out.Release()
	}
	return err
}
func (s *tableApplySink) Finalize(ctx context.Context) error { return s.downstream.Finalize(ctx) }
func (s *tableApplySink) Close() error {
	if c, ok := s.downstream.(interface{ Close() error }); ok {
		return c.Close()
	}
	return nil
}
