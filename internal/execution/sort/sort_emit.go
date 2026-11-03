package sort

import (
	"context"
	"errors"

	"github.com/steamwo/fssql/internal/core/vector"
)

func (s *ExternalSort) emitRows(ctx context.Context, next func(func([]byte) error) error, emit func(*vector.DataChunk) error) error {
	if emit == nil {
		return errors.New("sort emit callback required")
	}
	var cols []*vector.Vector
	count := 0
	release := func() {
		for _, v := range cols {
			if v != nil {
				v.Release()
			}
		}
		cols = nil
	}
	defer func() { release() }()
	alloc := func() error {
		cols = make([]*vector.Vector, len(s.cfg.InputTypes))
		for i, t := range s.cfg.InputTypes {
			v, err := vector.NewFlat(s.cfg.Scope, s.cfg.Buffers, t, vector.DefaultVectorSize)
			if err != nil {
				return err
			}
			cols[i] = v
		}
		return nil
	}
	flush := func() error {
		if count == 0 {
			return nil
		}
		// Full batches already have the correct cardinality. Transfer them
		// directly; only the final partial batch requires a compact copy.
		output := cols
		if count != vector.DefaultVectorSize {
			output = make([]*vector.Vector, len(cols))
			for ci, t := range s.cfg.InputTypes {
				v, err := vector.NewFlat(s.cfg.Scope, s.cfg.Buffers, t, count)
				if err == nil {
					output[ci] = v
					for r := 0; r < count; r++ {
						if err = vector.CopyValue(cols[ci], r, v, r); err != nil {
							break
						}
					}
				}
				if err != nil {
					for _, x := range output {
						if x != nil {
							x.Release()
						}
					}
					return err
				}
			}
			release()
		}
		cols = nil
		chunk, err := vector.NewDataChunk(output...)
		if err != nil {
			for _, v := range output {
				v.Release()
			}
			return err
		}
		count = 0
		if err := emit(chunk); err != nil {
			chunk.Release()
			return err
		}
		return nil
	}
	err := next(func(row []byte) error {
		if err := ctx.Err(); err != nil {
			return err
		}
		if cols == nil {
			if err := alloc(); err != nil {
				return err
			}
		}
		if err := s.layout.MaterializeRow(row, cols, count); err != nil {
			return err
		}
		count++
		if count == vector.DefaultVectorSize {
			return flush()
		}
		return nil
	})
	if err == nil {
		err = flush()
	}
	return err
}

func(s *ExternalSort)Emit(ctx context.Context,emit func(*vector.DataChunk)error)error{if s.finalized{return errors.New("sort finalized")};if err:=ctx.Err();err!=nil{return err};if len(s.runs)==0{idx,err:=s.sortedIndices();if err!=nil{return err};err=s.emitRows(ctx,func(consume func([]byte)error)error{for i:=0;i<idx.n;i++{if err:=consume(s.builder.Row(idx.get(i)));err!=nil{return err}};return nil},emit);idx.release();if err!=nil{return err};s.finalized=true;return nil};if s.builder.Count()>0{if err:=s.spillBuilder(ctx);err!=nil{return err}};if err:=s.compactRuns(ctx);err!=nil{return err};err:=s.emitRows(ctx,func(consume func([]byte)error)error{return s.mergeRuns(ctx,s.runs,consume)},emit);if err!=nil{return err};for _,r:=range s.runs{if err:=s.deleteRun(r);err!=nil{return err}};s.runs=nil;_ = s.runMeta.Resize(0);s.finalized=true;return nil}
func(s *ExternalSort)Stats()Stats{return s.stats}
func(s *ExternalSort)Release(){if s.builder!=nil{s.builder.Release();s.builder=nil};for _,r:=range s.runs{if r!=nil&&r.pages!=nil{r.pages.Release();r.pages=nil}};s.runs=nil;if s.runMeta!=nil{s.runMeta.Release();s.runMeta=nil};if s.store!=nil{_ = s.store.Close();s.store=nil}}
