package sort

import (
	"context"
	"errors"

	"github.com/steamwo/fssql/internal/core/memory"
	"github.com/steamwo/fssql/internal/core/vector"
)

var errSortOutputLimit = errors.New("sort output limit reached")

func (s *ExternalSort) emitRows(ctx context.Context, next func(func([]byte) error) error, emit func(*vector.DataChunk) error) error {
	if emit == nil {
		return errors.New("sort emit callback required")
	}
	var cols []*vector.Vector
	count := 0
	produced := 0
	batchSize := vector.DefaultVectorSize
	if s.emitLimit > 0 && s.emitLimit < batchSize {
		batchSize = s.emitLimit
	}
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
		for {
			cols = make([]*vector.Vector, len(s.cfg.InputTypes))
			var allocErr error
			for i, t := range s.cfg.InputTypes {
				v, err := vector.NewFlat(s.cfg.Scope, s.cfg.Buffers, t, batchSize)
				if err != nil {
					allocErr = err
					break
				}
				cols[i] = v
			}
			if allocErr == nil {
				return nil
			}
			release()
			if s.emitLimit == 0 || batchSize == 1 || !errors.Is(allocErr, memory.ErrLimitExceeded) {
				return allocErr
			}
			batchSize = (batchSize + 1) / 2
		}
	}
	flush := func() error {
		if count == 0 {
			return nil
		}
		// Full batches already have the correct cardinality. Transfer them
		// directly; only the final partial batch requires a compact copy.
		output := cols
		if count != batchSize {
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
		if s.emitLimit > 0 && produced == s.emitLimit {
			return errSortOutputLimit
		}
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
		produced++
		if count == batchSize {
			return flush()
		}
		return nil
	})
	if err == nil || errors.Is(err, errSortOutputLimit) {
		err = flush()
	}
	return err
}
func(s *ExternalSort)Emit(ctx context.Context,emit func(*vector.DataChunk)error)error{if s.finalized{return errors.New("sort finalized")};if err:=ctx.Err();err!=nil{return err};if len(s.runs)==0{idx,err:=s.sortedIndices();if err!=nil{return err};err=s.emitRows(ctx,func(consume func([]byte)error)error{for i:=0;i<idx.n;i++{if err:=consume(s.builder.Row(idx.get(i)));err!=nil{return err}};return nil},emit);idx.release();if err!=nil{return err};s.finalized=true;return nil};if s.builder.Count()>0{if err:=s.spillBuilder(ctx);err!=nil{return err}};if err:=s.compactRuns(ctx);err!=nil{return err};err:=s.emitRows(ctx,func(consume func([]byte)error)error{return s.mergeRuns(ctx,s.runs,consume)},emit);if err!=nil{return err};for _,r:=range s.runs{if err:=s.deleteRun(r);err!=nil{return err}};s.runs=nil;_ = s.runMeta.Resize(0);s.finalized=true;return nil}
func(s *ExternalSort)Stats()Stats{return s.stats}
func(s *ExternalSort)Release(){if s.builder!=nil{s.builder.Release();s.builder=nil};for _,r:=range s.runs{if r!=nil&&r.pages!=nil{r.pages.Release();r.pages=nil}};s.runs=nil;if s.runMeta!=nil{s.runMeta.Release();s.runMeta=nil};if s.store!=nil{_ = s.store.Close();s.store=nil}}
