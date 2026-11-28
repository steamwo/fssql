package join

import (
	"context"
	"errors"

	"github.com/steamwo/fssql/internal/core/memory"
	"github.com/steamwo/fssql/internal/core/types"
	"github.com/steamwo/fssql/internal/core/vector"
	exechash "github.com/steamwo/fssql/internal/execution/hash"
	"github.com/steamwo/fssql/internal/execution/pipeline"
)

// batchEmitter 按列收集一个 probe 批次的输出，固定选择数组由调用栈持有。
// probe 值借用输入 chunk，build 值借用驻留分区，flush 后清除引用。
type batchEmitter struct {
	typ         Type
	scope       *memory.Scope
	bm          *memory.BufferManager
	leftTypes   []types.LogicalType
	rightTypes  []types.LogicalType
	buildSide   Side
	buildLayout exechash.RowLayout
	downstream  pipeline.Sink

	required           []bool
	pool               *outputPool
	output             *joinOutput
	scratchReservation *memory.Reservation
	comparer           exechash.ChunkKeyComparer
	cols               []*vector.Vector
	probeChunk         *vector.DataChunk
	probeRows          [vector.DefaultVectorSize]int    // probeRows 保存当前批次的输入行号。
	buildRows          [vector.DefaultVectorSize][]byte // buildRows 借用匹配的构建行。
	count              int
	stopped            bool
}

// newBatchEmitter creates an unpooled emitter for kernel benchmarks. Production
// sinks lease emitters and their match-selection scratch from the query pool.
func newBatchEmitter(typ Type, scope *memory.Scope, bm *memory.BufferManager, leftTypes, rightTypes []types.LogicalType, buildSide Side, buildLayout exechash.RowLayout, downstream pipeline.Sink) batchEmitter {
	return batchEmitter{
		typ: typ, scope: scope, bm: bm, leftTypes: leftTypes, rightTypes: rightTypes,
		buildSide: buildSide, buildLayout: buildLayout, downstream: downstream,
	}
}

func (e *batchEmitter) isSemiOrAnti() bool { return e.typ == Semi || e.typ == Anti }

func (e *batchEmitter) probeTypes() []types.LogicalType {
	if e.buildSide == RightSide {
		return e.leftTypes
	}
	return e.rightTypes
}

func (e *batchEmitter) outputTypes() []types.LogicalType {
	if e.isSemiOrAnti() {
		return e.leftTypes
	}
	out := make([]types.LogicalType, 0, len(e.leftTypes)+len(e.rightTypes))
	out = append(out, e.leftTypes...)
	out = append(out, e.rightTypes...)
	return out
}

func (e *batchEmitter) probeStart() int {
	if e.isSemiOrAnti() || e.buildSide == RightSide {
		return 0
	}
	return len(e.leftTypes)
}

func (e *batchEmitter) buildStart() int {
	if e.buildSide == RightSide {
		return len(e.leftTypes)
	}
	return 0
}

func (e *batchEmitter) ensure() error {
	if e.output != nil && e.output.capacity < e.count {
		e.output.chunk.Release()
		e.output, e.cols = nil, nil
	}
	if e.cols != nil {
		for _, col := range e.cols {
			col.ResetStringHeap()
		}
		return nil
	}
	if e.pool != nil {
		out, err := e.pool.acquire(e.scope, e.bm, e.typ, e.leftTypes, e.rightTypes, e.required, e.count)
		if err != nil {
			return err
		}
		e.output, e.cols = out, out.chunk.Columns
		for _, col := range e.cols {
			col.ResetStringHeap()
		}
		return nil
	}
	cols, err := newOutputColumns(e.scope, e.bm, e.outputTypes(), e.required, e.count)
	if err != nil {
		return err
	}
	e.cols = cols
	return nil
}

func (e *batchEmitter) beginChunk(ctx context.Context, chunk *vector.DataChunk) error {
	if e.probeChunk != nil && e.probeChunk != chunk {
		return e.flush(ctx)
	}
	e.probeChunk = chunk
	return nil
}

// appendPair 批量收集输出，最多每 256 行检查一次取消，避免逐行加锁读取 context。
func (e *batchEmitter) appendPair(ctx context.Context, chunk *vector.DataChunk, probeRow int, buildRow []byte) error {
	if e.stopped {
		return nil
	}
	if e.count&255 == 0 {
		if err := ctx.Err(); err != nil {
			return err
		}
	}
	if err := e.beginChunk(ctx, chunk); err != nil {
		return err
	}
	if e.count == vector.DefaultVectorSize {
		if err := e.flush(ctx); err != nil {
			return err
		}
	}
	if e.stopped {
		return nil
	}
	e.probeChunk = chunk
	e.probeRows[e.count] = probeRow
	e.buildRows[e.count] = buildRow
	e.count++
	return nil
}

func (e *batchEmitter) appendProbeOnly(ctx context.Context, chunk *vector.DataChunk, probeRow int) error {
	return e.appendPair(ctx, chunk, probeRow, nil)
}

func (e *batchEmitter) appendBuildOnly(ctx context.Context, buildRow []byte) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	if e.probeChunk != nil {
		if err := e.flush(ctx); err != nil {
			return err
		}
	}
	if e.count == vector.DefaultVectorSize {
		if err := e.flush(ctx); err != nil {
			return err
		}
	}
	e.probeRows[e.count] = -1
	e.buildRows[e.count] = buildRow
	e.count++
	return nil
}

func (e *batchEmitter) flush(ctx context.Context) error {
	if e.count == 0 {
		return nil
	}
	if consumer, ok := e.downstream.(pipeline.JoinBatchConsumer); ok {
		accepted, err := consumer.ConsumeJoinBatch(ctx, pipeline.JoinBatch{
			Probe: e.probeChunk, ProbeRows: e.probeRows[:e.count], BuildRows: e.buildRows[:e.count],
			BuildLayout: e.buildLayout, ProbeStart: e.probeStart(), ProbeWidth: len(e.probeTypes()), BuildStart: e.buildStart(),
		})
		if accepted || err != nil {
			clear(e.buildRows[:e.count])
			e.probeChunk, e.count = nil, 0
			e.stopped = err == nil && pipeline.SinkFinished(e.downstream)
			return err
		}
	}
	if err := e.ensure(); err != nil {
		return err
	}
	probeTypes := e.probeTypes()
	probeStart := e.probeStart()
	if e.probeChunk != nil {
		if len(e.probeChunk.Columns) < len(probeTypes) {
			return errors.New("join probe chunk column count mismatch")
		}
		for i := range probeTypes {
			if e.required != nil && !e.required[probeStart+i] {
				continue
			}
			if err := vector.CopySelectionInto(e.cols[probeStart+i], 0, e.probeChunk.Columns[i], e.probeRows[:e.count]); err != nil {
				return err
			}
		}
	} else {
		for i := range probeTypes {
			if e.required != nil && !e.required[probeStart+i] {
				continue
			}
			for row := 0; row < e.count; row++ {
				e.cols[probeStart+i].SetNull(row)
			}
		}
	}
	if !e.isSemiOrAnti() {
		buildStart := e.buildStart()
		buildCols := e.cols[buildStart : buildStart+len(e.buildLayout.Columns)]
		if err := e.buildLayout.MaterializeSelectedRows(e.buildRows[:e.count], buildCols, neededColumns(e.required, buildStart, len(buildCols))); err != nil {
			return err
		}
	}
	var err error
	if e.output != nil {
		var transferred bool
		transferred, err = consumeOutput(ctx, e.downstream, e.output, e.count)
		if transferred {
			e.output, e.cols = nil, nil
		}
	} else {
		chunk := &vector.DataChunk{Columns: e.cols, Cardinality: e.count}
		err = pipeline.ConsumeOwnedChunk(ctx, e.downstream, chunk)
		e.cols = nil
	}
	for i := 0; i < e.count; i++ {
		e.buildRows[i] = nil
	}
	e.probeChunk = nil
	e.count = 0
	e.stopped = err == nil && pipeline.SinkFinished(e.downstream)
	return err
}

func (e *batchEmitter) abort() {
	e.comparer.Clear()
	if e.output != nil {
		e.pool.put(e.output)
		e.output = nil
	} else {
		for _, v := range e.cols {
			if v != nil {
				v.Release()
			}
		}
	}
	clear(e.buildRows[:e.count])
	e.cols = nil
	e.probeChunk = nil
	e.count = 0
}
