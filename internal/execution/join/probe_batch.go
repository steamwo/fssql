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

// batchEmitter buffers probe row selections for one probe chunk and gathers the
// output columns column-by-column on flush. Probe values are borrowed from the
// input chunk; build values are encoded rows owned by the resident partition.
type batchEmitter struct {
	typ         Type
	scope       *memory.Scope
	bm          *memory.BufferManager
	leftTypes   []types.LogicalType
	rightTypes  []types.LogicalType
	buildSide   Side
	buildLayout exechash.RowLayout
	downstream  pipeline.Sink

	required   []bool
	cols       []*vector.Vector
	probeChunk *vector.DataChunk
	probeRows  []int
	buildRows  [][]byte
	count      int
}

func newBatchEmitter(typ Type, scope *memory.Scope, bm *memory.BufferManager, leftTypes, rightTypes []types.LogicalType, buildSide Side, buildLayout exechash.RowLayout, downstream pipeline.Sink) *batchEmitter {
	return &batchEmitter{
		typ: typ, scope: scope, bm: bm, leftTypes: leftTypes, rightTypes: rightTypes,
		buildSide: buildSide, buildLayout: buildLayout, downstream: downstream,
		probeRows: make([]int, vector.DefaultVectorSize),
		buildRows: make([][]byte, vector.DefaultVectorSize),
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
	if e.cols != nil {
		return nil
	}
	cols, err := newOutputColumns(e.scope, e.bm, e.outputTypes(), e.required)
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

func (e *batchEmitter) appendPair(ctx context.Context, chunk *vector.DataChunk, probeRow int, buildRow []byte) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	if err := e.beginChunk(ctx, chunk); err != nil {
		return err
	}
	if e.count == vector.DefaultVectorSize {
		if err := e.flush(ctx); err != nil {
			return err
		}
		e.probeChunk = chunk
	}
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
	chunk := &vector.DataChunk{Columns: e.cols, Cardinality: e.count}
	err := e.downstream.Consume(ctx, chunk)
	for _, v := range e.cols {
		v.Release()
	}
	for i := 0; i < e.count; i++ {
		e.buildRows[i] = nil
	}
	e.cols = nil
	e.probeChunk = nil
	e.count = 0
	return err
}

func (e *batchEmitter) abort() {
	for _, v := range e.cols {
		if v != nil {
			v.Release()
		}
	}
	for i := range e.buildRows {
		e.buildRows[i] = nil
	}
	e.cols = nil
	e.probeChunk = nil
	e.count = 0
}
