package join

import (
	"context"
	"strconv"
	"sync"

	"github.com/steamwo/fssql/internal/core/memory"
	"github.com/steamwo/fssql/internal/core/types"
	"github.com/steamwo/fssql/internal/core/vector"
	exechash "github.com/steamwo/fssql/internal/execution/hash"
	"github.com/steamwo/fssql/internal/execution/pipeline"
)

type joinOutput struct {
	chunk    vector.DataChunk
	capacity int
}

// outputPool owns query-local buffers, including their managed reservations.
// Concurrent probe calls lease separate outputs. Close frees idle buffers;
// outstanding leases are freed when returned, never while being consumed.
type outputPool struct {
	mu       sync.Mutex
	free     []*joinOutput
	emitters []*batchEmitter
	closed   bool
}

func (p *outputPool) acquire(scope *memory.Scope, bm *memory.BufferManager, typ Type, left, right []types.LogicalType, needed []bool, count int) (*joinOutput, error) {
	p.mu.Lock()
	if p.closed {
		p.mu.Unlock()
		return nil, memory.ErrClosed
	}
	var out *joinOutput
	if n := len(p.free); n > 0 {
		out = p.free[n-1]
		p.free[n-1] = nil
		p.free = p.free[:n-1]
	}
	p.mu.Unlock()
	if out != nil && out.capacity < count {
		out.chunk.Release()
		out = nil
	}
	if out == nil {
		ts := left
		if typ != Semi && typ != Anti {
			ts = append(append(make([]types.LogicalType, 0, len(left)+len(right)), left...), right...)
		}
		cols, err := newOutputColumns(scope, bm, ts, needed, count)
		if err != nil {
			return nil, err
		}
		out = &joinOutput{chunk: vector.DataChunk{Columns: cols}, capacity: count}
	}
	return out, nil
}

// Match selection arrays can escape through the optional aggregate interface.
// Lease them with the emitter rather than allocating 64 KiB on every probe.
func (p *outputPool) emitter(scope *memory.Scope, bm *memory.BufferManager, typ Type, left, right []types.LogicalType, side Side, layout exechash.RowLayout, needed []bool, sink pipeline.Sink) (*batchEmitter, error) {
	p.mu.Lock()
	if p.closed {
		p.mu.Unlock()
		return nil, memory.ErrClosed
	}
	var e *batchEmitter
	if n := len(p.emitters); n > 0 {
		e = p.emitters[n-1]
		p.emitters[n-1] = nil
		p.emitters = p.emitters[:n-1]
	}
	p.mu.Unlock()
	if e == nil {
		reservation, err := scope.Reserve(memory.TagMetadata, int64(4*(strconv.IntSize/8)*vector.DefaultVectorSize))
		if err != nil {
			return nil, err
		}
		e = &batchEmitter{scratchReservation: reservation}
	}
	e.typ, e.scope, e.bm, e.leftTypes, e.rightTypes = typ, scope, bm, left, right
	e.buildSide, e.buildLayout, e.required, e.downstream, e.pool = side, layout, needed, sink, p
	e.stopped = false
	return e, nil
}

func (p *outputPool) releaseEmitter(e *batchEmitter) {
	e.abort()
	e.downstream = nil
	p.mu.Lock()
	defer p.mu.Unlock()
	if p.closed {
		e.scratchReservation.Release()
		return
	}
	p.emitters = append(p.emitters, e)
}

func (p *outputPool) put(out *joinOutput) {
	if out == nil {
		return
	}
	p.mu.Lock()
	defer p.mu.Unlock()
	if p.closed {
		out.chunk.Release()
		return
	}
	p.free = append(p.free, out)
}

func (p *outputPool) close() {
	p.mu.Lock()
	defer p.mu.Unlock()
	p.closed = true
	for _, out := range p.free {
		out.chunk.Release()
	}
	for _, e := range p.emitters {
		e.scratchReservation.Release()
	}
	p.free = nil
	p.emitters = nil
}

// consumeOutput retains buffers only across borrowed consumption. Ownership
// aware sinks keep their existing move/transfer path, including transforms that
// can move input columns even when their terminal sink borrows its input.
func consumeOutput(ctx context.Context, sink pipeline.Sink, out *joinOutput, count int) (transferred bool, err error) {
	out.chunk.Cardinality = count
	_, moves := sink.(pipeline.OwnedInputSink)
	if moves || pipeline.SinkTakesChunkOwnership(sink) {
		return true, pipeline.ConsumeOwnedChunk(ctx, sink, &out.chunk)
	}
	return false, sink.Consume(ctx, &out.chunk)
}
