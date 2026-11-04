package join

import (
	"context"
	"encoding/binary"
	"errors"
	"fmt"
	"math"
	"sync"

	"github.com/steamwo/fssql/internal/core/memory"
	"github.com/steamwo/fssql/internal/core/types"
	"github.com/steamwo/fssql/internal/core/vector"
	exechash "github.com/steamwo/fssql/internal/execution/hash"
	"github.com/steamwo/fssql/internal/execution/pipeline"
)

type Type uint8

const (
	Inner Type = iota
	Left
	Right
	Full
	Cross
	Semi
	Anti
)

type Config struct {
	Type       Type
	LeftTypes  []types.LogicalType
	RightTypes []types.LogicalType
	LeftKeys   []int
	RightKeys  []int
	Scope      *memory.Scope
	Buffers    *memory.BufferManager
}

const rowHashSlotWidth = 24

type rowHashTable struct {
	slots     *memory.Buffer
	next      *memory.Buffer
	slotsData []byte
	nextData  []byte
	capacity  int
}

func newRowHashTable(scope *memory.Scope, bm *memory.BufferManager, rows int) (*rowHashTable, error) {
	if rows < 0 || uint64(rows) > uint64(math.MaxUint32-1) {
		return nil, errors.New("join build row count exceeds uint32 range")
	}
	capacity := 8
	for capacity < rows*2 {
		if capacity > math.MaxInt/2 {
			return nil, errors.New("join hash table capacity overflow")
		}
		capacity <<= 1
	}
	slots, err := bm.Allocate(scope, memory.TagHashTable, capacity*rowHashSlotWidth)
	if err != nil {
		return nil, err
	}
	nextBytes := rows * 4
	if nextBytes < 4 {
		nextBytes = 4
	}
	next, err := bm.Allocate(scope, memory.TagHashTable, nextBytes)
	if err != nil {
		slots.Release()
		return nil, err
	}
	return &rowHashTable{slots: slots, next: next, slotsData: slots.Bytes(), nextData: next.Bytes(), capacity: capacity}, nil
}

// slot and Next borrow the immutable post-build buffers. Neither backing
// buffer is resized after construction, so the cached views stay valid until
// Release.
func (h *rowHashTable) slot(i int) []byte {
	return h.slotsData[i*rowHashSlotWidth : (i+1)*rowHashSlotWidth]
}
func (h *rowHashTable) Insert(hash uint64, row int) error {
	mask := h.capacity - 1
	for probe := 0; probe < h.capacity; probe++ {
		idx := (int(hash) + probe) & mask
		s := h.slot(idx)
		if s[0] == 0 {
			s[0] = 1
			binary.LittleEndian.PutUint64(s[8:16], hash)
			binary.LittleEndian.PutUint32(s[16:20], uint32(row+1))
			return nil
		}
		if binary.LittleEndian.Uint64(s[8:16]) == hash {
			head := binary.LittleEndian.Uint32(s[16:20])
			binary.LittleEndian.PutUint32(h.nextData[row*4:row*4+4], head)
			binary.LittleEndian.PutUint32(s[16:20], uint32(row+1))
			return nil
		}
	}
	return errors.New("join hash table full")
}
func (h *rowHashTable) Head(hash uint64) (int, bool) {
	mask := h.capacity - 1
	for probe := 0; probe < h.capacity; probe++ {
		idx := (int(hash) + probe) & mask
		s := h.slot(idx)
		if s[0] == 0 {
			return 0, false
		}
		if binary.LittleEndian.Uint64(s[8:16]) == hash {
			return int(binary.LittleEndian.Uint32(s[16:20])) - 1, true
		}
	}
	return 0, false
}
func (h *rowHashTable) Next(row int) (int, bool) {
	v := binary.LittleEndian.Uint32(h.nextData[row*4 : row*4+4])
	if v == 0 {
		return 0, false
	}
	return int(v) - 1, true
}
func (h *rowHashTable) Release() {
	if h.slots != nil {
		h.slots.Release()
		h.slots = nil
	}
	if h.next != nil {
		h.next.Release()
		h.next = nil
	}
	h.slotsData, h.nextData = nil, nil
}

type HashJoin struct {
	mu                            sync.Mutex
	typ                           Type
	scope                         *memory.Scope
	bm                            *memory.BufferManager
	leftTypes, rightTypes         []types.LogicalType
	leftKeys, rightKeys           []int
	leftKeyLayout, rightKeyLayout exechash.RowLayout
	rightLayout                   exechash.RowLayout
	rightRows                     *exechash.TupleDataCollection
	rightKeyRows                  *exechash.TupleDataCollection
	index                         *rowHashTable
	matched                       *memory.Buffer
	built                         bool
	failed                        error
	closed                        bool
}

func sameType(a, b types.LogicalType) bool { return a.ID == b.ID && a.Decimal == b.Decimal }

func New(config Config) (*HashJoin, error) {
	if config.Scope == nil || config.Buffers == nil {
		return nil, errors.New("join requires memory scope and buffer manager")
	}
	if config.Type > Anti {
		return nil, errors.New("invalid join type")
	}
	rightLayout, err := exechash.NewRelocatableRowLayout(config.RightTypes)
	if err != nil {
		return nil, fmt.Errorf("right join row layout: %w", err)
	}
	if _, err := exechash.NewRelocatableRowLayout(config.LeftTypes); err != nil {
		return nil, fmt.Errorf("left join row layout: %w", err)
	}
	if config.Type != Cross {
		if len(config.LeftKeys) == 0 || len(config.LeftKeys) != len(config.RightKeys) {
			return nil, errors.New("hash join requires matching non-empty key lists")
		}
	}
	leftKeyTypes := make([]types.LogicalType, len(config.LeftKeys))
	rightKeyTypes := make([]types.LogicalType, len(config.RightKeys))
	for i := range config.LeftKeys {
		lk, rk := config.LeftKeys[i], config.RightKeys[i]
		if lk < 0 || lk >= len(config.LeftTypes) || rk < 0 || rk >= len(config.RightTypes) {
			return nil, errors.New("join key index out of range")
		}
		if !sameType(config.LeftTypes[lk], config.RightTypes[rk]) {
			return nil, errors.New("join key type mismatch")
		}
		leftKeyTypes[i] = config.LeftTypes[lk]
		rightKeyTypes[i] = config.RightTypes[rk]
	}
	leftKeyLayout, err := exechash.NewRelocatableRowLayout(leftKeyTypes)
	if err != nil {
		return nil, err
	}
	rightKeyLayout, err := exechash.NewRelocatableRowLayout(rightKeyTypes)
	if err != nil {
		return nil, err
	}
	rightRows, err := exechash.NewTupleDataCollectionForLayout(config.Scope, config.Buffers, rightLayout, 1)
	if err != nil {
		return nil, err
	}
	var rightKeyRows *exechash.TupleDataCollection
	if config.Type != Cross {
		rightKeyRows, err = exechash.NewTupleDataCollectionForLayout(config.Scope, config.Buffers, rightKeyLayout, 1)
		if err != nil {
			rightRows.Release()
			return nil, err
		}
	}
	return &HashJoin{typ: config.Type, scope: config.Scope, bm: config.Buffers, leftTypes: append([]types.LogicalType(nil), config.LeftTypes...), rightTypes: append([]types.LogicalType(nil), config.RightTypes...), leftKeys: append([]int(nil), config.LeftKeys...), rightKeys: append([]int(nil), config.RightKeys...), leftKeyLayout: leftKeyLayout, rightKeyLayout: rightKeyLayout, rightLayout: rightLayout, rightRows: rightRows, rightKeyRows: rightKeyRows}, nil
}

func (j *HashJoin) releaseLocked() {
	if j.index != nil {
		j.index.Release()
		j.index = nil
	}
	if j.matched != nil {
		j.matched.Release()
		j.matched = nil
	}
	if j.rightKeyRows != nil {
		j.rightKeyRows.Release()
		j.rightKeyRows = nil
	}
	if j.rightRows != nil {
		j.rightRows.Release()
		j.rightRows = nil
	}
	j.closed = true
}
func (j *HashJoin) failLocked(err error) error {
	if j.failed == nil {
		j.failed = err
	}
	j.releaseLocked()
	return err
}
func (j *HashJoin) Close() {
	j.mu.Lock()
	defer j.mu.Unlock()
	j.releaseLocked()
}
func (j *HashJoin) BuildSink() pipeline.Sink {
	return &buildSink{join: j, keyCols: make([]*vector.Vector, len(j.rightKeys))}
}
func (j *HashJoin) ProbeSink(downstream pipeline.Sink) pipeline.Sink {
	return &probeSink{join: j, downstream: downstream, keyCols: make([]*vector.Vector, len(j.leftKeys))}
}

type buildSink struct {
	join    *HashJoin
	keyCols []*vector.Vector
}

func (s *buildSink) Consume(ctx context.Context, chunk *vector.DataChunk) error {
	j := s.join
	if err := ctx.Err(); err != nil {
		j.mu.Lock()
		defer j.mu.Unlock()
		return j.failLocked(err)
	}
	j.mu.Lock()
	defer j.mu.Unlock()
	if j.failed != nil {
		return j.failed
	}
	if j.closed {
		return errors.New("join closed")
	}
	if j.built {
		return errors.New("join build already finalized")
	}
	if len(chunk.Columns) != len(j.rightTypes) {
		return j.failLocked(errors.New("right chunk column count mismatch"))
	}
	var keyChunk vector.DataChunk
	if j.typ != Cross {
		keyChunk.Columns = s.keyCols
		keyChunk.Cardinality = chunk.Cardinality
		for i, col := range j.rightKeys {
			s.keyCols[i] = chunk.Columns[col]
		}
	}
	for row := 0; row < chunk.Cardinality; row++ {
		before := j.rightRows.Count()
		if err := j.rightRows.AppendChunkRow(chunk, row); err != nil {
			return j.failLocked(err)
		}
		if j.typ != Cross {
			if err := j.rightKeyRows.AppendChunkRow(&keyChunk, row); err != nil {
				_ = j.rightRows.Truncate(before)
				return j.failLocked(err)
			}
		}
	}
	return nil
}
func (s *buildSink) Finalize(ctx context.Context) error {
	j := s.join
	if err := ctx.Err(); err != nil {
		j.mu.Lock()
		defer j.mu.Unlock()
		return j.failLocked(err)
	}
	j.mu.Lock()
	defer j.mu.Unlock()
	if j.failed != nil {
		return j.failed
	}
	if j.closed {
		return errors.New("join closed")
	}
	if j.built {
		return nil
	}
	if j.typ != Cross {
		index, err := newRowHashTable(j.scope, j.bm, j.rightRows.Count())
		if err != nil {
			return j.failLocked(err)
		}
		for row := 0; row < j.rightKeyRows.Count(); row++ {
			h, err := j.rightKeyLayout.HashRow(j.rightKeyRows.Row(row))
			if err != nil {
				index.Release()
				return j.failLocked(err)
			}
			if err := index.Insert(h, row); err != nil {
				index.Release()
				return j.failLocked(err)
			}
		}
		j.index = index
	}
	if (j.typ == Right || j.typ == Full) && j.rightRows.Count() > 0 {
		matched, err := j.bm.Allocate(j.scope, memory.TagHashTable, (j.rightRows.Count()+7)/8)
		if err != nil {
			return j.failLocked(err)
		}
		j.matched = matched
	}
	j.built = true
	return nil
}

type probeSink struct {
	join       *HashJoin
	downstream pipeline.Sink
	keyCols    []*vector.Vector
}

func (j *HashJoin) markMatched(row int) {
	if j.matched == nil {
		return
	}
	b := j.matched.Bytes()
	b[row>>3] |= byte(1 << uint(row&7))
}
func (j *HashJoin) isMatched(row int) bool {
	if j.matched == nil {
		return false
	}
	return j.matched.Bytes()[row>>3]&(byte(1)<<uint(row&7)) != 0
}

func (s *probeSink) Consume(ctx context.Context, chunk *vector.DataChunk) error {
	j := s.join
	if err := ctx.Err(); err != nil {
		j.mu.Lock()
		defer j.mu.Unlock()
		return j.failLocked(err)
	}
	j.mu.Lock()
	defer j.mu.Unlock()
	if j.failed != nil {
		return j.failed
	}
	if j.closed {
		return errors.New("join closed")
	}
	if !j.built {
		return j.failLocked(errors.New("join build not finalized"))
	}
	if len(chunk.Columns) != len(j.leftTypes) {
		return j.failLocked(errors.New("left chunk column count mismatch"))
	}
	em := newEmitter(j, s.downstream)
	var keyRows *exechash.TupleDataCollection
	if j.typ != Cross {
		var err error
		keyRows, err = exechash.NewTupleDataCollectionForLayout(j.scope, j.bm, j.leftKeyLayout, chunk.Cardinality)
		if err != nil {
			return j.failLocked(err)
		}
		keyChunk := vector.DataChunk{Columns: s.keyCols, Cardinality: chunk.Cardinality}
		for i, col := range j.leftKeys {
			s.keyCols[i] = chunk.Columns[col]
		}
		for row := 0; row < chunk.Cardinality; row++ {
			if err := keyRows.AppendChunkRow(&keyChunk, row); err != nil {
				keyRows.Release()
				em.abort()
				return j.failLocked(err)
			}
		}
	}
	fail := func(err error) error {
		if keyRows != nil {
			keyRows.Release()
		}
		em.abort()
		return j.failLocked(err)
	}
	for leftRow := 0; leftRow < chunk.Cardinality; leftRow++ {
		if err := ctx.Err(); err != nil {
			return fail(err)
		}
		matched := false
		if j.typ == Cross {
			for rightRow := 0; rightRow < j.rightRows.Count(); rightRow++ {
				if err := em.append(ctx, chunk, leftRow, j.rightRows.Row(rightRow), true, true); err != nil {
					return fail(err)
				}
			}
			continue
		}
		keyRow := keyRows.Row(leftRow)
		h, err := j.leftKeyLayout.HashRow(keyRow)
		if err != nil {
			return fail(err)
		}
		rightRow, ok := j.index.Head(h)
		for ok {
			equal, err := j.leftKeyLayout.EqualRows(keyRow, j.rightKeyRows.Row(rightRow), exechash.NullsUnequal)
			if err != nil {
				return fail(err)
			}
			if equal {
				matched = true
				j.markMatched(rightRow)
				if j.typ == Semi {
					if err := em.append(ctx, chunk, leftRow, nil, true, false); err != nil {
						return fail(err)
					}
					break
				}
				if j.typ != Anti {
					if err := em.append(ctx, chunk, leftRow, j.rightRows.Row(rightRow), true, true); err != nil {
						return fail(err)
					}
				}
			}
			rightRow, ok = j.index.Next(rightRow)
		}
		if !matched && (j.typ == Left || j.typ == Full || j.typ == Anti) {
			if err := em.append(ctx, chunk, leftRow, nil, true, false); err != nil {
				return fail(err)
			}
		}
	}
	if keyRows != nil {
		keyRows.Release()
	}
	if err := em.flush(ctx); err != nil {
		em.abort()
		return j.failLocked(err)
	}
	return nil
}

func (s *probeSink) Finalize(ctx context.Context) error {
	j := s.join
	if err := ctx.Err(); err != nil {
		j.mu.Lock()
		defer j.mu.Unlock()
		return j.failLocked(err)
	}
	j.mu.Lock()
	defer j.mu.Unlock()
	if j.failed != nil {
		return j.failed
	}
	if j.closed {
		return errors.New("join closed")
	}
	if !j.built {
		return j.failLocked(errors.New("join build not finalized"))
	}
	em := newEmitter(j, s.downstream)
	if j.typ == Right || j.typ == Full {
		for row := 0; row < j.rightRows.Count(); row++ {
			if !j.isMatched(row) {
				if err := em.append(ctx, nil, 0, j.rightRows.Row(row), false, true); err != nil {
					em.abort()
					return j.failLocked(err)
				}
			}
		}
	}
	if err := em.flush(ctx); err != nil {
		em.abort()
		return j.failLocked(err)
	}
	err := s.downstream.Finalize(ctx)
	j.releaseLocked()
	return err
}

type emitter struct {
	join       *HashJoin
	downstream pipeline.Sink
	cols       []*vector.Vector
	count      int
}

func newEmitter(j *HashJoin, downstream pipeline.Sink) *emitter {
	return &emitter{join: j, downstream: downstream}
}
func (e *emitter) outputTypes() []types.LogicalType {
	if e.join.typ == Semi || e.join.typ == Anti {
		return e.join.leftTypes
	}
	out := make([]types.LogicalType, 0, len(e.join.leftTypes)+len(e.join.rightTypes))
	out = append(out, e.join.leftTypes...)
	out = append(out, e.join.rightTypes...)
	return out
}
func (e *emitter) ensure() error {
	if e.cols != nil {
		return nil
	}
	ts := e.outputTypes()
	e.cols = make([]*vector.Vector, len(ts))
	for i, t := range ts {
		v, err := vector.NewFlat(e.join.scope, e.join.bm, t, vector.DefaultVectorSize)
		if err != nil {
			for _, owned := range e.cols {
				if owned != nil {
					owned.Release()
				}
			}
			e.cols = nil
			return err
		}
		e.cols[i] = v
	}
	return nil
}
func (e *emitter) append(ctx context.Context, left *vector.DataChunk, leftRow int, rightRow []byte, leftPresent, rightPresent bool) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	if err := e.ensure(); err != nil {
		return err
	}
	outRow := e.count
	for i := range e.join.leftTypes {
		if leftPresent {
			if err := vector.CopyValue(left.Columns[i], leftRow, e.cols[i], outRow); err != nil {
				return err
			}
		} else {
			e.cols[i].SetNull(outRow)
		}
	}
	if e.join.typ != Semi && e.join.typ != Anti {
		rightCols := e.cols[len(e.join.leftTypes):]
		if rightPresent {
			if err := e.join.rightLayout.MaterializeRow(rightRow, rightCols, outRow); err != nil {
				return err
			}
		} else {
			for _, v := range rightCols {
				v.SetNull(outRow)
			}
		}
	}
	e.count++
	if e.count == vector.DefaultVectorSize {
		return e.flush(ctx)
	}
	return nil
}
func (e *emitter) flush(ctx context.Context) error {
	if e.count == 0 {
		return nil
	}
	chunk := &vector.DataChunk{Columns: e.cols, Cardinality: e.count}
	err := e.downstream.Consume(ctx, chunk)
	for _, v := range e.cols {
		v.Release()
	}
	e.cols = nil
	e.count = 0
	return err
}
func (e *emitter) abort() {
	for _, v := range e.cols {
		if v != nil {
			v.Release()
		}
	}
	e.cols = nil
	e.count = 0
}
