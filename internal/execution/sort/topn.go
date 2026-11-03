package sort

import (
	"context"
	"encoding/binary"
	"errors"
	"fmt"
	"math"

	"github.com/steamwo/fssql/internal/core/memory"
	"github.com/steamwo/fssql/internal/core/types"
	"github.com/steamwo/fssql/internal/core/vector"
	exechash "github.com/steamwo/fssql/internal/execution/hash"
)

type TopNConfig struct {
	InputTypes       []types.LogicalType
	Keys             []Key
	N                int
	MemoryBudget     int64
	Scope            *memory.Scope
	Buffers          *memory.BufferManager
	PreallocateFixed bool // PreallocateFixed reserves all fixed-width row/index state before Consume so runtime cannot fail after discarding non-candidates.
}

type TopN struct {
	cfg      TopNConfig
	layout   exechash.RowLayout
	entries  []*memory.Buffer
	meta     *memory.Reservation
	scratch  *memory.Buffer
	rowBytes int64

	preallocated      []*memory.Buffer
	preallocatedBytes int64
	preallocatedUsed  int
	indices           *managedIndices
	finalized         bool
}

const topNEntryMetadataBytes = int64(32)

func NewTopN(cfg TopNConfig) (*TopN, error) {
	if cfg.Scope == nil || cfg.Buffers == nil {
		return nil, errors.New("TopN requires scope and buffers")
	}
	if cfg.N < 0 {
		return nil, errors.New("TopN N must be non-negative")
	}
	if cfg.MemoryBudget <= 0 {
		return nil, errors.New("TopN memory budget must be positive")
	}
	if len(cfg.Keys) == 0 {
		return nil, errors.New("TopN requires sort keys")
	}
	for _, k := range cfg.Keys {
		if k.Column < 0 || k.Column >= len(cfg.InputTypes) {
			return nil, fmt.Errorf("TopN key %d out of range", k.Column)
		}
	}
	layout, err := exechash.NewRelocatableRowLayout(cfg.InputTypes)
	if err != nil {
		return nil, err
	}
	if int64(cfg.N) > math.MaxInt64/topNEntryMetadataBytes {
		return nil, errors.New("TopN metadata size overflow")
	}
	metaBytes := int64(cfg.N) * topNEntryMetadataBytes
	if metaBytes > cfg.MemoryBudget {
		return nil, memory.ErrLimitExceeded
	}
	meta, err := cfg.Scope.Reserve(memory.TagMetadata, metaBytes)
	if err != nil {
		return nil, err
	}

	scratchBytes := 64
	if cfg.N == 0 {
		scratchBytes = 1
	}
	if cfg.PreallocateFixed {
		if layout.VariableColumns != 0 {
			meta.Release()
			return nil, fmt.Errorf("TopN fixed preallocation requires fixed-width input")
		}
		if layout.FixedWidth > scratchBytes {
			scratchBytes = layout.FixedWidth
		}
		rowBytes, indexBytes, err := fixedTopNReservationBytes(cfg.N, layout.FixedWidth)
		if err != nil {
			meta.Release()
			return nil, err
		}
		if metaBytes > cfg.MemoryBudget-int64(scratchBytes) || rowBytes > cfg.MemoryBudget-metaBytes-int64(scratchBytes) || indexBytes > cfg.MemoryBudget-metaBytes-int64(scratchBytes)-rowBytes {
			meta.Release()
			return nil, memory.ErrLimitExceeded
		}
	}
	if metaBytes+int64(scratchBytes) > cfg.MemoryBudget {
		meta.Release()
		return nil, memory.ErrLimitExceeded
	}
	scratch, err := cfg.Buffers.Allocate(cfg.Scope, memory.TagSort, scratchBytes)
	if err != nil {
		meta.Release()
		return nil, err
	}
	t := &TopN{
		cfg:     cfg,
		layout:  layout,
		entries: make([]*memory.Buffer, 0, minInt(cfg.N, 1024)),
		meta:    meta,
		scratch: scratch,
	}
	if cfg.PreallocateFixed && cfg.N > 0 {
		if err := t.preallocateFixedState(); err != nil {
			t.Release()
			return nil, err
		}
	}
	return t, nil
}

func fixedTopNReservationBytes(n, rowWidth int) (int64, int64, error) {
	if n < 0 || rowWidth < 0 {
		return 0, 0, errors.New("TopN reservation size is negative")
	}
	if n == 0 {
		return 0, 0, nil
	}
	if rowWidth > 0 && int64(n) > math.MaxInt64/int64(rowWidth) {
		return 0, 0, errors.New("TopN row reservation size overflow")
	}
	if int64(n) > math.MaxInt64/4 {
		return 0, 0, errors.New("TopN index reservation size overflow")
	}
	return int64(n) * int64(rowWidth), int64(n) * 4, nil
}

func (t *TopN) preallocateFixedState() error {
	rowBytes, _, err := fixedTopNReservationBytes(t.cfg.N, t.layout.FixedWidth)
	if err != nil {
		return err
	}
	t.preallocated = make([]*memory.Buffer, 0, t.cfg.N)
	for i := 0; i < t.cfg.N; i++ {
		buf, err := t.cfg.Buffers.Allocate(t.cfg.Scope, memory.TagSort, t.layout.FixedWidth)
		if err != nil {
			return err
		}
		t.preallocated = append(t.preallocated, buf)
	}
	t.preallocatedBytes = rowBytes
	idx, err := newManagedIndices(t.cfg.Scope, t.cfg.Buffers, t.cfg.N)
	if err != nil {
		return err
	}
	t.indices = idx
	return nil
}

func minInt(a, b int) int {
	if a < b {
		return a
	}
	return b
}

func (t *TopN) managedBytesWith(delta int64) int64 {
	rows := t.rowBytes
	if t.preallocated != nil {
		rows = t.preallocatedBytes
	}
	indices := int64(0)
	if t.indices != nil && t.indices.buf != nil {
		indices = int64(t.indices.buf.Len())
	}
	return int64(t.scratch.Len()) + t.meta.Size() + rows + indices + delta
}

func (t *TopN) ensureScratch(n int) error {
	if n <= t.scratch.Len() {
		return nil
	}
	delta := int64(n - t.scratch.Len())
	if t.managedBytesWith(delta) > t.cfg.MemoryBudget {
		return memory.ErrLimitExceeded
	}
	return t.scratch.Resize(n, nil)
}

func (t *TopN) worse(i, j int) (bool, error) {
	cmp, err := compareRowsWith(t.layout, t.cfg.Keys, t.entries[i].Bytes(), t.entries[j].Bytes())
	return cmp > 0, err
}

func (t *TopN) up(i int) error {
	for i > 0 {
		p := (i - 1) / 2
		w, err := t.worse(i, p)
		if err != nil {
			return err
		}
		if !w {
			return nil
		}
		t.entries[i], t.entries[p] = t.entries[p], t.entries[i]
		i = p
	}
	return nil
}

func (t *TopN) down(i int) error {
	n := len(t.entries)
	for {
		l := 2*i + 1
		if l >= n {
			return nil
		}
		best := l
		if l+1 < n {
			w, err := t.worse(l+1, l)
			if err != nil {
				return err
			}
			if w {
				best = l + 1
			}
		}
		w, err := t.worse(best, i)
		if err != nil {
			return err
		}
		if !w {
			return nil
		}
		t.entries[i], t.entries[best] = t.entries[best], t.entries[i]
		i = best
	}
}

func (t *TopN) copyCandidate(row []byte) (*memory.Buffer, error) {
	if t.preallocated != nil {
		if t.preallocatedUsed >= len(t.preallocated) {
			return nil, errors.New("TopN fixed preallocation exhausted")
		}
		buf := t.preallocated[t.preallocatedUsed]
		t.preallocatedUsed++
		if len(row) != buf.Len() {
			return nil, errors.New("TopN fixed preallocation row width changed")
		}
		copy(buf.Bytes(), row)
		t.rowBytes += int64(len(row))
		return buf, nil
	}
	if t.managedBytesWith(int64(len(row))) > t.cfg.MemoryBudget {
		return nil, memory.ErrLimitExceeded
	}
	buf, err := t.cfg.Buffers.Allocate(t.cfg.Scope, memory.TagSort, len(row))
	if err != nil {
		return nil, err
	}
	copy(buf.Bytes(), row)
	t.rowBytes += int64(len(row))
	return buf, nil
}

func (t *TopN) Consume(ctx context.Context, c *vector.DataChunk) error {
	if t.finalized {
		return errors.New("TopN finalized")
	}
	if c == nil {
		return errors.New("nil chunk")
	}
	if len(c.Columns) != len(t.cfg.InputTypes) {
		return errors.New("TopN input column count mismatch")
	}
	if t.cfg.N == 0 {
		return ctx.Err()
	}
	for r := 0; r < c.Cardinality; r++ {
		if err := ctx.Err(); err != nil {
			return err
		}
		size, err := t.layout.RowSize(c, r)
		if err != nil {
			return err
		}
		if err := t.ensureScratch(maxInt(size, 1)); err != nil {
			return err
		}
		row := t.scratch.Bytes()[:size]
		if _, err := t.layout.WriteRow(c, r, row); err != nil {
			return err
		}
		if len(t.entries) < t.cfg.N {
			buf, err := t.copyCandidate(row)
			if err != nil {
				return err
			}
			t.entries = append(t.entries, buf)
			if err := t.up(len(t.entries) - 1); err != nil {
				return err
			}
			continue
		}
		cmp, err := compareRowsWith(t.layout, t.cfg.Keys, row, t.entries[0].Bytes())
		if err != nil {
			return err
		}
		if cmp >= 0 {
			continue
		}
		old := t.entries[0]
		delta := int64(len(row) - old.Len())
		if t.preallocated == nil {
			if delta > 0 && t.managedBytesWith(delta) > t.cfg.MemoryBudget {
				return memory.ErrLimitExceeded
			}
			if err := old.Resize(len(row), nil); err != nil {
				return err
			}
			t.rowBytes += delta
		} else if delta != 0 {
			return errors.New("TopN fixed preallocation row width changed")
		}
		copy(old.Bytes(), row)
		if err := t.down(0); err != nil {
			return err
		}
	}
	return nil
}

func maxInt(a, b int) int {
	if a > b {
		return a
	}
	return b
}

func (t *TopN) sortedIndexes() (*managedIndices, error) {
	n := len(t.entries)
	var idx *managedIndices
	if t.indices != nil {
		idx = t.indices
		t.indices = nil
		idx.n = n
		for i := 0; i < n; i++ {
			idx.set(i, i)
		}
	} else {
		var err error
		idx, err = newManagedIndices(t.cfg.Scope, t.cfg.Buffers, n)
		if err != nil {
			return nil, err
		}
	}
	less := func(i, j int) (bool, error) {
		cmp, err := compareRowsWith(t.layout, t.cfg.Keys, t.entries[idx.get(i)].Bytes(), t.entries[idx.get(j)].Bytes())
		return cmp < 0, err
	}
	sift := func(root, end int) error {
		for {
			child := root*2 + 1
			if child >= end {
				return nil
			}
			best := child
			if child+1 < end {
				l, err := less(best, child+1)
				if err != nil {
					return err
				}
				if l {
					best = child + 1
				}
			}
			l, err := less(root, best)
			if err != nil {
				return err
			}
			if !l {
				return nil
			}
			idx.swap(root, best)
			root = best
		}
	}
	for i := n/2 - 1; i >= 0; i-- {
		if err := sift(i, n); err != nil {
			idx.release()
			return nil, err
		}
	}
	for end := n - 1; end > 0; end-- {
		idx.swap(0, end)
		if err := sift(0, end); err != nil {
			idx.release()
			return nil, err
		}
	}
	return idx, nil
}

func (t *TopN) Emit(ctx context.Context, emit func(*vector.DataChunk) error) error {
	if t.finalized {
		return errors.New("TopN finalized")
	}
	if emit == nil {
		return errors.New("TopN emit callback required")
	}
	if err := ctx.Err(); err != nil {
		return err
	}
	idx, err := t.sortedIndexes()
	if err != nil {
		return err
	}
	defer idx.release()
	shim := &ExternalSort{cfg: Config{InputTypes: t.cfg.InputTypes, Keys: t.cfg.Keys, Scope: t.cfg.Scope, Buffers: t.cfg.Buffers}, layout: t.layout}
	err = shim.emitRows(ctx, func(consume func([]byte) error) error {
		for i := 0; i < idx.n; i++ {
			if err := consume(t.entries[idx.get(i)].Bytes()); err != nil {
				return err
			}
		}
		return nil
	}, emit)
	if err != nil {
		return err
	}
	t.finalized = true
	return nil
}

func (t *TopN) Release() {
	if t.preallocated != nil {
		for _, buf := range t.preallocated {
			if buf != nil {
				buf.Release()
			}
		}
		t.preallocated = nil
	} else {
		for _, buf := range t.entries {
			if buf != nil {
				buf.Release()
			}
		}
	t.entries = nil
	t.rowBytes = 0
	if t.indices != nil {
		t.indices.release()
		t.indices = nil
	}
	if t.scratch != nil {
		t.scratch.Release()
		t.scratch = nil
	}
	if t.meta != nil {
		t.meta.Release()
		t.meta = nil
	}
}

func encodeTopNIndex(dst []byte, v int) { binary.LittleEndian.PutUint32(dst, uint32(v)) }
