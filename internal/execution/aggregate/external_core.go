package aggregate

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
	storagetemp "github.com/steamwo/fssql/internal/storage/temp"
)

type ExternalConfig struct {
	InputTypes      []types.LogicalType
	GroupColumns    []int
	Specs           []Spec
	EstimatedGroups int
	MemoryBudget    int64
	RadixBits       uint8
	MaxDepth        int
	SpillBlockBytes int
	Scope           *memory.Scope
	Buffers         *memory.BufferManager
	Temp            *storagetemp.QueryScope
	Compression     storagetemp.Compression
	CountOnly       bool
}
type ExternalStats struct {
	SpillPasses        int
	SpilledGroups      uint64
	SpillPages         int
	Repartitions       int
	PeakAggregateBytes int64
}
type aggregatePartition struct {
	pages *storagetemp.PageList
	rows  uint64
}
type aggregateSpillWriter struct {
	store  *storagetemp.SpillablePageStore
	scope  *memory.Scope
	bm     *memory.BufferManager
	pages  *storagetemp.PageList
	buf    *memory.Buffer
	data   []byte
	used   int
	target int
	rows   uint64
}

func newAggregateSpillWriter(store *storagetemp.SpillablePageStore, scope *memory.Scope, bm *memory.BufferManager, target int) (*aggregateSpillWriter, error) {
	if target < 512 {
		target = 512
	}
	pages, err := storagetemp.NewPageList(scope, bm)
	if err != nil {
		return nil, err
	}
	buf, err := bm.Allocate(scope, memory.TagSpillIO, target)
	if err != nil {
		pages.Release()
		return nil, err
	}
	return &aggregateSpillWriter{store: store, scope: scope, bm: bm, pages: pages, buf: buf, data: buf.Bytes(), target: target}, nil
}
func (w *aggregateSpillWriter) flush(ctx context.Context) error {
	if w.used == 0 {
		return nil
	}
	id, err := w.store.WritePage(ctx, w.data[:w.used])
	if err != nil {
		return err
	}
	if err := w.pages.Append(id); err != nil {
		return err
	}
	w.used = 0
	if len(w.data) != w.target {
		if err := w.buf.Resize(w.target, nil); err != nil {
			return err
		}
		w.data = w.buf.Bytes()
	}
	return nil
}
func (w *aggregateSpillWriter) append(ctx context.Context, record []byte) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	need := 4 + len(record)
	if w.used > 0 && w.used+need > len(w.data) {
		if err := w.flush(ctx); err != nil {
			return err
		}
	}
	if need > len(w.data) {
		if err := w.buf.Resize(need, nil); err != nil {
			return err
		}
		w.data = w.buf.Bytes()
	}
	binary.LittleEndian.PutUint32(w.data[w.used:w.used+4], uint32(len(record)))
	copy(w.data[w.used+4:w.used+need], record)
	w.used += need
	w.rows++
	return nil
}
func (w *aggregateSpillWriter) close(ctx context.Context) error {
	if err := w.flush(ctx); err != nil {
		return err
	}
	if w.buf != nil {
		w.buf.Release()
		w.buf = nil
		w.data = nil
	}
	return nil
}
func (w *aggregateSpillWriter) release() {
	if w.buf != nil {
		w.buf.Release()
		w.buf = nil
		w.data = nil
	}
	if w.pages != nil {
		w.pages.Release()
		w.pages = nil
	}
}

type ExternalAggregate struct {
	cfg           ExternalConfig
	bound         []BoundSpec
	groupTypes    []types.LogicalType
	groupLayout   exechash.RowLayout
	local         *HashAggregate
	store         *storagetemp.SpillablePageStore
	partitions    []aggregatePartition
	radix         exechash.RadixPartitioner
	recordScratch *memory.Buffer
	recordData    []byte
	counted       uint64
	spilled       bool
	finalized     bool
	stats         ExternalStats
	shardGate     sync.RWMutex
	shardMu       sync.Mutex
	mergeMu       sync.Mutex
	statsMu       sync.Mutex
	shards        []*HashAggregate
	shardBytes    []int64
	totalBytes    int64
}

func NewExternalAggregate(cfg ExternalConfig) (*ExternalAggregate, error) {
	if cfg.Scope == nil || cfg.Buffers == nil || cfg.Temp == nil {
		return nil, errors.New("external aggregate requires scope, buffers and temp")
	}
	if cfg.MemoryBudget <= 0 {
		return nil, errors.New("external aggregate memory budget must be positive")
	}
	if cfg.RadixBits == 0 {
		cfg.RadixBits = 3
	}
	if cfg.RadixBits > 8 {
		return nil, errors.New("external aggregate radix bits baseline is limited to 1..8")
	}
	if cfg.MaxDepth <= 0 {
		cfg.MaxDepth = 8
	}
	if cfg.SpillBlockBytes <= 0 {
		cfg.SpillBlockBytes = 64 << 10
	}
	bound, err := Bind(cfg.InputTypes, cfg.Specs)
	if err != nil {
		return nil, err
	}
	gtypes := make([]types.LogicalType, len(cfg.GroupColumns))
	for i, c := range cfg.GroupColumns {
		if c < 0 || c >= len(cfg.InputTypes) {
			return nil, fmt.Errorf("group column %d out of range", c)
		}
		gtypes[i] = cfg.InputTypes[c]
	}
	gl, err := exechash.NewRelocatableRowLayout(gtypes)
	if err != nil {
		return nil, err
	}
	radix, err := exechash.NewRadixPartitioner(cfg.RadixBits)
	if err != nil {
		return nil, err
	}
	store, err := storagetemp.NewSpillablePageStore(cfg.Temp, cfg.Scope, cfg.Buffers, storagetemp.PageStoreConfig{Compression: cfg.Compression, MinCompressBytes: 1024, SegmentMaxBytes: 64 << 20})
	if err != nil {
		return nil, err
	}
	recordScratch, err := cfg.Buffers.Allocate(cfg.Scope, memory.TagSpillIO, 64)
	if err != nil {
		store.Close()
		return nil, err
	}
	e := &ExternalAggregate{cfg: cfg, bound: bound, groupTypes: gtypes, groupLayout: gl, store: store, radix: radix, partitions: make([]aggregatePartition, radix.Count()), recordScratch: recordScratch, recordData: recordScratch.Bytes()}
	if err := e.resetLocal(); err != nil {
		recordScratch.Release()
		store.Close()
		return nil, err
	}
	e.resetShardsToLocal()
	return e, nil
}
func (e *ExternalAggregate) resetLocal() error {
	a, err := NewHashAggregate(e.cfg.Scope, e.cfg.Buffers, e.cfg.InputTypes, e.cfg.GroupColumns, e.cfg.Specs, max(e.cfg.EstimatedGroups, 8))
	if err != nil {
		return err
	}
	e.local = a
	return nil
}
func (e *ExternalAggregate) Stats() ExternalStats {
	e.shardGate.RLock()
	defer e.shardGate.RUnlock()
	e.statsMu.Lock()
	defer e.statsMu.Unlock()
	return e.stats
}
func (e *ExternalAggregate) updatePeak(bytes int64) {
	e.statsMu.Lock()
	if bytes > e.stats.PeakAggregateBytes {
		e.stats.PeakAggregateBytes = bytes
	}
	e.statsMu.Unlock()
}
func (e *ExternalAggregate) Consume(ctx context.Context, chunk *vector.DataChunk) error {
	return e.consumeWorker(ctx, chunk)
}
func (e *ExternalAggregate) stateRecord(source *HashAggregate, group int, grow []byte, hashv uint64) ([]byte, error) {
	fixed := 12 + len(grow) + len(source.specs)*aggregateStateWidth
	extra := 0
	for ai, s := range source.specs {
		st := source.state(group, ai)
		if (s.Spec.Kind == Min || s.Spec.Kind == Max) && s.InputType.ID == types.Varchar && st[0] != 0 {
			extra += int(binary.LittleEndian.Uint32(st[20:24]))
		}
	}
	if fixed > math.MaxInt-extra {
		return nil, errors.New("aggregate spill record overflow")
	}
	need := fixed + extra
	if e.recordScratch == nil {
		return nil, errors.New("aggregate spill record scratch released")
	}
	if len(e.recordData) < need {
		if err := e.recordScratch.Resize(need, nil); err != nil {
			return nil, err
		}
		e.recordData = e.recordScratch.Bytes()
	}
	rec := e.recordData[:need]
	for i := range rec {
		rec[i] = 0
	}
	binary.LittleEndian.PutUint64(rec[:8], hashv)
	binary.LittleEndian.PutUint32(rec[8:12], uint32(len(grow)))
	copy(rec[12:12+len(grow)], grow)
	stateBase := 12 + len(grow)
	extraPos := fixed
	for ai, s := range source.specs {
		src := source.state(group, ai)
		dst := rec[stateBase+ai*aggregateStateWidth : stateBase+(ai+1)*aggregateStateWidth]
		copy(dst, src)
		if (s.Spec.Kind == Min || s.Spec.Kind == Max) && s.InputType.ID == types.Varchar && src[0] != 0 {
			off := int(binary.LittleEndian.Uint32(src[16:20]))
			n := int(binary.LittleEndian.Uint32(src[20:24]))
			if source.arena == nil || off < 0 || n < 0 || off > source.arenaUsed || n > source.arenaUsed-off {
				return nil, errors.New("invalid varchar aggregate state")
			}
			copy(rec[extraPos:extraPos+n], source.arena.Bytes()[off:off+n])
			binary.LittleEndian.PutUint32(dst[16:20], uint32(extraPos))
			extraPos += n
		}
	}
	return rec, nil
}
func (e *ExternalAggregate) spillAggregate(ctx context.Context, source *HashAggregate) error {
	if source == nil || source.GroupCount() == 0 {
		return nil
	}
	e.spilled = true
	e.stats.SpillPasses++
	writers := make([]*aggregateSpillWriter, e.radix.Count())
	release := func() {
		for _, w := range writers {
			if w != nil {
				w.release()
			}
		}
	}
	reader := source.groups.BorrowRowReader()
	for g := 0; g < source.GroupCount(); g++ {
		if g&255 == 0 {
			if err := ctx.Err(); err != nil {
				release()
				return err
			}
		}
		row := reader(g)
		hash, err := e.groupLayout.HashRow(row)
		if err != nil {
			release()
			return err
		}
		partition := e.radix.Partition(hash)
		w := writers[partition]
		if w == nil {
			w, err = newAggregateSpillWriter(e.store, e.cfg.Scope, e.cfg.Buffers, e.cfg.SpillBlockBytes)
			if err != nil {
				release()
				return err
			}
			writers[partition] = w
		}
		rec, err := e.stateRecord(source, g, row, hash)
		if err != nil {
			release()
			return err
		}
		if err := w.append(ctx, rec); err != nil {
			release()
			return err
		}
	}
	for p, w := range writers {
		if w == nil {
			continue
		}
		if err := w.close(ctx); err != nil {
			release()
			return err
		}
		if w.rows == 0 {
			continue
		}
		pagesWritten := w.pages.Count()
		if e.partitions[p].pages == nil {
			e.partitions[p].pages = w.pages
			w.pages = nil
		} else {
			for i := 0; i < w.pages.Count(); i++ {
				id, _ := w.pages.At(i)
				if err := e.partitions[p].pages.Append(id); err != nil {
					release()
					return err
				}
			}
			w.pages.Release()
			w.pages = nil
		}
		e.partitions[p].rows += w.rows
		e.stats.SpilledGroups += w.rows
		e.stats.SpillPages += pagesWritten
	}
	release()
	return nil
}
