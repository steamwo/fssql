package join

import (
	"context"
	"encoding/binary"
	"errors"
	"math"
	"sync"
	"sync/atomic"

	"github.com/steamwo/fssql/internal/core/memory"
	"github.com/steamwo/fssql/internal/core/types"
	"github.com/steamwo/fssql/internal/core/vector"
	exechash "github.com/steamwo/fssql/internal/execution/hash"
	"github.com/steamwo/fssql/internal/execution/pipeline"
	storagetemp "github.com/steamwo/fssql/internal/storage/temp"
)

type Side uint8

const (
	LeftSide Side = iota
	RightSide
)

type ExternalConfig struct {
	NullsEqual          bool // Parameter-domain identity includes NULL keys.
	Type                Type
	RequiredOutput      []bool // nil keeps all output columns; otherwise full schema width.
	LeftTypes           []types.LogicalType
	RightTypes          []types.LogicalType
	LeftKeys            []int
	RightKeys           []int
	LeftEstimatedBytes  uint64
	RightEstimatedBytes uint64
	MemoryBudget        int64
	RadixBits           uint8
	MaxDepth            int
	MinReductionPercent uint8
	SpillBlockBytes     int
	Scope               *memory.Scope
	Buffers             *memory.BufferManager
	Temp                *storagetemp.QueryScope
	Compression         storagetemp.Compression
}

type ExternalStats struct {
	BuildSide               Side
	ResidentBuildPartitions int
	SpilledBuildPartitions  int
	Repartitions            int
	TerminalSkewPartitions  int
	PeakResidentBytes       int64
	BuildRows               uint64
	ProbeRows               uint64
}

func (j *ExternalHashJoin) nullEquality() exechash.NullEquality {
	if j.config.NullsEqual {
		return exechash.NullsEqual
	}
	return exechash.NullsUnequal
}

const externalPartitionMetadataBytes = 128

func ChooseBuildSide(leftEstimatedBytes, rightEstimatedBytes uint64) Side {
	if leftEstimatedBytes != 0 && (rightEstimatedBytes == 0 || leftEstimatedBytes < rightEstimatedBytes) {
		return LeftSide
	}
	return RightSide
}

type recordCollection struct {
	scope    *memory.Scope
	bm       *memory.BufferManager
	layout   exechash.RowLayout
	data     *memory.Buffer
	offsets  *memory.Buffer
	count    int
	capacity int
	used     int
}

func newRecordCollection(scope *memory.Scope, bm *memory.BufferManager, layout exechash.RowLayout, capacity int) (*recordCollection, error) {
	if capacity < 1 {
		capacity = 1
	}
	dataBytes := capacity * (layout.FixedWidth + 12)
	if dataBytes < 64 {
		dataBytes = 64
	}
	data, err := bm.Allocate(scope, memory.TagHashTable, dataBytes)
	if err != nil {
		return nil, err
	}
	offsets, err := bm.Allocate(scope, memory.TagHashTable, (capacity+1)*4)
	if err != nil {
		data.Release()
		return nil, err
	}
	return &recordCollection{scope: scope, bm: bm, layout: layout, data: data, offsets: offsets, capacity: capacity}, nil
}

func (c *recordCollection) Count() int     { return c.count }
func (c *recordCollection) UsedBytes() int { return c.used }
func (c *recordCollection) AllocatedBytes() int {
	if c == nil || c.data == nil {
		return 0
	}
	return c.data.Len() + c.offsets.Len()
}

func (c *recordCollection) ensureRows(next int) error {
	if next <= c.capacity {
		return nil
	}
	cap := c.capacity * 2
	if cap < next {
		cap = next
	}
	if cap > math.MaxInt/4-1 {
		return errors.New("external join row offset capacity overflow")
	}
	if err := c.offsets.Resize((cap+1)*4, nil); err != nil {
		return err
	}
	c.capacity = cap
	return nil
}
func (c *recordCollection) ensureData(next int) error {
	if next <= c.data.Len() {
		return nil
	}
	cap := c.data.Len() * 2
	if cap < next {
		cap = next
	}
	if cap < 0 {
		return errors.New("external join record capacity overflow")
	}
	return c.data.Resize(cap, nil)
}

func (c *recordCollection) appendEncoded(hash uint64, row []byte) error {
	if len(row) > math.MaxUint32 || c.used > math.MaxUint32-12-len(row) {
		return errors.New("external join record collection exceeds 4GiB")
	}
	if err := c.ensureRows(c.count + 1); err != nil {
		return err
	}
	recordBytes := 12 + len(row)
	if err := c.ensureData(c.used + recordBytes); err != nil {
		return err
	}
	binary.LittleEndian.PutUint32(c.offsets.Bytes()[c.count*4:c.count*4+4], uint32(c.used))
	dst := c.data.Bytes()[c.used : c.used+recordBytes]
	binary.LittleEndian.PutUint64(dst[:8], hash)
	binary.LittleEndian.PutUint32(dst[8:12], uint32(len(row)))
	copy(dst[12:], row)
	c.used += recordBytes
	c.count++
	binary.LittleEndian.PutUint32(c.offsets.Bytes()[c.count*4:c.count*4+4], uint32(c.used))
	return nil
}

func (c *recordCollection) appendChunkRow(hash uint64, chunk *vector.DataChunk, row int) error {
	rowBytes, err := c.layout.RowSize(chunk, row)
	if err != nil {
		return err
	}
	if rowBytes > math.MaxUint32 || c.used > math.MaxUint32-12-rowBytes {
		return errors.New("external join record collection exceeds 4GiB")
	}
	if err := c.ensureRows(c.count + 1); err != nil {
		return err
	}
	recordBytes := 12 + rowBytes
	if err := c.ensureData(c.used + recordBytes); err != nil {
		return err
	}
	binary.LittleEndian.PutUint32(c.offsets.Bytes()[c.count*4:c.count*4+4], uint32(c.used))
	dst := c.data.Bytes()[c.used : c.used+recordBytes]
	binary.LittleEndian.PutUint64(dst[:8], hash)
	binary.LittleEndian.PutUint32(dst[8:12], uint32(rowBytes))
	if _, err := c.layout.WriteRow(chunk, row, dst[12:]); err != nil {
		return err
	}
	c.used += recordBytes
	c.count++
	binary.LittleEndian.PutUint32(c.offsets.Bytes()[c.count*4:c.count*4+4], uint32(c.used))
	return nil
}

func (c *recordCollection) record(i int) (uint64, []byte, bool) {
	if c == nil || i < 0 || i >= c.count {
		return 0, nil, false
	}
	start := int(binary.LittleEndian.Uint32(c.offsets.Bytes()[i*4 : i*4+4]))
	end := int(binary.LittleEndian.Uint32(c.offsets.Bytes()[(i+1)*4 : (i+1)*4+4]))
	if start < 0 || end < start+12 || end > c.used {
		return 0, nil, false
	}
	record := c.data.Bytes()[start:end]
	rowLen := int(binary.LittleEndian.Uint32(record[8:12]))
	if rowLen != len(record)-12 {
		return 0, nil, false
	}
	return binary.LittleEndian.Uint64(record[:8]), record[12:], true
}

func (c *recordCollection) Release() {
	if c == nil {
		return
	}
	if c.data != nil {
		c.data.Release()
		c.data = nil
	}
	if c.offsets != nil {
		c.offsets.Release()
		c.offsets = nil
	}
	c.count, c.capacity, c.used = 0, 0, 0
}

type spillWriter struct {
	store  *storagetemp.SpillablePageStore
	scope  *memory.Scope
	bm     *memory.BufferManager
	pages  *storagetemp.PageList
	buf    *memory.Buffer
	used   int
	target int
	rows   uint64
	bytes  uint64
	closed bool
}

func newSpillWriter(store *storagetemp.SpillablePageStore, scope *memory.Scope, bm *memory.BufferManager, target int) (*spillWriter, error) {
	if target < 256 {
		target = 256
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
	return &spillWriter{store: store, scope: scope, bm: bm, pages: pages, buf: buf, target: target}, nil
}

func (w *spillWriter) ensure(recordBytes int, ctx context.Context) error {
	if w.closed {
		return errors.New("spill writer closed")
	}
	if w.used > 0 && w.used+recordBytes > w.buf.Len() {
		if err := w.flush(ctx); err != nil {
			return err
		}
	}
	if recordBytes > w.buf.Len() {
		if err := w.buf.Resize(recordBytes, nil); err != nil {
			return err
		}
	}
	return nil
}

func (w *spillWriter) appendEncoded(ctx context.Context, hash uint64, row []byte) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	recordBytes := 12 + len(row)
	if err := w.ensure(recordBytes, ctx); err != nil {
		return err
	}
	dst := w.buf.Bytes()[w.used : w.used+recordBytes]
	binary.LittleEndian.PutUint64(dst[:8], hash)
	binary.LittleEndian.PutUint32(dst[8:12], uint32(len(row)))
	copy(dst[12:], row)
	w.used += recordBytes
	w.rows++
	w.bytes += uint64(recordBytes)
	return nil
}

func (w *spillWriter) appendChunkRow(ctx context.Context, hash uint64, layout exechash.RowLayout, chunk *vector.DataChunk, row int) error {
	rowBytes, err := layout.RowSize(chunk, row)
	if err != nil {
		return err
	}
	recordBytes := 12 + rowBytes
	if err := w.ensure(recordBytes, ctx); err != nil {
		return err
	}
	dst := w.buf.Bytes()[w.used : w.used+recordBytes]
	binary.LittleEndian.PutUint64(dst[:8], hash)
	binary.LittleEndian.PutUint32(dst[8:12], uint32(rowBytes))
	if _, err := layout.WriteRow(chunk, row, dst[12:]); err != nil {
		return err
	}
	w.used += recordBytes
	w.rows++
	w.bytes += uint64(recordBytes)
	return nil
}

func (w *spillWriter) flush(ctx context.Context) error {
	if w.used == 0 {
		return nil
	}
	id, err := w.store.WritePage(ctx, w.buf.Bytes()[:w.used])
	if err != nil {
		return err
	}
	if err := w.pages.Append(id); err != nil {
		return err
	}
	w.used = 0
	if w.buf.Len() != w.target {
		if err := w.buf.Resize(w.target, nil); err != nil {
			return err
		}
	}
	return nil
}

func (w *spillWriter) Close(ctx context.Context) error {
	if w == nil || w.closed {
		return nil
	}
	if err := w.flush(ctx); err != nil {
		return err
	}
	w.closed = true
	if w.buf != nil {
		w.buf.Release()
		w.buf = nil
	}
	return nil
}
func (w *spillWriter) Release() {
	if w == nil {
		return
	}
	if w.buf != nil {
		w.buf.Release()
		w.buf = nil
	}
	if w.pages != nil {
		w.pages.Release()
		w.pages = nil
	}
}

type partitionSet struct {
	pages *storagetemp.PageList
	rows  uint64
	bytes uint64
}

// Repartitioned page lists are local ownership, unlike the root spill writers.
// Release their metadata on both success and failure. Page bytes remain owned
// by the page store, which closes on a failed join. PageList.Release is idempotent.
func releasePartitionSetMetadata(sets []partitionSet) {
	for _, set := range sets {
		if set.pages != nil {
			set.pages.Release()
		}
	}
}

func (w *spillWriter) set() partitionSet {
	if w == nil {
		return partitionSet{}
	}
	return partitionSet{pages: w.pages, rows: w.rows, bytes: w.bytes}
}

type hybridPartition struct {
	resident   *recordCollection
	index      *rowHashTable
	buildSpill *spillWriter
	probeSpill *spillWriter
	spilled    bool
}

type ExternalHashJoin struct {
	mu      sync.Mutex
	spillMu sync.Mutex
	// probeRows is updated by concurrent probe workers; Stats folds it in.
	probeRows atomic.Uint64

	config       ExternalConfig
	buildSide    Side
	buildTypes   []types.LogicalType
	probeTypes   []types.LogicalType
	buildKeys    []int
	probeKeys    []int
	buildLayout  exechash.RowLayout
	probeLayout  exechash.RowLayout
	partitioner  exechash.RadixPartitioner
	policy       exechash.RepartitionPolicy
	store        *storagetemp.SpillablePageStore
	partitions   []hybridPartition
	partitionRes *memory.Reservation
	stats        ExternalStats
	built        bool
	failed       error
	closed       bool
}

func NewExternal(config ExternalConfig) (*ExternalHashJoin, error) {
	if err := validateRequiredOutput(config); err != nil { return nil, err }
	config.RequiredOutput = append([]bool(nil), config.RequiredOutput...)
	if config.Type != Inner {
		return nil, errors.New("external hash join baseline currently supports INNER only")
	}
	if config.Scope == nil || config.Buffers == nil || config.Temp == nil {
		return nil, errors.New("external join requires memory, buffers and temporary scope")
	}
	if config.MemoryBudget < 4096 {
		return nil, errors.New("external join memory budget must be at least 4096 bytes")
	}
	if config.RadixBits == 0 {
		config.RadixBits = 4
	}
	if config.RadixBits > 8 {
		return nil, errors.New("external join radix bits baseline is limited to 1..8")
	}
	if config.MaxDepth <= 0 {
		config.MaxDepth = 4
	}
	if config.MinReductionPercent == 0 {
		config.MinReductionPercent = 10
	}
	if config.SpillBlockBytes <= 0 {
		config.SpillBlockBytes = 64 << 10
	}
	if len(config.LeftKeys) == 0 || len(config.LeftKeys) != len(config.RightKeys) {
		return nil, errors.New("external hash join requires matching non-empty key lists")
	}
	for i := range config.LeftKeys {
		lk, rk := config.LeftKeys[i], config.RightKeys[i]
		if lk < 0 || lk >= len(config.LeftTypes) || rk < 0 || rk >= len(config.RightTypes) {
			return nil, errors.New("external join key index out of range")
		}
		if !sameType(config.LeftTypes[lk], config.RightTypes[rk]) {
			return nil, errors.New("external join key type mismatch")
		}
	}
	leftLayout, err := exechash.NewRelocatableRowLayout(config.LeftTypes)
	if err != nil {
		return nil, err
	}
	rightLayout, err := exechash.NewRelocatableRowLayout(config.RightTypes)
	if err != nil {
		return nil, err
	}
	partitioner, err := exechash.NewRadixPartitioner(config.RadixBits)
	if err != nil {
		return nil, err
	}
	policy, err := exechash.NewRepartitionPolicy(config.MaxDepth, config.MinReductionPercent)
	if err != nil {
		return nil, err
	}
	store, err := storagetemp.NewSpillablePageStore(config.Temp, config.Scope, config.Buffers, storagetemp.PageStoreConfig{Compression: config.Compression, MinCompressBytes: 1024})
	if err != nil {
		return nil, err
	}
	partRes, err := config.Scope.Reserve(memory.TagMetadata, int64(partitioner.Count()*externalPartitionMetadataBytes))
	if err != nil {
		_ = store.Close()
		return nil, err
	}
	buildSide := ChooseBuildSide(config.LeftEstimatedBytes, config.RightEstimatedBytes)
	j := &ExternalHashJoin{config: config, buildSide: buildSide, partitioner: partitioner, policy: policy, store: store, partitions: make([]hybridPartition, partitioner.Count()), partitionRes: partRes}
	j.stats.BuildSide = buildSide
	if buildSide == RightSide {
		j.buildTypes, j.probeTypes = config.RightTypes, config.LeftTypes
		j.buildKeys, j.probeKeys = config.RightKeys, config.LeftKeys
		j.buildLayout, j.probeLayout = rightLayout, leftLayout
	} else {
		j.buildTypes, j.probeTypes = config.LeftTypes, config.RightTypes
		j.buildKeys, j.probeKeys = config.LeftKeys, config.RightKeys
		j.buildLayout, j.probeLayout = leftLayout, rightLayout
	}
	return j, nil
}

func (j *ExternalHashJoin) BuildSide() Side { return j.buildSide }
func (j *ExternalHashJoin) Stats() ExternalStats {
	j.mu.Lock()
	stats := j.stats
	j.mu.Unlock()
	stats.ProbeRows = j.probeRows.Load()
	return stats
}
func (j *ExternalHashJoin) BuildSink() pipeline.Sink { return &externalBuildSink{join: j} }
func (j *ExternalHashJoin) ProbeSink(downstream pipeline.Sink) pipeline.Sink {
	return &externalProbeSink{join: j, downstream: downstream}
}

func (j *ExternalHashJoin) spillTarget() int {
	perPartition := int(j.config.MemoryBudget) / (j.partitioner.Count() * 2)
	if perPartition < 256 {
		perPartition = 256
	}
	if perPartition > j.config.SpillBlockBytes {
		perPartition = j.config.SpillBlockBytes
	}
	return perPartition
}

func (j *ExternalHashJoin) residentBytes() int64 {
	var total int64
	for i := range j.partitions {
		p := &j.partitions[i]
		if p.resident != nil {
			total += int64(p.resident.AllocatedBytes())
		}
		if p.index != nil {
			total += int64(indexEstimateBytes(p.resident.Count()))
		}
	}
	if total > j.stats.PeakResidentBytes {
		j.stats.PeakResidentBytes = total
	}
	return total
}

func indexEstimateBytes(rows int) int {
	capacity := 8
	for capacity < rows*2 {
		capacity <<= 1
	}
	next := rows * 4
	if next < 4 {
		next = 4
	}
	return capacity*rowHashSlotWidth + next
}

func (j *ExternalHashJoin) partitionFor(hash uint64, depth int) (int, error) {
	return j.partitioner.PartitionAtDepth(hash, depth)
}

func (j *ExternalHashJoin) ensureResident(partition int) (*recordCollection, error) {
	p := &j.partitions[partition]
	if p.resident != nil {
		return p.resident, nil
	}
	c, err := newRecordCollection(j.config.Scope, j.config.Buffers, j.buildLayout, 1)
	if err != nil {
		return nil, err
	}
	p.resident = c
	return c, nil
}

func (j *ExternalHashJoin) spillPartition(ctx context.Context, partition int) error {
	p := &j.partitions[partition]
	if p.spilled {
		return nil
	}
	w, err := newSpillWriter(j.store, j.config.Scope, j.config.Buffers, j.spillTarget())
	if err != nil {
		return err
	}
	if p.resident != nil {
		reader := p.resident.borrowReader()
		for i := 0; i < p.resident.Count(); i++ {
			h, row, ok := reader.record(i)
			if !ok {
				w.Release()
				return errors.New("invalid resident external join record")
			}
			if err := w.appendEncoded(ctx, h, row); err != nil {
				w.Release()
				return err
			}
		}
		p.resident.Release()
		p.resident = nil
	}
	if p.index != nil {
		p.index.Release()
		p.index = nil
	}
	p.buildSpill = w
	p.spilled = true
	j.stats.SpilledBuildPartitions++
	return nil
}

func (j *ExternalHashJoin) enforceBuildBudget(ctx context.Context) error {
	limit := j.config.MemoryBudget / 2
	for j.residentBytes() > limit {
		largest := -1
		largestBytes := -1
		for i := range j.partitions {
			p := &j.partitions[i]
			if p.spilled || p.resident == nil {
				continue
			}
			if n := p.resident.UsedBytes(); n > largestBytes {
				largest, largestBytes = i, n
			}
		}
		if largest < 0 {
			return errors.New("external join cannot reduce resident build memory")
		}
		if err := j.spillPartition(ctx, largest); err != nil {
			return err
		}
	}
	return nil
}

func (j *ExternalHashJoin) releaseLocked() {
	for i := range j.partitions {
		p := &j.partitions[i]
		if p.index != nil {
			p.index.Release()
			p.index = nil
		}
		if p.resident != nil {
			p.resident.Release()
			p.resident = nil
		}
		if p.buildSpill != nil {
			p.buildSpill.Release()
			p.buildSpill = nil
		}
		if p.probeSpill != nil {
			p.probeSpill.Release()
			p.probeSpill = nil
		}
	}
	if j.store != nil {
		_ = j.store.Close()
		j.store = nil
	}
	if j.partitionRes != nil {
		j.partitionRes.Release()
		j.partitionRes = nil
	}
	j.closed = true
}
func (j *ExternalHashJoin) failLocked(err error) error {
	if j.failed == nil {
		j.failed = err
	}
	j.releaseLocked()
	return err
}
func (j *ExternalHashJoin) Close() {
	j.mu.Lock()
	defer j.mu.Unlock()
	j.releaseLocked()
}

type externalBuildSink struct{ join *ExternalHashJoin }

func (s *externalBuildSink) Consume(ctx context.Context, chunk *vector.DataChunk) error {
	j := s.join
	j.mu.Lock()
	defer j.mu.Unlock()
	if err := ctx.Err(); err != nil {
		return j.failLocked(err)
	}
	if j.failed != nil {
		return j.failed
	}
	if j.closed || j.built {
		return errors.New("external join build is closed or finalized")
	}
	if len(chunk.Columns) != len(j.buildTypes) {
		return j.failLocked(errors.New("external join build column count mismatch"))
	}
	var hashes [vector.DefaultVectorSize]uint64
	if err := exechash.HashChunk(chunk, j.buildKeys, hashes[:chunk.Cardinality]); err != nil {
		return j.failLocked(err)
	}
	for row := 0; row < chunk.Cardinality; row++ {
		part, err := j.partitionFor(hashes[row], 0)
		if err != nil {
			return j.failLocked(err)
		}
		p := &j.partitions[part]
		if p.spilled {
			if err := p.buildSpill.appendChunkRow(ctx, hashes[row], j.buildLayout, chunk, row); err != nil {
				return j.failLocked(err)
			}
		} else {
			c, err := j.ensureResident(part)
			if err != nil {
				return j.failLocked(err)
			}
			if err := c.appendChunkRow(hashes[row], chunk, row); err != nil {
				return j.failLocked(err)
			}
		}
		j.stats.BuildRows++
	}
	if err := j.enforceBuildBudget(ctx); err != nil {
		return j.failLocked(err)
	}
	return nil
}

func (s *externalBuildSink) Finalize(ctx context.Context) error {
	j := s.join
	j.mu.Lock()
	defer j.mu.Unlock()
	if err := ctx.Err(); err != nil {
		return j.failLocked(err)
	}
	if j.failed != nil {
		return j.failed
	}
	if j.closed {
		return errors.New("external join closed")
	}
	if j.built {
		return nil
	}
	for i := range j.partitions {
		p := &j.partitions[i]
		if p.spilled {
			if err := p.buildSpill.Close(ctx); err != nil {
				return j.failLocked(err)
			}
			continue
		}
		if p.resident == nil || p.resident.Count() == 0 {
			continue
		}
		if j.residentBytes()+int64(indexEstimateBytes(p.resident.Count())) > j.config.MemoryBudget {
			if err := j.spillPartition(ctx, i); err != nil {
				return j.failLocked(err)
			}
			if err := p.buildSpill.Close(ctx); err != nil {
				return j.failLocked(err)
			}
			continue
		}
		index, err := newRowHashTable(j.config.Scope, j.config.Buffers, p.resident.Count())
		if err != nil {
			if errors.Is(err, memory.ErrLimitExceeded) {
				if err := j.spillPartition(ctx, i); err != nil {
					return j.failLocked(err)
				}
				if err := p.buildSpill.Close(ctx); err != nil {
					return j.failLocked(err)
				}
				continue
			}
			return j.failLocked(err)
		}
		reader := p.resident.borrowReader()
		for row := 0; row < p.resident.Count(); row++ {
			h, _, ok := reader.record(row)
			if !ok {
				index.Release()
				return j.failLocked(errors.New("invalid resident build record"))
			}
			if err := index.Insert(h, row); err != nil {
				index.Release()
				return j.failLocked(err)
			}
		}
		p.index = index
		j.stats.ResidentBuildPartitions++
	}
	j.residentBytes()
	j.built = true
	return nil
}

type externalProbeSink struct {
	join       *ExternalHashJoin
	downstream pipeline.Sink
}

func (s *externalProbeSink) Consume(ctx context.Context, chunk *vector.DataChunk) error {
	j := s.join
	j.mu.Lock()
	if err := ctx.Err(); err != nil {
		j.mu.Unlock()
		return err
	}
	if j.failed != nil {
		err := j.failed
		j.mu.Unlock()
		return err
	}
	if j.closed || !j.built {
		j.mu.Unlock()
		err := errors.New("external join build not finalized")
		j.recordProbeFailure(err)
		return err
	}
	if len(chunk.Columns) != len(j.probeTypes) {
		j.mu.Unlock()
		err := errors.New("external join probe column count mismatch")
		j.recordProbeFailure(err)
		return err
	}
	j.mu.Unlock()

	var hashes [vector.DefaultVectorSize]uint64
	if err := exechash.HashChunk(chunk, j.probeKeys, hashes[:chunk.Cardinality]); err != nil {
		j.recordProbeFailure(err)
		return err
	}
	comparer, err := exechash.NewChunkKeyComparer(chunk, j.probeKeys)
	if err != nil {
		j.recordProbeFailure(err)
		return err
	}
	em := newBatchEmitter(Inner, j.config.Scope, j.config.Buffers, j.config.LeftTypes, j.config.RightTypes, j.buildSide, j.buildLayout, s.downstream)
	em.required = j.config.RequiredOutput
	if err := j.probeBatch(ctx, chunk, hashes[:chunk.Cardinality], comparer, em, nil); err != nil {
		em.abort()
		j.recordProbeFailure(err)
		return err
	}
	j.probeRows.Add(uint64(chunk.Cardinality))
	if err := em.flush(ctx); err != nil {
		em.abort()
		j.recordProbeFailure(err)
		return err
	}
	return nil
}

func (j *ExternalHashJoin) recordProbeFailure(err error) {
	j.mu.Lock()
	if j.failed == nil {
		j.failed = err
	}
	j.mu.Unlock()
}

// appendProbeSpill serializes spill writer creation and append without holding
// the build/probe lifecycle mutex, so resident partitions stay lock-free.
func (j *ExternalHashJoin) appendProbeSpill(ctx context.Context, p *hybridPartition, hash uint64, chunk *vector.DataChunk, row int) error {
	j.spillMu.Lock()
	defer j.spillMu.Unlock()
	if p.probeSpill == nil {
		w, err := newSpillWriter(j.store, j.config.Scope, j.config.Buffers, j.spillTarget())
		if err != nil {
			return err
		}
		p.probeSpill = w
	}
	return p.probeSpill.appendChunkRow(ctx, hash, j.probeLayout, chunk, row)
}

func (j *ExternalHashJoin) matchRecord(ctx context.Context, build recordReader, index *rowHashTable, probeRow []byte, hash uint64, em *storedEmitter) error {
	buildIndex, ok := index.Head(hash)
	for ok {
		_, buildRow, valid := build.record(buildIndex)
		if !valid {
			return errors.New("invalid build record")
		}
		equal, err := exechash.EqualProjectedRows(j.buildLayout, buildRow, j.buildKeys, j.probeLayout, probeRow, j.probeKeys, j.nullEquality())
		if err != nil {
			return err
		}
		if equal {
			if err := em.append(ctx, buildRow, probeRow); err != nil {
				return err
			}
		}
		buildIndex, ok = index.Next(buildIndex)
	}
	return nil
}

func scanRecords(payload []byte, fn func(hash uint64, row []byte) error) error {
	for off := 0; off < len(payload); {
		if len(payload)-off < 12 {
			return errors.New("corrupt external join spill record header")
		}
		h := binary.LittleEndian.Uint64(payload[off : off+8])
		n := int(binary.LittleEndian.Uint32(payload[off+8 : off+12]))
		off += 12
		if n < 0 || n > len(payload)-off {
			return errors.New("corrupt external join spill record length")
		}
		if err := fn(h, payload[off:off+n]); err != nil {
			return err
		}
		off += n
	}
	return nil
}

func (j *ExternalHashJoin) deleteSet(set partitionSet) error {
	if set.pages == nil {
		return nil
	}
	for i := 0; i < set.pages.Count(); i++ {
		id, _ := set.pages.At(i)
		if err := j.store.DeletePage(id); err != nil {
			return err
		}
	}
	set.pages.Release()
	return nil
}

func (j *ExternalHashJoin) loadBuild(ctx context.Context, set partitionSet) (*recordCollection, *rowHashTable, error) {
	build, err := newRecordCollection(j.config.Scope, j.config.Buffers, j.buildLayout, int(set.rows))
	if err != nil {
		return nil, nil, err
	}
	for i := 0; i < set.pages.Count(); i++ {
		id, _ := set.pages.At(i)
		buf, err := j.store.ReadPage(ctx, id)
		if err != nil {
			build.Release()
			return nil, nil, err
		}
		err = scanRecords(buf.Bytes(), func(hash uint64, row []byte) error { return build.appendEncoded(hash, row) })
		buf.Release()
		if err != nil {
			build.Release()
			return nil, nil, err
		}
	}
	index, err := newRowHashTable(j.config.Scope, j.config.Buffers, build.Count())
	if err != nil {
		build.Release()
		return nil, nil, err
	}
	reader := build.borrowReader()
	for i := 0; i < build.Count(); i++ {
		h, _, ok := reader.record(i)
		if !ok {
			index.Release()
			build.Release()
			return nil, nil, errors.New("invalid loaded build record")
		}
		if err := index.Insert(h, i); err != nil {
			index.Release()
			build.Release()
			return nil, nil, err
		}
	}
	return build, index, nil
}

func (j *ExternalHashJoin) joinInMemorySet(ctx context.Context, buildSet, probeSet partitionSet, em *storedEmitter) error {
	build, index, err := j.loadBuild(ctx, buildSet)
	if err != nil {
		return err
	}
	defer build.Release()
	defer index.Release()
	reader := build.borrowReader()
	for i := 0; i < probeSet.pages.Count(); i++ {
		id, _ := probeSet.pages.At(i)
		buf, err := j.store.ReadPage(ctx, id)
		if err != nil {
			return err
		}
		err = scanRecords(buf.Bytes(), func(hash uint64, row []byte) error { return j.matchRecord(ctx, reader, index, row, hash, em) })
		buf.Release()
		if err != nil {
			return err
		}
	}
	if err := j.deleteSet(buildSet); err != nil {
		return err
	}
	if err := j.deleteSet(probeSet); err != nil {
		return err
	}
	return nil
}

func (j *ExternalHashJoin) repartitionSet(ctx context.Context, input partitionSet, depth int, withHistogram bool) ([]partitionSet, *exechash.PartitionHistogram, error) {
	count := j.partitioner.Count()
	writers := make([]*spillWriter, count)
	sets := make([]partitionSet, count)
	var histogram *exechash.PartitionHistogram
	var err error
	if withHistogram {
		histogram, err = exechash.NewPartitionHistogram(j.config.Scope, j.config.Buffers, count)
		if err != nil {
			return nil, nil, err
		}
	}
	cleanup := func() {
		releasePartitionSetMetadata(sets)
		for _, w := range writers {
			if w != nil {
				w.Release()
			}
		}
		if histogram != nil {
			histogram.Release()
		}
	}
	for i := 0; i < input.pages.Count(); i++ {
		id, _ := input.pages.At(i)
		buf, readErr := j.store.ReadPage(ctx, id)
		if readErr != nil {
			cleanup()
			return nil, nil, readErr
		}
		err = scanRecords(buf.Bytes(), func(hash uint64, row []byte) error {
			part, err := j.partitionFor(hash, depth)
			if err != nil {
				return err
			}
			if writers[part] == nil {
				writers[part], err = newSpillWriter(j.store, j.config.Scope, j.config.Buffers, j.spillTarget())
				if err != nil {
					return err
				}
			}
			if histogram != nil {
				if err := histogram.Add(part, uint64(12+len(row))); err != nil {
					return err
				}
			}
			return writers[part].appendEncoded(ctx, hash, row)
		})
		buf.Release()
		if err != nil {
			cleanup()
			return nil, nil, err
		}
		if err := j.store.DeletePage(id); err != nil {
			cleanup()
			return nil, nil, err
		}
	}
	input.pages.Release()
	for i, w := range writers {
		if w == nil {
			continue
		}
		if err := w.Close(ctx); err != nil {
			cleanup()
			return nil, nil, err
		}
		sets[i] = w.set()
		w.buf = nil
		writers[i] = nil
	}
	return sets, histogram, nil
}

func (j *ExternalHashJoin) terminalNestedLoop(ctx context.Context, buildSet, probeSet partitionSet, em *storedEmitter) error {
	j.stats.TerminalSkewPartitions++
	for bi := 0; bi < buildSet.pages.Count(); bi++ {
		buildID, _ := buildSet.pages.At(bi)
		buildBuf, err := j.store.ReadPage(ctx, buildID)
		if err != nil {
			return err
		}
		for pi := 0; pi < probeSet.pages.Count(); pi++ {
			probeID, _ := probeSet.pages.At(pi)
			probeBuf, err := j.store.ReadPage(ctx, probeID)
			if err != nil {
				buildBuf.Release()
				return err
			}
			err = scanRecords(buildBuf.Bytes(), func(buildHash uint64, buildRow []byte) error {
				return scanRecords(probeBuf.Bytes(), func(probeHash uint64, probeRow []byte) error {
					if buildHash != probeHash {
						return nil
					}
					eq, err := exechash.EqualProjectedRows(j.buildLayout, buildRow, j.buildKeys, j.probeLayout, probeRow, j.probeKeys, j.nullEquality())
					if err != nil || !eq {
						return err
					}
					return em.append(ctx, buildRow, probeRow)
				})
			})
			probeBuf.Release()
			if err != nil {
				buildBuf.Release()
				return err
			}
		}
		buildBuf.Release()
	}
	if err := j.deleteSet(buildSet); err != nil {
		return err
	}
	return j.deleteSet(probeSet)
}

func (j *ExternalHashJoin) processSpilled(ctx context.Context, buildSet, probeSet partitionSet, depth int, parentBytes uint64, em *storedEmitter) error {
	if buildSet.rows == 0 || probeSet.rows == 0 {
		if err := j.deleteSet(buildSet); err != nil {
			return err
		}
		return j.deleteSet(probeSet)
	}
	if buildSet.bytes <= uint64(j.config.MemoryBudget/4) {
		return j.joinInMemorySet(ctx, buildSet, probeSet, em)
	}
	decision := j.policy.Decide(depth, parentBytes, buildSet.bytes, uint64(j.config.MemoryBudget/4))
	if decision.Action == exechash.PartitionTerminalSkew {
		return j.terminalNestedLoop(ctx, buildSet, probeSet, em)
	}
	nextDepth := depth + 1
	buildChildren, histogram, err := j.repartitionSet(ctx, buildSet, nextDepth, true)
	if err != nil {
		return err
	}
	defer releasePartitionSetMetadata(buildChildren)
	probeChildren, _, err := j.repartitionSet(ctx, probeSet, nextDepth, false)
	if err != nil {
		histogram.Release()
		return err
	}
	defer releasePartitionSetMetadata(probeChildren)
	j.stats.Repartitions++
	for part := 0; part < len(buildChildren); part++ {
		bset := buildChildren[part]
		pset := probeChildren[part]
		if bset.rows == 0 || pset.rows == 0 {
			if err := j.deleteSet(bset); err != nil {
				histogram.Release()
				return err
			}
			if err := j.deleteSet(pset); err != nil {
				histogram.Release()
				return err
			}
			continue
		}
		stat, err := histogram.Stat(part)
		if err != nil {
			histogram.Release()
			return err
		}
		childDecision := j.policy.Decide(nextDepth, buildSet.bytes, stat.Bytes, uint64(j.config.MemoryBudget/4))
		if childDecision.Action == exechash.PartitionTerminalSkew {
			if err := j.terminalNestedLoop(ctx, bset, pset, em); err != nil {
				histogram.Release()
				return err
			}
			continue
		}
		if err := j.processSpilled(ctx, bset, pset, nextDepth, buildSet.bytes, em); err != nil {
			histogram.Release()
			return err
		}
	}
	histogram.Release()
	return nil
}

func (s *externalProbeSink) Finalize(ctx context.Context) error {
	j := s.join
	j.mu.Lock()
	defer j.mu.Unlock()
	if err := ctx.Err(); err != nil {
		return j.failLocked(err)
	}
	if j.failed != nil {
		return j.failed
	}
	if j.closed || !j.built {
		return j.failLocked(errors.New("external join not ready for finalize"))
	}
	for i := range j.partitions {
		p := &j.partitions[i]
		if p.probeSpill != nil {
			if err := p.probeSpill.Close(ctx); err != nil {
				return j.failLocked(err)
			}
		}
	}
	em := newStoredEmitter(j, s.downstream)
	for i := range j.partitions {
		p := &j.partitions[i]
		if !p.spilled {
			continue
		}
		buildSet := p.buildSpill.set()
		var probeSet partitionSet
		if p.probeSpill != nil {
			probeSet = p.probeSpill.set()
		}
		if err := j.processSpilled(ctx, buildSet, probeSet, 0, 0, em); err != nil {
			em.abort()
			return j.failLocked(err)
		}
		p.buildSpill.pages = nil
		if p.probeSpill != nil {
			p.probeSpill.pages = nil
		}
	}
	if err := em.flush(ctx); err != nil {
		em.abort()
		return j.failLocked(err)
	}
	if err := s.downstream.Finalize(ctx); err != nil {
		return j.failLocked(err)
	}
	j.releaseLocked()
	return nil
}

type storedEmitter struct {
	join       *ExternalHashJoin
	downstream pipeline.Sink
	cols       []*vector.Vector
	count      int
}

func newStoredEmitter(j *ExternalHashJoin, downstream pipeline.Sink) *storedEmitter {
	return &storedEmitter{join: j, downstream: downstream}
}
func (e *storedEmitter) ensure() error {
	if e.cols != nil {
		return nil
	}
	typesOut := make([]types.LogicalType, 0, len(e.join.config.LeftTypes)+len(e.join.config.RightTypes))
	typesOut = append(typesOut, e.join.config.LeftTypes...)
	typesOut = append(typesOut, e.join.config.RightTypes...)
	cols, err := newOutputColumns(e.join.config.Scope, e.join.config.Buffers, typesOut, e.join.config.RequiredOutput)
	if err != nil { return err }
	e.cols = cols
	return nil
}
func (e *storedEmitter) append(ctx context.Context, buildRow, probeRow []byte) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	if err := e.ensure(); err != nil {
		return err
	}
	leftCols := e.cols[:len(e.join.config.LeftTypes)]
	rightCols := e.cols[len(e.join.config.LeftTypes):]
	if e.join.buildSide == RightSide {
		if err := e.join.probeLayout.MaterializeSelectedRow(probeRow, leftCols, e.count, neededColumns(e.join.config.RequiredOutput, 0, len(leftCols))); err != nil {
			return err
		}
		if err := e.join.buildLayout.MaterializeSelectedRow(buildRow, rightCols, e.count, neededColumns(e.join.config.RequiredOutput, len(e.join.config.LeftTypes), len(rightCols))); err != nil {
			return err
		}
	} else {
		if err := e.join.buildLayout.MaterializeSelectedRow(buildRow, leftCols, e.count, neededColumns(e.join.config.RequiredOutput, 0, len(leftCols))); err != nil {
			return err
		}
		if err := e.join.probeLayout.MaterializeSelectedRow(probeRow, rightCols, e.count, neededColumns(e.join.config.RequiredOutput, len(e.join.config.LeftTypes), len(rightCols))); err != nil {
			return err
		}
	}
	e.count++
	if e.count == vector.DefaultVectorSize {
		return e.flush(ctx)
	}
	return nil
}
func (e *storedEmitter) flush(ctx context.Context) error {
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
func (e *storedEmitter) abort() {
	for _, v := range e.cols {
		if v != nil {
			v.Release()
		}
	}
	e.cols = nil
	e.count = 0
}
