package csv

import (
	"context"
	"errors"
	"fmt"
	"sync"

	"github.com/steamwo/fssql/internal/core/memory"
	"github.com/steamwo/fssql/internal/core/types"
	"github.com/steamwo/fssql/internal/core/vector"
	"github.com/steamwo/fssql/internal/storage/fileset"
	"github.com/steamwo/fssql/internal/storage/vfs"
)

const defaultTargetRangeBytes int64 = 4 << 20
const defaultSerialRangeBytes int64 = 1 << 20

type ScanOptions struct {
	EnableParallel   bool
	TargetRangeBytes int64
	Limit            uint64 // Safe pre-filter row limit; 0 means unlimited.
	Projection       []int  // Empty retains every bound input column.
	CountOnly        bool   // Internal aggregate scan: validate records without payload vectors.
}

type rangeJob struct {
	file       fileset.FileSnapshot
	offset     int64
	length     int64
	skipHeader bool
	serial     bool
	readHint   int64 // readHint 记录一个 batch 实际消费的字节数，避免串行窗口重复读取未消费尾部。
}

// Source is a concurrency-safe Push Pipeline source. Next acquires range jobs
// under a small mutex but performs VFS I/O and CSV decoding outside it, allowing
// pipeline workers to scan independent safe ranges concurrently.
type Source struct {
	registry         *vfs.Registry
	binding          SourceBinding
	scope            *memory.Scope
	bm               *memory.BufferManager
	lex              LexConfig
	null             []byte
	serialRangeBytes int64

	mu             sync.Mutex
	jobs           []rangeJob
	failed         error
	closed         bool
	serialMu       sync.Mutex
	limitMu        sync.Mutex
	limit, emitted uint64
	projection     []int
	countOnly      bool
	types          []types.LogicalType
	window         *memory.Buffer // Protected by serialMu; retained across serial batches.
	windowURI      string
	windowOffset   int64
}

func NewSource(ctx context.Context, registry *vfs.Registry, binding SourceBinding, opts ScanOptions, scope *memory.Scope, bm *memory.BufferManager) (*Source, error) {
	if registry == nil || binding.scanSet == nil || scope == nil || bm == nil {
		return nil, errors.New("csv: source requires registry, binding, memory scope, and buffer manager")
	}
	lex := binding.inference.Lex
	if lex == (LexConfig{}) {
		lex = DefaultLexConfig()
	}
	target := opts.TargetRangeBytes
	if target <= 0 {
		target = defaultTargetRangeBytes
	}
	serialTarget := target
	if serialTarget > defaultSerialRangeBytes {
		serialTarget = defaultSerialRangeBytes
	}
	s := &Source{
		registry:         registry,
		binding:          binding,
		scope:            scope,
		bm:               bm,
		lex:              lex,
		null:             append([]byte(nil), binding.inference.NullLiteral...),
		serialRangeBytes: serialTarget,
		limit:            opts.Limit,
		projection:       append([]int(nil), opts.Projection...),
		countOnly:        opts.CountOnly,
		types:            binding.Types(),
	}
	for _, index := range s.projection {
		if index < 0 || index >= len(s.types) {
			return nil, fmt.Errorf("csv: projection index %d out of range", index)
		}
	}
	// Planning parallel quote-safe boundaries reads the whole file. A small
	// LIMIT instead uses the bounded sequential path and plans files lazily.
	if s.limit > 0 {
		opts.EnableParallel = false
	}
	for _, file := range binding.scanSet.Files() {
		jobs, err := s.planFile(ctx, file, opts.EnableParallel, target)
		if err != nil {
			return nil, err
		}
		s.jobs = append(s.jobs, jobs...)
	}
	return s, nil
}

func (s *Source) planFile(ctx context.Context, file fileset.FileSnapshot, parallel bool, target int64) ([]rangeJob, error) {
	if file.Size <= 0 {
		return nil, nil
	}
	caps, err := s.registry.EffectiveCapabilities(ctx, file.URI)
	if err != nil {
		return nil, err
	}
	if !caps.Has(vfs.RandomRead) {
		return nil, fmt.Errorf("csv: scan requires RandomRead for %s", file.URI.String())
	}
	serial := !(parallel && caps.Has(vfs.ParallelRandomRead))
	if serial || file.Size <= target {
		return []rangeJob{{file: file, length: file.Size, skipHeader: s.binding.inference.Header, serial: serial}}, nil
	}
	offsets, err := s.planSafeOffsets(ctx, file, target)
	if err != nil {
		return nil, err
	}
	if len(offsets) == 0 {
		return []rangeJob{{file: file, length: file.Size, skipHeader: s.binding.inference.Header, serial: true}}, nil
	}
	jobs := make([]rangeJob, 0, len(offsets)+1)
	start := int64(0)
	for _, end := range offsets {
		if end <= start || end >= file.Size {
			continue
		}
		jobs = append(jobs, rangeJob{file: file, offset: start, length: end - start, skipHeader: start == 0 && s.binding.inference.Header})
		start = end
	}
	if start < file.Size {
		jobs = append(jobs, rangeJob{file: file, offset: start, length: file.Size - start, skipHeader: start == 0 && s.binding.inference.Header})
	}
	return jobs, nil
}

func (s *Source) planSafeOffsets(ctx context.Context, file fileset.FileSnapshot, target int64) ([]int64, error) {
	count := int((file.Size + target - 1) / target)
	if count < 2 {
		return nil, nil
	}
	summaries := make([]ChunkSummary, count)
	for i := 0; i < count; i++ {
		off := int64(i) * target
		length := target
		if off+length > file.Size {
			length = file.Size - off
		}
		result, err := s.read(ctx, file, off, length)
		if err != nil {
			return nil, err
		}
		summary, sumErr := SummarizeChunk(result.Buffer.Bytes(), s.lex)
		result.Buffer.Release()
		if sumErr != nil {
			return nil, fmt.Errorf("csv: summarize range %d: %w", i, sumErr)
		}
		summaries[i] = summary
	}
	starts, finalState, err := ResolveChunkStarts(summaries)
	if err != nil {
		return nil, err
	}
	if err := ValidateFinalBoundaryState(s.lex, finalState); err != nil {
		return nil, err
	}

	offsets := make([]int64, 0, count-1)
	for i := 1; i < count; i++ {
		base := int64(i) * target
		length := target
		if base+length > file.Size {
			length = file.Size - base
		}
		result, err := s.read(ctx, file, base, length)
		if err != nil {
			return nil, err
		}
		rel, ok, _, boundaryErr := FirstSafeRecordBoundary(result.Buffer.Bytes(), s.lex, starts[i])
		result.Buffer.Release()
		if boundaryErr != nil {
			return nil, boundaryErr
		}
		if !ok {
			continue
		}
		absolute := base + int64(rel)
		if absolute <= 0 || absolute >= file.Size {
			continue
		}
		if len(offsets) == 0 || absolute > offsets[len(offsets)-1] {
			offsets = append(offsets, absolute)
		}
	}
	return offsets, nil
}

func (s *Source) read(ctx context.Context, file fileset.FileSnapshot, offset, length int64) (vfs.RangeResult, error) {
	driver, err := s.registry.DriverFor(file.URI)
	if err != nil {
		return vfs.RangeResult{}, err
	}
	return driver.ReadRange(ctx, vfs.RangeRequest{URI: file.URI, Offset: offset, Length: length, Expected: &file.Identity, Tag: memory.TagReadAhead}, s.scope, s.bm)
}

func (s *Source) takeJob() (rangeJob, bool, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.failed != nil {
		return rangeJob{}, false, s.failed
	}
	if s.closed {
		return rangeJob{}, false, nil
	}
	if len(s.jobs) == 0 {
		return rangeJob{}, false, nil
	}
	job := s.jobs[0]
	s.jobs = s.jobs[1:]
	return job, true, nil
}

func (s *Source) pushFront(job rangeJob) {
	if job.length <= 0 {
		return
	}
	s.mu.Lock()
	if s.closed || s.failed != nil {
		s.mu.Unlock()
		return
	}
	s.jobs = append([]rangeJob{job}, s.jobs...)
	s.mu.Unlock()
}

func (s *Source) fail(err error) {
	s.mu.Lock()
	if s.failed == nil {
		s.failed = err
	}
	s.jobs = nil
	s.mu.Unlock()
}

func (s *Source) Next(ctx context.Context) (*vector.DataChunk, bool, error) {
	if s.limit > 0 {
		s.limitMu.Lock()
		defer s.limitMu.Unlock()
		if err := ctx.Err(); err != nil {
			_ = s.Close()
			return nil, false, err
		}
		if s.emitted >= s.limit {
			_ = s.Close()
			return nil, false, nil
		}
	}
	for {
		if err := ctx.Err(); err != nil {
			_ = s.Close()
			return nil, false, err
		}
		job, ok, err := s.takeJob()
		if err != nil {
			return nil, false, err
		}
		if !ok {
			s.releaseInputWindow()
			return nil, false, nil
		}
		var chunk *vector.DataChunk
		var remainder *rangeJob
		if job.serial {
			s.serialMu.Lock()
			chunk, remainder, err = s.runJob(ctx, job)
			s.serialMu.Unlock()
		} else {
			chunk, remainder, err = s.runJob(ctx, job)
		}
		if err != nil {
			s.fail(err)
			_ = s.Close()
			return nil, false, err
		}
		if remainder != nil {
			s.pushFront(*remainder)
		}
		if chunk == nil {
			continue
		}
		if s.limit > 0 {
			s.emitted += uint64(chunk.Cardinality)
			if s.emitted >= s.limit {
				_ = s.Close()
			}
		}
		return chunk, true, nil
	}
}

// runJob keeps serial scans bounded even when the logical job represents an
// entire file. DecodeChunk leaves a trailing partial record unconsumed, so the
// next job starts at the last complete record boundary and re-reads at most one
// bounded window instead of reserving the whole file.
func (s *Source) runJob(ctx context.Context, job rangeJob) (*vector.DataChunk, *rangeJob, error) {
	readLength := job.length
	if job.serial {
		hint := s.serialRangeBytes
		if readLength > hint {
			readLength = hint
		}
	}
	for {
		if err := ctx.Err(); err != nil {
			return nil, nil, err
		}
		result, err := s.readWindow(ctx, job, readLength)
		if err != nil {
			return nil, nil, err
		}
		data := result.Buffer.Bytes()
		if job.serial {
			data = data[job.offset-s.windowOffset:]
		}
		readLength = int64(len(data))
		prefixConsumed := 0
		if job.offset == 0 {
			data, prefixConsumed = trimCSVUTF8BOM(data)
		}
		if job.skipHeader {
			headerBytes, ok, _, boundaryErr := FirstSafeRecordBoundary(data, s.lex, BoundaryState{Lex: StateFieldStart})
			if boundaryErr != nil {
				s.releaseRead(job, result.Buffer)
				return nil, nil, boundaryErr
			}
			if !ok {
				s.releaseRead(job, result.Buffer)
				if readLength == job.length {
					return nil, nil, fmt.Errorf("csv: header row does not end inside file for %s", job.file.URI.String())
				}
				readLength = growSerialWindow(readLength, job.length)
				continue
			}
			prefixConsumed += headerBytes
			data = data[headerBytes:]
		}
		final := readLength == job.length
		maxRows := vector.DefaultVectorSize
		if s.limit > 0 && s.limit-s.emitted < uint64(maxRows) {
			maxRows = int(s.limit - s.emitted)
		}
		var chunk *vector.DataChunk
		var consumed int
		var decodeErr error
		if s.countOnly {
			var rows int
			rows, consumed, decodeErr = countCSVRecords(data, s.lex, final, maxRows, len(s.types))
			if rows > 0 && decodeErr == nil {
				chunk = &vector.DataChunk{Cardinality: rows}
			}
		} else {
			chunk, consumed, decodeErr = DecodeChunk(data, s.types, DecodeOptions{DateFormat:s.binding.inference.DateFormat,TimestampFormat:s.binding.inference.TimestampFormat,Formats:s.binding.schema.Formats,Lex: s.lex, Final: final, NullLiteral: s.null, Projection: s.projection, MaxRows: maxRows}, s.scope, s.bm)
		}
		if !job.serial {
			result.Buffer.Release()
		}
		if decodeErr != nil {
			return nil, nil, decodeErr
		}
		if consumed < 0 || consumed > len(data) {
			if chunk != nil {
				chunk.Release()
			}
			return nil, nil, fmt.Errorf("csv: decoder consumed invalid byte count %d of %d", consumed, len(data))
		}
		totalConsumed := prefixConsumed + consumed
		if totalConsumed == 0 && !final {
			if chunk != nil {
				chunk.Release()
			}
			if readLength < s.serialRangeBytes {
				readLength = min(s.serialRangeBytes, job.length)
			} else {
				readLength = growSerialWindow(readLength, job.length)
			}
			s.releaseWindow()
			continue
		}
		nextOffset := job.offset + int64(totalConsumed)
		endOffset := job.offset + job.length
		var remainder *rangeJob
		if nextOffset < endOffset {
			r := job
			r.offset = nextOffset
			r.length = endOffset - nextOffset
			r.skipHeader = false
			r.readHint = nextSerialReadHint(consumed, chunk, s.serialRangeBytes)
			remainder = &r
		}
		if job.serial && remainder == nil {
			s.releaseWindow()
		}
		return chunk, remainder, nil
	}
}

// readWindow reuses the unconsumed bytes of a managed sequential window.
// Partial records are re-read from their start; growth still reserves first.
func (s *Source) readWindow(ctx context.Context, job rangeJob, length int64) (vfs.RangeResult, error) {
	if !job.serial {
		return s.read(ctx, job.file, job.offset, length)
	}
	if s.window != nil && s.windowURI == job.file.URI.String() && job.offset >= s.windowOffset && job.offset < s.windowOffset+int64(s.window.Len()) {
		return vfs.RangeResult{Buffer: s.window}, nil
	}
	s.releaseWindow()
	result, err := s.read(ctx, job.file, job.offset, length)
	if err != nil {
		return result, err
	}
	s.window, s.windowURI, s.windowOffset = result.Buffer, job.file.URI.String(), job.offset
	return result, nil
}

func (s *Source) releaseWindow() {
	if s.window != nil {
		s.window.Release()
		s.window = nil
	}
}

func (s *Source) releaseRead(job rangeJob, buffer *memory.Buffer) {
	if job.serial {
		s.releaseWindow()
	} else {
		buffer.Release()
	}
}

// Close is called after pipeline workers drain, or on direct-source failure.
func (s *Source) Close() error {
	s.mu.Lock()
	s.closed = true
	s.jobs = nil
	s.mu.Unlock()
	s.releaseInputWindow()
	return nil
}

func (s *Source) releaseInputWindow() {
	s.serialMu.Lock()
	s.releaseWindow()
	s.serialMu.Unlock()
}

// trimCSVUTF8BOM 与 DuckDB 一致地忽略文件起始的 UTF-8 BOM；BOM 属于文件头，
// 不进入首列值，也不影响后续 offset 计算。
func trimCSVUTF8BOM(data []byte) ([]byte, int) {
	if len(data) >= 3 && data[0] == 0xEF && data[1] == 0xBB && data[2] == 0xBF {
		return data[3:], 3
	}
	return data, 0
}

// nextSerialReadHint 让下一个窗口接近一个 batch 的实际字节数。若窗口大于
// DecodeChunk 单批行数，继续按原窗口读取会在每次 Next 重复读取未消费尾部。
func nextSerialReadHint(consumed int, chunk *vector.DataChunk, limit int64) int64 {
	rows := 0
	if chunk != nil {
		rows = chunk.Cardinality
	}
	if consumed <= 0 || rows <= 0 {
		return 0
	}
	target := int64(consumed) * int64(vector.DefaultVectorSize) / int64(rows)
	if target < 1 {
		target = 1
	}
	if target > limit {
		target = limit
	}
	return target
}

func growSerialWindow(current, remaining int64) int64 {
	if current >= remaining {
		return remaining
	}
	next := current * 2
	if next <= current || next > remaining {
		return remaining
	}
	return next
}
