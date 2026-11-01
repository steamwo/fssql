package csv

import (
	"context"
	"errors"
	"fmt"
	"sync"

	"github.com/steamwo/fssql/internal/core/memory"
	"github.com/steamwo/fssql/internal/core/vector"
	"github.com/steamwo/fssql/internal/storage/fileset"
	"github.com/steamwo/fssql/internal/storage/vfs"
)

const defaultTargetRangeBytes int64 = 4 << 20

type ScanOptions struct {
	EnableParallel   bool
	TargetRangeBytes int64
}

type rangeJob struct {
	file       fileset.FileSnapshot
	offset     int64
	length     int64
	skipHeader bool
	serial     bool
}

// Source is a concurrency-safe Push Pipeline source. Next acquires range jobs
// under a small mutex but performs VFS I/O and CSV decoding outside it, allowing
// pipeline workers to scan independent safe ranges concurrently.
type Source struct {
	registry *vfs.Registry
	binding  SourceBinding
	scope    *memory.Scope
	bm       *memory.BufferManager
	lex      LexConfig
	null     []byte

	mu       sync.Mutex
	jobs     []rangeJob
	failed   error
	serialMu sync.Mutex
}

func NewSource(ctx context.Context, registry *vfs.Registry, binding SourceBinding, opts ScanOptions, scope *memory.Scope, bm *memory.BufferManager) (*Source, error) {
	if registry == nil || binding.scanSet == nil || scope == nil || bm == nil {
		return nil, errors.New("csv: source requires registry, binding, memory scope, and buffer manager")
	}
	lex := binding.inference.Lex
	if lex == (LexConfig{}) { lex = DefaultLexConfig() }
	target := opts.TargetRangeBytes
	if target <= 0 { target = defaultTargetRangeBytes }
	s := &Source{registry: registry, binding: binding, scope: scope, bm: bm, lex: lex, null: append([]byte(nil), binding.inference.NullLiteral...)}
	for _, file := range binding.scanSet.Files() {
		jobs, err := s.planFile(ctx, file, opts.EnableParallel, target)
		if err != nil { return nil, err }
		s.jobs = append(s.jobs, jobs...)
	}
	return s, nil
}

func (s *Source) planFile(ctx context.Context, file fileset.FileSnapshot, parallel bool, target int64) ([]rangeJob, error) {
	if file.Size <= 0 { return nil, nil }
	caps, err := s.registry.EffectiveCapabilities(ctx, file.URI)
	if err != nil { return nil, err }
	if !caps.Has(vfs.RandomRead) {
		return nil, fmt.Errorf("csv: scan requires RandomRead for %s", file.URI.String())
	}
	serial := !(parallel && caps.Has(vfs.ParallelRandomRead))
	if serial || file.Size <= target {
		return []rangeJob{{file: file, length: file.Size, skipHeader: s.binding.inference.Header, serial: serial}}, nil
	}
	offsets, err := s.planSafeOffsets(ctx, file, target)
	if err != nil { return nil, err }
	if len(offsets) == 0 {
		return []rangeJob{{file: file, length: file.Size, skipHeader: s.binding.inference.Header, serial: true}}, nil
	}
	jobs := make([]rangeJob, 0, len(offsets)+1)
	start := int64(0)
	for _, end := range offsets {
		if end <= start || end >= file.Size { continue }
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
	if count < 2 { return nil, nil }
	summaries := make([]ChunkSummary, count)
	for i := 0; i < count; i++ {
		off := int64(i) * target
		length := target
		if off+length > file.Size { length = file.Size - off }
		result, err := s.read(ctx, file, off, length)
		if err != nil { return nil, err }
		summary, sumErr := SummarizeChunk(result.Buffer.Bytes(), s.lex)
		result.Buffer.Release()
		if sumErr != nil { return nil, fmt.Errorf("csv: summarize range %d: %w", i, sumErr) }
		summaries[i] = summary
	}
	starts, finalState, err := ResolveChunkStarts(summaries)
	if err != nil { return nil, err }
	if err := ValidateFinalBoundaryState(s.lex, finalState); err != nil { return nil, err }

	offsets := make([]int64, 0, count-1)
	for i := 1; i < count; i++ {
		base := int64(i) * target
		length := target
		if base+length > file.Size { length = file.Size - base }
		result, err := s.read(ctx, file, base, length)
		if err != nil { return nil, err }
		rel, ok, _, boundaryErr := FirstSafeRecordBoundary(result.Buffer.Bytes(), s.lex, starts[i])
		result.Buffer.Release()
		if boundaryErr != nil { return nil, boundaryErr }
		if !ok { continue }
		absolute := base + int64(rel)
		if absolute <= 0 || absolute >= file.Size { continue }
		if len(offsets) == 0 || absolute > offsets[len(offsets)-1] { offsets = append(offsets, absolute) }
	}
	return offsets, nil
}

func (s *Source) read(ctx context.Context, file fileset.FileSnapshot, offset, length int64) (vfs.RangeResult, error) {
	driver, err := s.registry.DriverFor(file.URI)
	if err != nil { return vfs.RangeResult{}, err }
	return driver.ReadRange(ctx, vfs.RangeRequest{URI: file.URI, Offset: offset, Length: length, Expected: &file.Identity, Tag: memory.TagReadAhead}, s.scope, s.bm)
}

func (s *Source) takeJob() (rangeJob, bool, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.failed != nil { return rangeJob{}, false, s.failed }
	if len(s.jobs) == 0 { return rangeJob{}, false, nil }
	job := s.jobs[0]
	s.jobs = s.jobs[1:]
	return job, true, nil
}

func (s *Source) pushFront(job rangeJob) {
	if job.length <= 0 { return }
	s.mu.Lock()
	s.jobs = append([]rangeJob{job}, s.jobs...)
	s.mu.Unlock()
}

func (s *Source) fail(err error) {
	s.mu.Lock()
	if s.failed == nil { s.failed = err }
	s.jobs = nil
	s.mu.Unlock()
}

func (s *Source) Next(ctx context.Context) (*vector.DataChunk, bool, error) {
	for {
		if err := ctx.Err(); err != nil { return nil, false, err }
		job, ok, err := s.takeJob()
		if err != nil { return nil, false, err }
		if !ok { return nil, false, nil }
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
			return nil, false, err
		}
		if remainder != nil { s.pushFront(*remainder) }
		if chunk == nil { continue }
		return chunk, true, nil
	}
}

func (s *Source) runJob(ctx context.Context, job rangeJob) (*vector.DataChunk, *rangeJob, error) {
	result, err := s.read(ctx, job.file, job.offset, job.length)
	if err != nil { return nil, nil, err }
	defer result.Buffer.Release()
	data := result.Buffer.Bytes()
	base := job.offset
	if job.skipHeader {
		headerBytes, ok, _, boundaryErr := FirstSafeRecordBoundary(data, s.lex, BoundaryState{Lex: StateFieldStart})
		if boundaryErr != nil { return nil, nil, boundaryErr }
		if !ok { return nil, nil, fmt.Errorf("csv: header row does not end inside planned range for %s", job.file.URI.String()) }
		data = data[headerBytes:]
		base += int64(headerBytes)
	}
	if len(data) == 0 { return nil, nil, nil }
	chunk, consumed, err := DecodeChunk(data, s.binding.Types(), DecodeOptions{Lex: s.lex, Final: true, NullLiteral: s.null}, s.scope, s.bm)
	if err != nil { return nil, nil, err }
	if consumed < 0 || consumed > len(data) {
		if chunk != nil { chunk.Release() }
		return nil, nil, fmt.Errorf("csv: decoder consumed invalid byte count %d of %d", consumed, len(data))
	}
	var remainder *rangeJob
	if consumed < len(data) {
		r := job
		r.offset = base + int64(consumed)
		r.length = int64(len(data) - consumed)
		r.skipHeader = false
		remainder = &r
	}
	return chunk, remainder, nil
}
