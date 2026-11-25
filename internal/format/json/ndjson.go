package json

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"sync"

	"github.com/steamwo/fssql/internal/core/memory"
	"github.com/steamwo/fssql/internal/core/types"
	"github.com/steamwo/fssql/internal/core/vector"
	"github.com/steamwo/fssql/internal/introspection"
	"github.com/steamwo/fssql/internal/storage/fileset"
	"github.com/steamwo/fssql/internal/storage/vfs"
)

const defaultNDJSONSampleBytes int64 = 1 << 20
const defaultNDJSONRangeBytes int64 = 4 << 20
const defaultNDJSONSerialRangeBytes int64 = 1 << 20

func FrameNDJSON(input []byte, final bool, maxRecords int) (records [][]byte, consumed int, err error) {
	if maxRecords <= 0 {
		maxRecords = vector.DefaultVectorSize
	}
	start := 0
	for start < len(input) {
		relative := bytes.IndexByte(input[start:], '\n')
		if relative < 0 {
			break
		}
		i := start + relative
		line := bytes.TrimSpace(input[start:i])
		consumed = i + 1
		start = i + 1
		if len(line) == 0 {
			continue
		}
		records = append(records, line)
		if len(records) == maxRecords {
			return records, consumed, nil
		}
	}
	if final && start < len(input) && len(records) < maxRecords {
		line := bytes.TrimSpace(input[start:])
		consumed = len(input)
		if len(line) > 0 {
			records = append(records, line)
		}
	}
	return records, consumed, nil
}
func FirstNDJSONBoundary(input []byte) (int, bool) {
	if i := bytes.IndexByte(input, '\n'); i >= 0 {
		return i + 1, true
	}
	return 0, false
}

type NDJSONBindOptions struct {
	FileSet                     fileset.PlanOptions
	SampleBytes, MaxSampleBytes int64
	MaxRows                     int
}
type NDJSONBinding struct {
	scanSet  *fileset.ScanSet
	schema   InferredSchema
	explicit bool
}

func (b NDJSONBinding) ScanSet() *fileset.ScanSet { return b.scanSet }
func (b NDJSONBinding) Schema() InferredSchema {
	out := b.schema
	out.Fields = append([]types.StructField(nil), b.schema.Fields...)
	return out
}
func (b NDJSONBinding) MatchedFiles() int {
	if b.scanSet == nil {
		return 0
	}
	return b.scanSet.MatchedFiles()
}
func (b NDJSONBinding) Fingerprint() string {
	if b.scanSet == nil {
		return ""
	}
	return b.scanSet.Fingerprint()
}

type NDJSONBinder struct {
	registry *vfs.Registry
	planner  *fileset.Planner
	scope    *memory.Scope
	bm       *memory.BufferManager
}

func NewNDJSONBinder(r *vfs.Registry, s *memory.Scope, bm *memory.BufferManager) (*NDJSONBinder, error) {
	if r == nil || s == nil || bm == nil {
		return nil, errors.New("json: NDJSON binder requires registry, memory scope, and buffer manager")
	}
	return &NDJSONBinder{registry: r, planner: fileset.NewPlanner(r), scope: s, bm: bm}, nil
}
func (b *NDJSONBinder) Bind(ctx context.Context, pattern string, opts NDJSONBindOptions) (NDJSONBinding, error) {
	pattern, fileOpts, columns, err := fileOptions(pattern, opts.FileSet, b.registry)
	if err != nil {
		return NDJSONBinding{}, err
	}
	scanSet, err := b.planner.Plan(ctx, pattern, fileOpts)
	if err != nil {
		return NDJSONBinding{}, err
	}
	if len(columns) != 0 {
		return NDJSONBinding{scanSet: scanSet, schema: InferredSchema{Fields: columns}, explicit: true}, nil
	}
	file := scanSet.Files()[0]
	if file.Size <= 0 {
		return NDJSONBinding{}, fmt.Errorf("json: empty NDJSON file")
	}
	caps, err := b.registry.EffectiveCapabilities(ctx, file.URI)
	if err != nil {
		return NDJSONBinding{}, err
	}
	if !caps.Has(vfs.RandomRead) {
		return NDJSONBinding{}, fmt.Errorf("json: NDJSON bind requires RandomRead")
	}
	d, err := b.registry.DriverFor(file.URI)
	if err != nil {
		return NDJSONBinding{}, err
	}
	sample := opts.SampleBytes
	if sample <= 0 {
		sample = defaultNDJSONSampleBytes
	}
	maxSample := opts.MaxSampleBytes
	if maxSample <= 0 {
		maxSample = 8 << 20
	}
	if sample > file.Size {
		sample = file.Size
	}
	if maxSample > file.Size {
		maxSample = file.Size
	}
	maxRows := opts.MaxRows
	if maxRows <= 0 {
		maxRows = 2048
	}
	for {
		res, err := vfs.ReadInputRange(ctx, d.ReadRange, vfs.RangeRequest{URI: file.URI, Offset: 0, Length: sample, Expected: &file.Identity, Tag: memory.TagReadAhead}, b.scope, b.bm)
		if err != nil {
			return NDJSONBinding{}, err
		}
		// records 只是 managed range buffer 的 slice views；InferRecords 完成前不能 Release buffer。
		sampleData, _ := trimNDJSONUTF8BOM(res.Buffer.Bytes())
		records, _, _ := FrameNDJSON(sampleData, sample == file.Size, maxRows)
		if len(records) > 0 {
			schema, inferErr := InferRecords(records, maxRows)
			res.Buffer.Release()
			if inferErr == nil {
				return NDJSONBinding{scanSet: scanSet, schema: schema}, nil
			}
			if sample >= maxSample || sample >= file.Size {
				return NDJSONBinding{}, inferErr
			}
		} else {
			res.Buffer.Release()
			if sample >= maxSample || sample >= file.Size {
				return NDJSONBinding{}, fmt.Errorf("json: no complete NDJSON records")
			}
		}
		next := sample * 2
		if next > maxSample {
			next = maxSample
		}
		if next > file.Size {
			next = file.Size
		}
		if next == sample {
			return NDJSONBinding{}, fmt.Errorf("json: inference sample stalled")
		}
		sample = next
	}
}
func (b *NDJSONBinder) InspectSource(ctx context.Context, pattern string, opts NDJSONBindOptions) (introspection.SourceReport, error) {
	binding, err := b.Bind(ctx, pattern, opts)
	if err != nil {
		return introspection.SourceReport{}, err
	}
	if binding.explicit {
		return introspection.NewSourceReport(binding.Schema().Fields, introspection.SchemaSourceExplicit, introspection.ConfidenceExact, binding.MatchedFiles(), 0, binding.Fingerprint(), false, false), nil
	}
	return introspection.NewSourceReport(binding.Schema().Fields, introspection.SchemaSourceSampleInferred, introspection.ConfidenceSampled, binding.MatchedFiles(), 1, binding.Fingerprint(), false, false), nil
}

type NDJSONScanOptions struct {
	EnableParallel   bool
	TargetRangeBytes int64
	Projection       ProjectedPathSet
	CountOnly        bool // Internal COUNT(*) source: validate records without payload vectors.
}
type ndjsonJob struct {
	file           fileset.FileSnapshot
	offset, length int64
	serial         bool
	readHint       int64 // readHint 记录一个 batch 实际消费的字节数，避免串行窗口重复读取未消费尾部。
}
type NDJSONSource struct {
	registry         *vfs.Registry
	binding          NDJSONBinding
	scope            *memory.Scope
	bm               *memory.BufferManager
	projection       ProjectedPathSet
	countOnly        bool
	serialRangeBytes int64
	mu               sync.Mutex
	jobs             []ndjsonJob
	failed           error
	serialMu         sync.Mutex
}

func NewNDJSONSource(ctx context.Context, r *vfs.Registry, b NDJSONBinding, opts NDJSONScanOptions, scope *memory.Scope, bm *memory.BufferManager) (*NDJSONSource, error) {
	if r == nil || b.scanSet == nil || scope == nil || bm == nil {
		return nil, errors.New("json: invalid NDJSON source setup")
	}
	target := opts.TargetRangeBytes
	if target <= 0 {
		target = defaultNDJSONRangeBytes
	}
	serialTarget := target
	if serialTarget > defaultNDJSONSerialRangeBytes {
		serialTarget = defaultNDJSONSerialRangeBytes
	}
	s := &NDJSONSource{registry: r, binding: b, scope: scope, bm: bm, projection: opts.Projection, countOnly: opts.CountOnly, serialRangeBytes: serialTarget}
	for _, f := range b.scanSet.Files() {
		jobs, err := s.planFile(ctx, f, opts.EnableParallel, target)
		if err != nil {
			return nil, err
		}
		s.jobs = append(s.jobs, jobs...)
	}
	return s, nil
}
func (s *NDJSONSource) planFile(ctx context.Context, file fileset.FileSnapshot, parallel bool, target int64) ([]ndjsonJob, error) {
	if file.Size <= 0 {
		return nil, nil
	}
	caps, err := s.registry.EffectiveCapabilities(ctx, file.URI)
	if err != nil {
		return nil, err
	}
	if !caps.Has(vfs.RandomRead) {
		return nil, fmt.Errorf("json: NDJSON scan requires RandomRead")
	}
	serial := !(parallel && caps.Has(vfs.ParallelRandomRead))
	if serial || file.Size <= target {
		return []ndjsonJob{{file: file, length: file.Size, serial: serial}}, nil
	}
	var offsets []int64
	for base := target; base < file.Size; base += target {
		length := target
		if base+length > file.Size {
			length = file.Size - base
		}
		res, err := s.read(ctx, file, base, length)
		if err != nil {
			return nil, err
		}
		rel, ok := FirstNDJSONBoundary(res.Buffer.Bytes())
		res.Buffer.Release()
		if !ok {
			continue
		}
		abs := base + int64(rel)
		if abs < file.Size && (len(offsets) == 0 || abs > offsets[len(offsets)-1]) {
			offsets = append(offsets, abs)
		}
	}
	if len(offsets) == 0 {
		return []ndjsonJob{{file: file, length: file.Size, serial: true}}, nil
	}
	jobs := make([]ndjsonJob, 0, len(offsets)+1)
	start := int64(0)
	for _, end := range offsets {
		if end > start {
			jobs = append(jobs, ndjsonJob{file: file, offset: start, length: end - start})
			start = end
		}
	}
	if start < file.Size {
		jobs = append(jobs, ndjsonJob{file: file, offset: start, length: file.Size - start})
	}
	return jobs, nil
}
func (s *NDJSONSource) read(ctx context.Context, file fileset.FileSnapshot, off, n int64) (vfs.RangeResult, error) {
	d, err := s.registry.DriverFor(file.URI)
	if err != nil {
		return vfs.RangeResult{}, err
	}
	return vfs.ReadInputRange(ctx, d.ReadRange, vfs.RangeRequest{URI: file.URI, Offset: off, Length: n, Expected: &file.Identity, Tag: memory.TagReadAhead}, s.scope, s.bm)
}
func (s *NDJSONSource) take() (ndjsonJob, bool, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.failed != nil {
		return ndjsonJob{}, false, s.failed
	}
	if len(s.jobs) == 0 {
		return ndjsonJob{}, false, nil
	}
	j := s.jobs[0]
	s.jobs = s.jobs[1:]
	return j, true, nil
}
func (s *NDJSONSource) pushFront(j ndjsonJob) {
	if j.length <= 0 {
		return
	}
	s.mu.Lock()
	s.jobs = append([]ndjsonJob{j}, s.jobs...)
	s.mu.Unlock()
}
func (s *NDJSONSource) fail(err error) {
	s.mu.Lock()
	if s.failed == nil {
		s.failed = err
	}
	s.jobs = nil
	s.mu.Unlock()
}
func (s *NDJSONSource) Next(ctx context.Context) (*vector.DataChunk, bool, error) {
	for {
		if err := ctx.Err(); err != nil {
			return nil, false, err
		}
		j, ok, err := s.take()
		if err != nil {
			return nil, false, err
		}
		if !ok {
			return nil, false, nil
		}
		var c *vector.DataChunk
		var rem *ndjsonJob
		if j.serial {
			s.serialMu.Lock()
			c, rem, err = s.runJob(ctx, j)
			s.serialMu.Unlock()
		} else {
			c, rem, err = s.runJob(ctx, j)
		}
		if err != nil {
			err = fmt.Errorf("ndjson: read %s: %w", j.file.URI, err)
			s.fail(err)
			return nil, false, err
		}
		if rem != nil {
			s.pushFront(*rem)
		}
		if c != nil {
			return c, true, nil
		}
	}
}

// runJob bounds serial reads even when a logical job spans an entire object.
// FrameNDJSON leaves an incomplete trailing line unconsumed, so the next job
// resumes from the last complete newline without retaining source-owned buffers.
func (s *NDJSONSource) runJob(ctx context.Context, j ndjsonJob) (*vector.DataChunk, *ndjsonJob, error) {
	readLength := j.length
	if j.serial {
		hint := j.readHint
		if hint <= 0 || hint > s.serialRangeBytes {
			hint = s.serialRangeBytes
		}
		if readLength > hint {
			readLength = hint
		}
	}
	for {
		if err := ctx.Err(); err != nil {
			return nil, nil, err
		}
		res, err := s.read(ctx, j.file, j.offset, readLength)
		if err != nil {
			return nil, nil, err
		}
		final := readLength == j.length
		data := res.Buffer.Bytes()
		prefixConsumed := 0
		if j.offset == 0 {
			data, prefixConsumed = trimNDJSONUTF8BOM(data)
		}
		records, consumed, frameErr := FrameNDJSON(data, final, vector.DefaultVectorSize)
		if frameErr != nil {
			res.Buffer.Release()
			return nil, nil, frameErr
		}
		if consumed < 0 || consumed > len(data) {
			res.Buffer.Release()
			return nil, nil, fmt.Errorf("json: NDJSON framer consumed invalid byte count %d of %d", consumed, len(data))
		}
		if consumed == 0 && !final {
			res.Buffer.Release()
			readLength = growNDJSONSerialWindow(readLength, j.length)
			continue
		}
		nextOffset := j.offset + int64(prefixConsumed+consumed)
		endOffset := j.offset + j.length
		var rem *ndjsonJob
		if nextOffset < endOffset {
			r := j
			r.offset = nextOffset
			r.length = endOffset - nextOffset
			r.readHint = nextNDJSONSerialReadHint(consumed, len(records), s.serialRangeBytes)
			rem = &r
		}
		if len(records) == 0 {
			res.Buffer.Release()
			return nil, rem, nil
		}
		if s.countOnly {
			err := validateCountRecords(ctx, records, s.binding.schema.Fields, s.scope, s.bm)
			res.Buffer.Release()
			if err != nil {
				return nil, nil, err
			}
			return &vector.DataChunk{Cardinality: len(records)}, rem, nil
		}
		chunk, err := DecodeRecords(records, s.binding.schema.Fields, s.projection, s.scope, s.bm)
		res.Buffer.Release()
		if err != nil {
			return nil, nil, err
		}
		return chunk, rem, nil
	}
}

// trimNDJSONUTF8BOM 与 CSV 路径一致地忽略文件起始 BOM；FrameNDJSON 不把 BOM
// 视为空白，若不剥离会污染首条记录的 JSON 解析。
func trimNDJSONUTF8BOM(data []byte) ([]byte, int) {
	if len(data) >= 3 && data[0] == 0xEF && data[1] == 0xBB && data[2] == 0xBF {
		return data[3:], 3
	}
	return data, 0
}

// nextNDJSONSerialReadHint 让下一个窗口接近一个 batch 的实际字节数，避免重复读取尾部。
func nextNDJSONSerialReadHint(consumed, records int, limit int64) int64 {
	if consumed <= 0 || records <= 0 {
		return 0
	}
	target := int64(consumed) * int64(vector.DefaultVectorSize) / int64(records)
	if target < 1 {
		target = 1
	}
	if target > limit {
		target = limit
	}
	return target
}

func growNDJSONSerialWindow(current, remaining int64) int64 {
	if current >= remaining {
		return remaining
	}
	next := current * 2
	if next <= current || next > remaining {
		return remaining
	}
	return next
}
