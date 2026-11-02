package json

import (
	"bytes"
	"context"
	"errors"
	"fmt"

	"github.com/steamwo/fssql/internal/core/memory"
	"github.com/steamwo/fssql/internal/core/types"
	"github.com/steamwo/fssql/internal/introspection"
	"github.com/steamwo/fssql/internal/storage/fileset"
	"github.com/steamwo/fssql/internal/storage/vfs"
)

const (
	defaultJSONSampleBytes int64 = 1 << 20
	defaultJSONMaxBytes    int64 = 8 << 20
	defaultJSONMaxRows           = 2048
)

var errJSONNeedMoreData = errors.New("json: source sample needs more data")

// JSONBindOptions 控制普通 JSON source 的 bounded schema inference。
type JSONBindOptions struct {
	FileSet        fileset.PlanOptions // FileSet 交给统一 FileSetPlanner 冻结 source identity。
	SampleBytes    int64               // SampleBytes 是首次 inference sample 上限。
	MaxSampleBytes int64               // MaxSampleBytes 是 inference 的绝对读取上限；不会 silent whole-file fallback。
	MaxRows        int                 // MaxRows 限制 schema inference 参与的 object 数量。
}

// JSONBinding 是 Query/Inspect 共用的普通 JSON source Bind 结果。
type JSONBinding struct {
	scanSet *fileset.ScanSet // scanSet 是 statement 内不可变文件集合。
	schema  InferredSchema   // schema 来自现有 InferRecords typed inference。
}

// ScanSet 返回冻结 ScanSet；调用方不得修改其文件身份。
func (b JSONBinding) ScanSet() *fileset.ScanSet { return b.scanSet }

// Schema 返回 schema 副本，避免调用方修改 binding 内 slice。
func (b JSONBinding) Schema() InferredSchema {
	out := b.schema
	out.Fields = append([]types.StructField(nil), b.schema.Fields...)
	return out
}

// MatchedFiles 返回冻结 ScanSet 命中的文件数。
func (b JSONBinding) MatchedFiles() int {
	if b.scanSet == nil {
		return 0
	}
	return b.scanSet.MatchedFiles()
}

// Fingerprint 返回 FileSetPlanner 产生的 immutable dataset fingerprint。
func (b JSONBinding) Fingerprint() string {
	if b.scanSet == nil {
		return ""
	}
	return b.scanSet.Fingerprint()
}

// JSONBinder 是普通 JSON query/introspection 唯一 schema Bind 入口。
type JSONBinder struct {
	registry *vfs.Registry       // registry 是统一 VFS capability/driver registry。
	planner  *fileset.Planner    // planner 先冻结 ScanSet 再读取 sample。
	scope    *memory.Scope       // scope 约束 sample buffer 的 managed memory ownership。
	bm       *memory.BufferManager // bm 创建 managed range buffer。
}

// NewJSONBinder 创建普通 JSON SourceBinder。
func NewJSONBinder(registry *vfs.Registry, scope *memory.Scope, bm *memory.BufferManager) (*JSONBinder, error) {
	if registry == nil || scope == nil || bm == nil {
		return nil, errors.New("json: source binder requires registry, memory scope, and buffer manager")
	}
	return &JSONBinder{registry: registry, planner: fileset.NewPlanner(registry), scope: scope, bm: bm}, nil
}

// Bind 是 read_json 与 Source Introspection 共用的 schema-resolution path。
// 它只做 bounded sample inference；不会因为 sample 不足静默读取整个大文件。
func (b *JSONBinder) Bind(ctx context.Context, pattern string, opts JSONBindOptions) (JSONBinding, error) {
	if b == nil || b.registry == nil || b.planner == nil || b.scope == nil || b.bm == nil {
		return JSONBinding{}, errors.New("json: uninitialized source binder")
	}
	scanSet, err := b.planner.Plan(ctx, pattern, opts.FileSet)
	if err != nil {
		return JSONBinding{}, err
	}
	files := scanSet.Files()
	if len(files) == 0 {
		return JSONBinding{}, fileset.ErrNoMatches
	}
	file := files[0]
	if file.Size <= 0 {
		return JSONBinding{}, fmt.Errorf("json: cannot infer schema from empty file %s", file.URI.String())
	}
	caps, err := b.registry.EffectiveCapabilities(ctx, file.URI)
	if err != nil {
		return JSONBinding{}, err
	}
	if !caps.Has(vfs.RandomRead) {
		return JSONBinding{}, fmt.Errorf("json: schema inference requires RandomRead for %s", file.URI.String())
	}
	driver, err := b.registry.DriverFor(file.URI)
	if err != nil {
		return JSONBinding{}, err
	}

	sample := opts.SampleBytes
	if sample <= 0 {
		sample = defaultJSONSampleBytes
	}
	maxSample := opts.MaxSampleBytes
	if maxSample <= 0 {
		maxSample = defaultJSONMaxBytes
	}
	if maxSample < sample {
		maxSample = sample
	}
	if sample > file.Size {
		sample = file.Size
	}
	if maxSample > file.Size {
		maxSample = file.Size
	}
	maxRows := opts.MaxRows
	if maxRows <= 0 {
		maxRows = defaultJSONMaxRows
	}

	for {
		result, readErr := driver.ReadRange(ctx, vfs.RangeRequest{
			URI: file.URI, Offset: 0, Length: sample, Expected: &file.Identity, Tag: memory.TagReadAhead,
		}, b.scope, b.bm)
		if readErr != nil {
			return JSONBinding{}, readErr
		}
		records, frameErr := FrameJSONRecords(result.Buffer.Bytes(), sample == file.Size, maxRows)
		result.Buffer.Release()
		if frameErr == nil && len(records) > 0 {
			schema, inferErr := InferRecords(records, maxRows)
			if inferErr == nil {
				return JSONBinding{scanSet: scanSet, schema: schema}, nil
			}
			return JSONBinding{}, inferErr
		}
		if frameErr != nil && !errors.Is(frameErr, errJSONNeedMoreData) {
			return JSONBinding{}, frameErr
		}
		if sample >= maxSample || sample >= file.Size {
			if frameErr != nil {
				return JSONBinding{}, frameErr
			}
			return JSONBinding{}, fmt.Errorf("json: no complete object records in bounded sample")
		}
		next := sample * 2
		if next > maxSample {
			next = maxSample
		}
		if next > file.Size {
			next = file.Size
		}
		if next == sample {
			return JSONBinding{}, fmt.Errorf("json: inference sample stalled")
		}
		sample = next
	}
}

// InspectSource 将同一个 JSON Bind 结果投影为统一 SourceReport。
func (b *JSONBinder) InspectSource(ctx context.Context, pattern string, opts JSONBindOptions) (introspection.SourceReport, error) {
	binding, err := b.Bind(ctx, pattern, opts)
	if err != nil {
		return introspection.SourceReport{}, err
	}
	return introspection.NewSourceReport(
		binding.Schema().Fields,
		introspection.SchemaSourceSampleInferred,
		introspection.ConfidenceSampled,
		binding.MatchedFiles(), 1, binding.Fingerprint(), false, false,
	), nil
}

// FrameJSONRecords 从 bounded sample 中提取 top-level object rows。
// 支持单 object、object array 与 whitespace-separated object values；不使用 map[string]any。
func FrameJSONRecords(input []byte, final bool, maxRows int) ([][]byte, error) {
	if maxRows <= 0 {
		maxRows = defaultJSONMaxRows
	}
	data := bytes.TrimSpace(input)
	if len(data) == 0 {
		if final {
			return nil, fmt.Errorf("json: empty source")
		}
		return nil, errJSONNeedMoreData
	}
	if data[0] == '[' {
		return frameJSONArray(data, final, maxRows)
	}
	return frameJSONValues(data, final, maxRows)
}

// frameJSONArray 提取 top-level array 中已经完整的 object elements。
func frameJSONArray(data []byte, final bool, maxRows int) ([][]byte, error) {
	pos := skipJSONSpace(data, 1)
	var out [][]byte
	for {
		if pos >= len(data) {
			if final {
				return nil, fmt.Errorf("json: unterminated top-level array")
			}
			return nil, errJSONNeedMoreData
		}
		if data[pos] == ']' {
			if len(out) == 0 {
				return nil, fmt.Errorf("json: top-level array contains no object rows")
			}
			return out, nil
		}
		start := pos
		end, complete, err := scanJSONValueEnd(data, start)
		if err != nil {
			return nil, err
		}
		if !complete {
			if len(out) > 0 {
				return out, nil
			}
			if final {
				return nil, fmt.Errorf("json: truncated top-level array element")
			}
			return nil, errJSONNeedMoreData
		}
		if data[start] != '{' {
			return nil, fmt.Errorf("json: top-level array elements must be objects")
		}
		out = append(out, bytes.TrimSpace(data[start:end]))
		if len(out) >= maxRows {
			return out, nil
		}
		pos = skipJSONSpace(data, end)
		if pos >= len(data) {
			if final {
				return nil, fmt.Errorf("json: unterminated top-level array")
			}
			return out, nil
		}
		switch data[pos] {
		case ',':
			pos = skipJSONSpace(data, pos+1)
		case ']':
			return out, nil
		default:
			return nil, fmt.Errorf("json: expected comma or ] after array element")
		}
	}
}

// frameJSONValues 支持一个或多个由 whitespace 分隔的 top-level object values。
func frameJSONValues(data []byte, final bool, maxRows int) ([][]byte, error) {
	var out [][]byte
	pos := 0
	for pos < len(data) {
		pos = skipJSONSpace(data, pos)
		if pos >= len(data) {
			break
		}
		if data[pos] != '{' {
			return nil, fmt.Errorf("json: top-level values must be objects or an object array")
		}
		end, complete, err := scanJSONValueEnd(data, pos)
		if err != nil {
			return nil, err
		}
		if !complete {
			if len(out) > 0 {
				return out, nil
			}
			if final {
				return nil, fmt.Errorf("json: truncated top-level object")
			}
			return nil, errJSONNeedMoreData
		}
		out = append(out, bytes.TrimSpace(data[pos:end]))
		if len(out) >= maxRows {
			return out, nil
		}
		pos = end
	}
	if len(out) == 0 {
		if final {
			return nil, fmt.Errorf("json: no object records")
		}
		return nil, errJSONNeedMoreData
	}
	return out, nil
}

// scanJSONValueEnd 在不解码 payload 的前提下找到一个完整 JSON value 的结束位置。
func scanJSONValueEnd(data []byte, start int) (end int, complete bool, err error) {
	if start < 0 || start >= len(data) {
		return 0, false, nil
	}
	open := data[start]
	if open != '{' && open != '[' {
		return 0, false, fmt.Errorf("json: expected object/array value")
	}
	stack := make([]byte, 0, 16)
	inString := false
	escaped := false
	for i := start; i < len(data); i++ {
		c := data[i]
		if inString {
			if escaped {
				escaped = false
				continue
			}
			if c == '\\' {
				escaped = true
				continue
			}
			if c == '"' {
				inString = false
			}
			continue
		}
		switch c {
		case '"':
			inString = true
		case '{', '[':
			stack = append(stack, c)
		case '}', ']':
			if len(stack) == 0 {
				return 0, false, fmt.Errorf("json: unexpected closing delimiter")
			}
			top := stack[len(stack)-1]
			if (top == '{' && c != '}') || (top == '[' && c != ']') {
				return 0, false, fmt.Errorf("json: mismatched closing delimiter")
			}
			stack = stack[:len(stack)-1]
			if len(stack) == 0 {
				return i + 1, true, nil
			}
		}
	}
	return 0, false, nil
}

// skipJSONSpace 跳过 JSON grammar 允许的 ASCII whitespace。
func skipJSONSpace(data []byte, pos int) int {
	for pos < len(data) {
		switch data[pos] {
		case ' ', '\t', '\r', '\n':
			pos++
		default:
			return pos
		}
	}
	return pos
}
