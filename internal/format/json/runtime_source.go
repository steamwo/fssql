package json

import (
	"context"
	"fmt"
	"sync"

	"github.com/steamwo/fssql/internal/core/memory"
	"github.com/steamwo/fssql/internal/core/vector"
	"github.com/steamwo/fssql/internal/storage/fileset"
	"github.com/steamwo/fssql/internal/storage/vfs"
)

const defaultJSONRuntimeRangeBytes int64 = 1 << 20

// JSONRuntimeSourceConfig 描述 Stable read_json runtime source；Pattern/Fingerprint 必须来自 Statement 已绑定 ScanSpec。
type JSONRuntimeSourceConfig struct {
	Binder              *JSONBinder // Binder 必须与 SQL Bind/InspectSource 共用真实 JSONBinder。
	Pattern             string      // Pattern 是 Stable read_json 的 source literal。
	ExpectedFingerprint string      // ExpectedFingerprint 是 Statement 冻结的 DatasetFingerprint。
	RangeBytes          int64       // RangeBytes 仅控制串行 RandomRead 粒度；0 使用默认值。
}

type jsonRuntimeMode uint8

const (
	jsonRuntimeUnknown jsonRuntimeMode = iota
	jsonRuntimeValues
	jsonRuntimeArray
)

// jsonRuntimeFrameState 保存单个文件的 top-level framing 状态；它不保存 payload 数据。
type jsonRuntimeFrameState struct {
	mode             jsonRuntimeMode // mode 区分 whitespace values 与 object array。
	arrayExpectValue bool            // arrayExpectValue 表示 array 当前应读取 object/结束括号。
	arrayDone        bool            // arrayDone 表示已读取 top-level ]，后续只允许 whitespace。
	sawRecord        bool            // sawRecord 用于拒绝普通 values 模式的空文件。
}

// JSONRuntimeSource 串行扫描 frozen ScanSet 中的普通 JSON 文件。
// Advanced structural parallelism 仍是 Experimental，因此多 worker 对 Next 的调用由 mu 串行化。
type JSONRuntimeSource struct {
	mu        sync.Mutex             // mu 保护文件游标、carry、framing state 与 Close。
	registry  *vfs.Registry          // registry 是统一 VFS/capability 入口。
	binding   JSONBinding            // binding 保存 frozen ScanSet 与 typed schema。
	scope     *memory.Scope          // scope 约束 carry/range/decode managed memory。
	bm        *memory.BufferManager  // bm 分配 managed carry。
	files     []fileset.FileSnapshot // files 是 binding ScanSet 的不可变快照副本。
	rangeSize int64                  // rangeSize 是每次 RandomRead 最大字节数。

	fileIndex int                   // fileIndex 是当前文件序号。
	fileReady bool                  // fileReady 表示当前文件 capability 已验证。
	offset    int64                 // offset 是当前文件下一次未读取的 byte offset。
	carry     *memory.Buffer        // carry 保存尚未完成 framing 的 managed bytes。
	frame     jsonRuntimeFrameState // frame 保存当前文件结构状态。

	terminalErr error // terminalErr 固化不可恢复 parse/read/decode 错误。
	closed      bool  // closed 防止终态后继续读取。
}

// NewJSONRuntimeSource 重新走真实 bounded JSON Bind，验证 frozen fingerprint 后建立串行 range source。
func NewJSONRuntimeSource(ctx context.Context, cfg JSONRuntimeSourceConfig) (*JSONRuntimeSource, error) {
	if cfg.Binder == nil {
		return nil, fmt.Errorf("json: runtime source requires JSONBinder")
	}
	if cfg.Pattern == "" || cfg.ExpectedFingerprint == "" {
		return nil, fmt.Errorf("json: runtime source requires pattern and expected fingerprint")
	}
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	binding, err := cfg.Binder.Bind(ctx, cfg.Pattern, JSONBindOptions{})
	if err != nil {
		return nil, err
	}
	if binding.Fingerprint() != cfg.ExpectedFingerprint {
		return nil, fmt.Errorf("json: source snapshot changed: planned=%q runtime=%q", cfg.ExpectedFingerprint, binding.Fingerprint())
	}
	files := binding.ScanSet().Files()
	if len(files) == 0 {
		return nil, fmt.Errorf("json: runtime binding has no files")
	}
	rangeSize := cfg.RangeBytes
	if rangeSize <= 0 {
		rangeSize = defaultJSONRuntimeRangeBytes
	}
	return &JSONRuntimeSource{
		registry: cfg.Binder.registry,
		binding: binding,
		scope: cfg.Binder.scope,
		bm: cfg.Binder.bm,
		files: files,
		rangeSize: rangeSize,
	}, nil
}

// Next 输出至多一个 Vector batch；跨 range object 使用 managed carry 按需增长，不做 whole-file fallback。
func (s *JSONRuntimeSource) Next(ctx context.Context) (*vector.DataChunk, bool, error) {
	if s == nil {
		return nil, false, fmt.Errorf("json: nil runtime source")
	}
	if err := ctx.Err(); err != nil {
		return nil, false, err
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.closed {
		return nil, false, fmt.Errorf("json: runtime source is closed")
	}
	if s.terminalErr != nil {
		return nil, false, s.terminalErr
	}

	for {
		if s.fileIndex >= len(s.files) {
			return nil, false, nil
		}
		if err := s.ensureFileLocked(ctx); err != nil {
			return nil, false, s.failLocked(err)
		}

		file := s.files[s.fileIndex]
		final := s.offset >= file.Size
		data := []byte(nil)
		if s.carry != nil {
			data = s.carry.Bytes()
		}
		records, consumed, done, needMore, err := frameRuntimeJSON(data, final, vector.DefaultVectorSize, &s.frame)
		if err != nil {
			return nil, false, s.failLocked(fmt.Errorf("json: read %s: %w", file.URI, err))
		}
		if len(records) > 0 {
			// records 是 carry 的 slice views；DecodeRecords 完成前不能 compact/release carry。
			chunk, decodeErr := DecodeRecords(records, s.binding.schema.Fields, ProjectedPathSet{}, s.scope, s.bm)
			if decodeErr != nil {
				return nil, false, s.failLocked(fmt.Errorf("json: decode %s: %w", file.URI, decodeErr))
			}
			if compactErr := s.consumeLocked(consumed); compactErr != nil {
				chunk.Release()
				return nil, false, s.failLocked(compactErr)
			}
			if done {
				s.finishFileLocked()
			}
			return chunk, true, nil
		}
		if consumed > 0 {
			if err := s.consumeLocked(consumed); err != nil {
				return nil, false, s.failLocked(err)
			}
		}
		if done {
			s.finishFileLocked()
			continue
		}
		if !needMore {
			return nil, false, s.failLocked(fmt.Errorf("json: runtime framer made no progress"))
		}
		read, err := s.readMoreLocked(ctx)
		if err != nil {
			return nil, false, s.failLocked(err)
		}
		if !read {
			// 下一轮以 final=true 再跑 framer，由统一语义产生 truncated/empty 错误。
			continue
		}
	}
}

// ensureFileLocked 验证当前文件仍可通过 RandomRead 访问；调用方必须持有 mu。
func (s *JSONRuntimeSource) ensureFileLocked(ctx context.Context) error {
	if s.fileReady {
		return nil
	}
	file := s.files[s.fileIndex]
	if file.Size <= 0 {
		return fmt.Errorf("json: empty runtime file %s", file.URI.String())
	}
	caps, err := s.registry.EffectiveCapabilities(ctx, file.URI)
	if err != nil {
		return err
	}
	if !caps.Has(vfs.RandomRead) {
		return fmt.Errorf("json: runtime scan requires RandomRead for %s", file.URI.String())
	}
	s.fileReady = true
	return nil
}

// readMoreLocked 读取当前文件下一段 frozen object bytes 并追加到 managed carry；调用方必须持有 mu。
func (s *JSONRuntimeSource) readMoreLocked(ctx context.Context) (bool, error) {
	file := s.files[s.fileIndex]
	if s.offset >= file.Size {
		return false, nil
	}
	length := s.rangeSize
	if remain := file.Size - s.offset; length > remain {
		length = remain
	}
	driver, err := s.registry.DriverFor(file.URI)
	if err != nil {
		return false, err
	}
	result, err := vfs.ReadInputRange(ctx, driver.ReadRange, vfs.RangeRequest{
		URI: file.URI, Offset: s.offset, Length: length, Expected: &file.Identity, Tag: memory.TagReadAhead,
	}, s.scope, s.bm)
	if err != nil {
		return false, err
	}
	defer result.Buffer.Release()
	part := result.Buffer.Bytes()
	if len(part) != int(length) {
		return false, fmt.Errorf("json: runtime range returned %d bytes, want %d", len(part), length)
	}
	if s.carry == nil {
		carry, allocErr := s.bm.Allocate(s.scope, memory.TagJSONDecode, len(part))
		if allocErr != nil {
			return false, allocErr
		}
		copy(carry.Bytes(), part)
		s.carry = carry
	} else {
		old := s.carry.Len()
		if resizeErr := s.carry.Resize(old+len(part), nil); resizeErr != nil {
			return false, resizeErr
		}
		copy(s.carry.Bytes()[old:], part)
	}
	s.offset += length
	return true, nil
}

// consumeLocked 移除已完成 framing 的前缀；调用方必须持有 mu。
func (s *JSONRuntimeSource) consumeLocked(consumed int) error {
	if consumed == 0 {
		return nil
	}
	if s.carry == nil || consumed < 0 || consumed > s.carry.Len() {
		return fmt.Errorf("json: invalid consumed byte count %d", consumed)
	}
	if consumed == s.carry.Len() {
		s.carry.Release()
		s.carry = nil
		return nil
	}
	data := s.carry.Bytes()
	remaining := len(data) - consumed
	copy(data[:remaining], data[consumed:])
	return s.carry.Resize(remaining, nil)
}

// finishFileLocked 完成当前文件并释放 framing carry；调用方必须持有 mu。
func (s *JSONRuntimeSource) finishFileLocked() {
	if s.carry != nil {
		s.carry.Release()
		s.carry = nil
	}
	s.fileIndex++
	s.fileReady = false
	s.offset = 0
	s.frame = jsonRuntimeFrameState{}
}

// failLocked 固化不可恢复错误；调用方必须持有 mu。
func (s *JSONRuntimeSource) failLocked(err error) error {
	if err == nil {
		err = fmt.Errorf("json: runtime source failed")
	}
	if s.terminalErr == nil {
		s.terminalErr = err
	}
	return s.terminalErr
}

// Close 释放 carry ownership；可重复调用。Pipeline 会在所有 worker 退出后调用。
func (s *JSONRuntimeSource) Close() error {
	if s == nil {
		return nil
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.closed {
		return nil
	}
	if s.carry != nil {
		s.carry.Release()
		s.carry = nil
	}
	s.closed = true
	return nil
}

// frameRuntimeJSON 在当前 managed carry 上增量提取完整 object records。
// consumed 只覆盖可安全丢弃的结构前缀/完整 record；不完整 object bytes 必须留在 carry。
func frameRuntimeJSON(data []byte, final bool, maxRecords int, state *jsonRuntimeFrameState) (records [][]byte, consumed int, done bool, needMore bool, err error) {
	if state == nil {
		return nil, 0, false, false, fmt.Errorf("json: nil runtime framing state")
	}
	if maxRecords <= 0 {
		maxRecords = vector.DefaultVectorSize
	}
	pos := 0
	if state.mode == jsonRuntimeUnknown {
		pos = skipJSONSpace(data, pos)
		consumed = pos
		if pos >= len(data) {
			if final {
				return nil, consumed, false, false, fmt.Errorf("json: empty runtime file")
			}
			return nil, consumed, false, true, nil
		}
		switch data[pos] {
		case '[':
			state.mode = jsonRuntimeArray
			state.arrayExpectValue = true
			pos++
			consumed = pos
		case '{':
			state.mode = jsonRuntimeValues
		default:
			return nil, consumed, false, false, fmt.Errorf("json: top-level values must be objects or an object array")
		}
	}

	for {
		switch state.mode {
		case jsonRuntimeValues:
			pos = skipJSONSpace(data, pos)
			consumed = pos
			if pos >= len(data) {
				if final {
					if !state.sawRecord {
						return records, consumed, false, false, fmt.Errorf("json: no object records")
					}
					return records, consumed, true, false, nil
				}
				return records, consumed, false, true, nil
			}
			if data[pos] != '{' {
				return records, consumed, false, false, fmt.Errorf("json: top-level values must be objects")
			}
			end, complete, scanErr := scanJSONValueEnd(data, pos)
			if scanErr != nil {
				return records, consumed, false, false, scanErr
			}
			if !complete {
				if final {
					return records, consumed, false, false, fmt.Errorf("json: truncated top-level object")
				}
				return records, consumed, false, true, nil
			}
			records = append(records, data[pos:end])
			state.sawRecord = true
			pos = end
			consumed = pos
			if len(records) >= maxRecords {
				return records, consumed, false, false, nil
			}

		case jsonRuntimeArray:
			if state.arrayDone {
				pos = skipJSONSpace(data, pos)
				consumed = pos
				if pos < len(data) {
					return records, consumed, false, false, fmt.Errorf("json: trailing data after top-level array")
				}
				if final {
					return records, consumed, true, false, nil
				}
				return records, consumed, false, true, nil
			}
			if state.arrayExpectValue {
				pos = skipJSONSpace(data, pos)
				consumed = pos
				if pos >= len(data) {
					if final {
						return records, consumed, false, false, fmt.Errorf("json: unterminated top-level array")
					}
					return records, consumed, false, true, nil
				}
				if data[pos] == ']' {
					state.arrayDone = true
					pos++
					consumed = pos
					continue
				}
				if data[pos] != '{' {
					return records, consumed, false, false, fmt.Errorf("json: top-level array elements must be objects")
				}
				end, complete, scanErr := scanJSONValueEnd(data, pos)
				if scanErr != nil {
					return records, consumed, false, false, scanErr
				}
				if !complete {
					if final {
						return records, consumed, false, false, fmt.Errorf("json: truncated top-level array element")
					}
					return records, consumed, false, true, nil
				}
				records = append(records, data[pos:end])
				state.sawRecord = true
				state.arrayExpectValue = false
				pos = end
				consumed = pos
				if len(records) >= maxRecords {
					return records, consumed, false, false, nil
				}
				continue
			}

			pos = skipJSONSpace(data, pos)
			consumed = pos
			if pos >= len(data) {
				if final {
					return records, consumed, false, false, fmt.Errorf("json: unterminated top-level array")
				}
				return records, consumed, false, true, nil
			}
			switch data[pos] {
			case ',':
				state.arrayExpectValue = true
				pos++
				consumed = pos
			case ']':
				state.arrayDone = true
				pos++
				consumed = pos
			default:
				return records, consumed, false, false, fmt.Errorf("json: expected comma or ] after array element")
			}

		default:
			return records, consumed, false, false, fmt.Errorf("json: invalid runtime framing mode %d", state.mode)
		}
	}
}
