package gosql

import (
	"context"
	"errors"
	"fmt"
	"io"
	"strings"
	"sync"
	"sync/atomic"

	"github.com/steamwo/fssql/internal/core/memory"
	"github.com/steamwo/fssql/internal/core/stats"
	"github.com/steamwo/fssql/internal/core/vector"
	"github.com/steamwo/fssql/internal/execution/pipeline"
	"github.com/steamwo/fssql/internal/planner"
	"github.com/steamwo/fssql/internal/query"
	"github.com/steamwo/fssql/internal/session"
	"github.com/steamwo/fssql/internal/sql/binder"
	storagetemp "github.com/steamwo/fssql/internal/storage/temp"
)

var typedQuerySequence atomic.Uint64

func typedSourceExtensions(registrations []registeredTableFunction, buffers *memory.BufferManager, memoryBudget int64) session.QuerySourceExtensionFactory {
	if len(registrations) == 0 {
		return nil
	}
	return func(q *query.Context) ([]session.TableBindAdapter, []session.ScanOpenAdapter, error) {
		if q == nil || q.Memory == nil || q.Closed() {
			return nil, nil, fmt.Errorf("gosql: typed sources require a live query")
		}
		queryID := typedQuerySequence.Add(1)
		bindings := make([]session.TableBindAdapter, 0, len(registrations))
		scans := make([]session.ScanOpenAdapter, 0, len(registrations))
		states := make([]*typedSourceState, 0, len(registrations))
		for _, registration := range registrations {
			registration := registration
			fingerprint := stats.DatasetFingerprint(fmt.Sprintf("typed:%d:%s", queryID, strings.ToLower(registration.name)))
			state := &typedSourceState{
				q:            q,
				buffers:      buffers,
				definition:   registration,
				fingerprint:  fingerprint,
				memoryBudget: memoryBudget,
			}
			if err := q.AddCleanup(state.Close); err != nil {
				for i := len(states) - 1; i >= 0; i-- {
					_ = states[i].Close()
				}
				return nil, nil, err
			}
			states = append(states, state)
			bindings = append(bindings, session.TableBindAdapter{
				FunctionID: registration.id,
				Bind: func(ctx context.Context, _ string) (binder.ResolvedRelation, error) {
					if err := ctx.Err(); err != nil {
						return binder.ResolvedRelation{}, err
					}
					state.noteReference()
					columns := make([]binder.RelationColumn, len(registration.columns))
					copy(columns, registration.columns)
					return binder.ResolvedRelation{
						ID:          "typed:" + strings.ToLower(registration.name),
						Columns:     columns,
						Fingerprint: fingerprint,
					}, nil
				},
			})
			scans = append(scans, session.ScanOpenAdapter{
				FunctionID: registration.id,
				Open: func(ctx context.Context, spec planner.ScanSpec) (pipeline.Source, error) {
					return state.openSource(ctx, spec)
				},
			})
		}
		return bindings, scans, nil
	}
}

type typedSourceState struct {
	q            *query.Context
	buffers      *memory.BufferManager
	definition   registeredTableFunction
	fingerprint  stats.DatasetFingerprint
	memoryBudget int64

	mu             sync.Mutex
	reader         BatchReader
	readerOpened   bool
	readerClosed   bool
	singleClaimed  bool
	bindReferences int

	materializeOnce sync.Once
	materializeErr  error
	resident        []*memory.Buffer
	residentBytes   int64
	store           *storagetemp.SpillablePageStore
	pages           []storagetemp.PageID
	closed          bool
}

func (s *typedSourceState) noteReference() {
	s.mu.Lock()
	s.bindReferences++
	s.mu.Unlock()
}

func (s *typedSourceState) openSource(ctx context.Context, spec planner.ScanSpec) (pipeline.Source, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	if spec.Fingerprint != s.fingerprint {
		return nil, fmt.Errorf("gosql: typed source %s snapshot mismatch", s.definition.name)
	}
	s.mu.Lock()
	references := spec.References
	if s.bindReferences > references {
		references = s.bindReferences
	}
	if references > 1 {
		s.mu.Unlock()
		return &typedReplaySource{state: s}, nil
	}
	defer s.mu.Unlock()
	if s.closed {
		return nil, fmt.Errorf("gosql: typed source %s is closed", s.definition.name)
	}
	if s.singleClaimed {
		return nil, fmt.Errorf("gosql: typed source %s was planned as single-use but opened more than once", s.definition.name)
	}
	s.singleClaimed = true
	return &typedStreamSource{state: s}, nil
}

func (s *typedSourceState) readBatch(ctx context.Context) (*vector.DataChunk, bool, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.closed {
		return nil, false, fmt.Errorf("gosql: typed source %s is closed", s.definition.name)
	}
	if err := ctx.Err(); err != nil {
		return nil, false, err
	}
	if s.readerClosed {
		return nil, false, nil
	}
	if !s.readerOpened {
		reader, err := s.definition.open(ctx)
		if err != nil {
			return nil, false, fmt.Errorf("gosql: open typed source %s: %w", s.definition.name, err)
		}
		if reader == nil {
			return nil, false, fmt.Errorf("gosql: open typed source %s returned nil reader", s.definition.name)
		}
		s.reader = reader
		s.readerOpened = true
	}
	writer, err := newManagedBatchWriter(s.q.Memory, s.buffers, s.definition.columns)
	if err != nil {
		return nil, false, err
	}
	rows, nextErr := s.reader.Next(ctx, writer)
	if err := ctx.Err(); err != nil && nextErr == nil {
		nextErr = err
	}
	if nextErr != nil && !errors.Is(nextErr, io.EOF) {
		writer.abort()
		closeErr := s.closeReaderLocked()
		return nil, false, errors.Join(nextErr, closeErr)
	}
	if rows == 0 {
		writer.abort()
		if errors.Is(nextErr, io.EOF) {
			return nil, false, s.closeReaderLocked()
		}
		closeErr := s.closeReaderLocked()
		return nil, false, errors.Join(fmt.Errorf("gosql: typed source %s returned an empty batch without EOF", s.definition.name), closeErr)
	}
	chunk, err := writer.finish(rows)
	if err != nil {
		closeErr := s.closeReaderLocked()
		return nil, false, errors.Join(err, closeErr)
	}
	if errors.Is(nextErr, io.EOF) {
		if closeErr := s.closeReaderLocked(); closeErr != nil {
			chunk.Release()
			return nil, false, closeErr
		}
	}
	return chunk, true, nil
}

// closeReaderLocked 仅记录已成功的读取器关闭，失败时保留给查询清理重试。
func (s *typedSourceState) closeReaderLocked() error {
	if s.readerClosed {
		return nil
	}
	if !s.readerOpened || s.reader == nil {
		s.readerClosed = true
		return nil
	}
	if err := s.reader.Close(); err != nil {
		return err
	}
	s.readerClosed = true
	return nil
}

func (s *typedSourceState) closeReader() error {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.closeReaderLocked()
}

func (s *typedSourceState) ensureMaterialized(ctx context.Context) error {
	s.materializeOnce.Do(func() {
		s.materializeErr = s.materialize(ctx)
	})
	return s.materializeErr
}

func (s *typedSourceState) materialize(ctx context.Context) error {
	for {
		chunk, ok, err := s.readBatch(ctx)
		if err != nil {
			return err
		}
		if !ok {
			return nil
		}
		encoded, err := vector.SerializeChunk(chunk, s.q.Memory, s.buffers)
		if err != nil && errors.Is(err, memory.ErrLimitExceeded) && len(s.resident) != 0 {
			if spillErr := s.spillResident(ctx); spillErr != nil {
				chunk.Release()
				return errors.Join(err, spillErr)
			}
			encoded, err = vector.SerializeChunk(chunk, s.q.Memory, s.buffers)
		}
		chunk.Release()
		if err != nil {
			return err
		}
		if s.store != nil || s.memoryBudget <= 0 || s.residentBytes+int64(encoded.Len()) > s.memoryBudget {
			if err := s.spillResident(ctx); err != nil {
				encoded.Release()
				return err
			}
			if err := s.spillBuffer(ctx, encoded); err != nil {
				encoded.Release()
				return err
			}
			encoded.Release()
			continue
		}
		s.resident = append(s.resident, encoded)
		s.residentBytes += int64(encoded.Len())
	}
}

func (s *typedSourceState) ensureStore() error {
	if s.store != nil {
		return nil
	}
	if s.q.Temp == nil {
		return fmt.Errorf("gosql: typed source materialization requires temporary storage")
	}
	store, err := storagetemp.NewSpillablePageStore(s.q.Temp, s.q.Memory, s.buffers, storagetemp.PageStoreConfig{
		SegmentMaxBytes: 64 << 20,
		Compression:     storagetemp.CompressionNone,
	})
	if err != nil {
		return err
	}
	s.store = store
	return nil
}

func (s *typedSourceState) spillResident(ctx context.Context) error {
	if len(s.resident) == 0 {
		return s.ensureStore()
	}
	if err := s.ensureStore(); err != nil {
		return err
	}
	for _, encoded := range s.resident {
		if encoded == nil {
			continue
		}
		if err := s.spillBuffer(ctx, encoded); err != nil {
			return err
		}
		encoded.Release()
	}
	s.resident = nil
	s.residentBytes = 0
	return nil
}

func (s *typedSourceState) spillBuffer(ctx context.Context, encoded *memory.Buffer) error {
	if encoded == nil {
		return fmt.Errorf("gosql: nil materialized batch")
	}
	if err := s.ensureStore(); err != nil {
		return err
	}
	page, err := s.store.WritePage(ctx, encoded.Bytes())
	if err != nil {
		return err
	}
	s.pages = append(s.pages, page)
	return nil
}

func (s *typedSourceState) materializedCount() int {
	if s.store != nil {
		return len(s.pages)
	}
	return len(s.resident)
}

func (s *typedSourceState) materializedChunk(ctx context.Context, index int) (*vector.DataChunk, error) {
	if s.store != nil {
		if index < 0 || index >= len(s.pages) {
			return nil, fmt.Errorf("gosql: materialized batch index %d out of range", index)
		}
		page, err := s.store.ReadPage(ctx, s.pages[index])
		if err != nil {
			return nil, err
		}
		chunk, decodeErr := vector.DeserializeChunk(page.Bytes(), s.q.Memory, s.buffers)
		page.Release()
		return chunk, decodeErr
	}
	if index < 0 || index >= len(s.resident) {
		return nil, fmt.Errorf("gosql: materialized batch index %d out of range", index)
	}
	return vector.DeserializeChunk(s.resident[index].Bytes(), s.q.Memory, s.buffers)
}

// Close 释放尚未完成的读取器和溢写资源，已成功部分不会在重试中重复关闭。
func (s *typedSourceState) Close() error {
	if s == nil {
		return nil
	}
	s.mu.Lock()
	s.closed = true
	readerErr := s.closeReaderLocked()
	resident := s.resident
	s.resident = nil
	s.residentBytes = 0
	store := s.store
	s.mu.Unlock()

	for _, encoded := range resident {
		if encoded != nil {
			encoded.Release()
		}
	}
	var storeErr error
	if store != nil {
		storeErr = store.Close()
		if storeErr == nil {
			s.mu.Lock()
			if s.store == store {
				s.store = nil
				s.pages = nil
			}
			s.mu.Unlock()
		}
	}
	return errors.Join(readerErr, storeErr)
}

type typedStreamSource struct{ state *typedSourceState }

func (s *typedStreamSource) Next(ctx context.Context) (*vector.DataChunk, bool, error) {
	if s == nil || s.state == nil {
		return nil, false, fmt.Errorf("gosql: nil typed stream source")
	}
	return s.state.readBatch(ctx)
}

func (s *typedStreamSource) Close() error {
	if s == nil || s.state == nil {
		return nil
	}
	return s.state.closeReader()
}

type typedReplaySource struct {
	state *typedSourceState
	mu    sync.Mutex
	index int
}

func (s *typedReplaySource) Next(ctx context.Context) (*vector.DataChunk, bool, error) {
	if s == nil || s.state == nil {
		return nil, false, fmt.Errorf("gosql: nil typed replay source")
	}
	if err := s.state.ensureMaterialized(ctx); err != nil {
		return nil, false, err
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.index >= s.state.materializedCount() {
		return nil, false, nil
	}
	chunk, err := s.state.materializedChunk(ctx, s.index)
	if err != nil {
		return nil, false, err
	}
	s.index++
	return chunk, true, nil
}

func (*typedReplaySource) Close() error { return nil }

var _ pipeline.CloseableSource = (*typedStreamSource)(nil)
var _ pipeline.CloseableSource = (*typedReplaySource)(nil)
