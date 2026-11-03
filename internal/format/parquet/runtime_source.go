package parquet

import (
	"context"
	"fmt"
	"io"
	"sort"
	"sync"

	"github.com/steamwo/fssql/internal/core/memory"
	"github.com/steamwo/fssql/internal/core/types"
	"github.com/steamwo/fssql/internal/core/vector"
	formatscan "github.com/steamwo/fssql/internal/format/scan"
	"github.com/steamwo/fssql/internal/storage/fileset"
)

// RuntimeSourceConfig 描述 read_parquet runtime source；Pattern/Fingerprint 必须来自已绑定 ScanSpec。
type RuntimeSourceConfig struct {
	Binder              *SourceBinder       // Binder 必须是 Query/Inspect 共用的真实 Parquet SourceBinder。
	Pattern             string              // Pattern 是 Stable read_parquet 的 source literal。
	ExpectedFingerprint string              // ExpectedFingerprint 是 Statement 冻结的 DatasetFingerprint。
	BatchSize           int                 // BatchSize 为 0 时使用 vector.DefaultVectorSize。
	MaxFooterBytes      int64               // MaxFooterBytes 沿用 SourceBinder/MetadataCache 的 footer 上限。
	Pushdown            formatscan.Pushdown // Pushdown 是 concrete adapter 已翻译的安全候选；residual physical operators 仍保留。
}

// runtimeRowGroupScanner 是 RuntimeSource 对 RowGroupScanner 的最窄内部依赖，便于验证游标/Close 生命周期。
type runtimeRowGroupScanner interface {
	Next(context.Context) (*vector.DataChunk, error)
	Close()
}

// runtimePreparedFile 保存当前文件访问时已验证的 row-group scanner 构造器。
type runtimePreparedFile struct {
	rowGroups  []RowGroup
	newScanner func(RowGroup) (runtimeRowGroupScanner, error)
}

// runtimeScanTask owns exactly one row-group scanner while it is either ready
// or claimed by one Next call. A scanner is never used by two workers at once.
type runtimeScanTask struct {
	scanner runtimeRowGroupScanner
}

// RuntimeSource coordinates a frozen ScanSet while allowing independent
// row-group scanners to perform I/O/decode concurrently. The mutex protects
// only cursor/task ownership; scanner.Next always runs outside it.
type RuntimeSource struct {
	mu          sync.Mutex             // mu protects file/row-group cursors, ready tasks, terminal state and Close.
	files       []fileset.FileSnapshot // files come from the runtime-verified immutable ScanSet.
	prepareFile runtimePrepareFileFunc // prepareFile remains lazy to avoid Experimental full-file validation.
	fileIndex   int                    // fileIndex is the next file whose row groups may be claimed.
	rowGroup    int                    // rowGroup is the next unclaimed row group in prepared.
	prepared    *runtimePreparedFile   // prepared is metadata for fileIndex.
	ready       []*runtimeScanTask     // ready contains scanners between batches; ownership is exclusively under mu.
	inflight    int                    // inflight counts scanners currently executing Next outside mu.
	active      sync.WaitGroup         // active lets Close wait until all claimed scanners leave Next.
	notify      chan struct{}          // notify wakes claimers when inflight tasks requeue/finish or terminal state changes.
	terminalErr error                  // terminalErr is sticky after the first unrecoverable reader/schema error.
	closed      bool                   // closed prevents new task claims.
	limitMu     sync.Mutex             // accepted scan LIMIT remains serial so early-stop order cannot race across row groups.

	outputTypes       []types.LogicalType // outputTypes preserves the Binder relation's full column layout.
	projectionIndexes []int               // projectionIndexes maps narrow scanner output back to relation indexes; nil means full width.
	scope             *memory.Scope       // scope/buffers create managed NULL placeholders for unread columns.
	buffers           *memory.BufferManager
	limit             uint64 // limit is a safe scan LIMIT candidate; 0 means not accepted.
	emitted           uint64 // emitted counts rows handed to residual pipeline and is protected by mu.
}

// runtimePrepareFileFunc 在文件真正被访问时加载 footer、做严格 schema gate，并返回 row-group scanner factory。
type runtimePrepareFileFunc func(context.Context, fileset.FileSnapshot) (runtimePreparedFile, error)

// NewRuntimeSource 通过真实 bounded SourceBinder 建立 read_parquet runtime source。
// 它先重新冻结 FileSet 并核对 fingerprint，再按访问顺序逐文件做 schema correctness gate。
func NewRuntimeSource(ctx context.Context, cfg RuntimeSourceConfig) (*RuntimeSource, error) {
	if cfg.Binder == nil {
		return nil, fmt.Errorf("parquet: runtime source requires SourceBinder")
	}
	if cfg.Pattern == "" || cfg.ExpectedFingerprint == "" {
		return nil, fmt.Errorf("parquet: runtime source requires pattern and expected fingerprint")
	}
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	batchSize := cfg.BatchSize
	if batchSize == 0 {
		batchSize = vector.DefaultVectorSize
	}
	if batchSize < 1 || batchSize > vector.DefaultVectorSize {
		return nil, fmt.Errorf("parquet: invalid runtime batch size %d", batchSize)
	}
	if cfg.Pushdown.Limit > 0 && cfg.Pushdown.Limit < uint64(batchSize) {
		batchSize = int(cfg.Pushdown.Limit)
	}

	// Runtime 仍走同一个 bounded Bind；禁止把 Experimental full validation 静默带进普通 query。
	binding, err := cfg.Binder.Bind(ctx, cfg.Pattern, SourceBindOptions{
		ValidationMode: SchemaValidationBounded,
		MaxFooterBytes: cfg.MaxFooterBytes,
	})
	if err != nil {
		return nil, err
	}
	if binding.Fingerprint() != cfg.ExpectedFingerprint {
		return nil, fmt.Errorf("parquet: source snapshot changed: planned=%q runtime=%q", cfg.ExpectedFingerprint, binding.Fingerprint())
	}
	files := binding.ScanSet().Files()
	if len(files) == 0 {
		return nil, fmt.Errorf("parquet: runtime binding has no files")
	}
	metadata := binding.Metadata()
	if len(metadata) == 0 || metadata[0].Footer.Schema == nil {
		return nil, fmt.Errorf("parquet: runtime binding has no base schema metadata")
	}
	baseTree := metadata[0].Footer.Schema
	resolved := binding.Schema()
	projectionIndexes, projectionNames, filters, err := resolveRuntimePushdown(resolved, cfg.Pushdown)
	if err != nil {
		return nil, err
	}
	outputTypes := make([]types.LogicalType, len(resolved.Fields))
	for i, field := range resolved.Fields {
		outputTypes[i] = field.Type
	}
	binder := cfg.Binder
	prepare := func(ctx context.Context, file fileset.FileSnapshot) (runtimePreparedFile, error) {
		driver, err := binder.registry.DriverFor(file.URI)
		if err != nil {
			return runtimePreparedFile{}, err
		}
		footer, err := binder.cache.GetOrDiscover(ctx, driver, FooterSource{
			URI: file.URI, Size: file.Size, Identity: file.Identity,
		}, binder.scope, binder.bm, cfg.MaxFooterBytes)
		if err != nil {
			return runtimePreparedFile{}, fmt.Errorf("parquet: runtime footer %s: %w", file.URI.String(), err)
		}
		if footer.Schema == nil || footer.Schema.Root == nil {
			return runtimePreparedFile{}, fmt.Errorf("parquet: runtime file %s has no schema", file.URI.String())
		}
		// 与 FullValidation 完全复用同一个 ResolveSchemas strict 规则，但只在文件实际被访问时执行。
		if _, err := ResolveSchemas([]*SchemaTree{baseTree, footer.Schema}, false); err != nil {
			return runtimePreparedFile{}, fmt.Errorf("parquet: runtime schema drift for %s: %w", file.URI.String(), err)
		}
		rowGroups := append([]RowGroup(nil), footer.Metadata.RowGroups...)
		return runtimePreparedFile{
			rowGroups: rowGroups,
			newScanner: func(rg RowGroup) (runtimeRowGroupScanner, error) {
				return NewRowGroupScanner(driver, file.URI, file.Identity, footer.Schema, rg, projectionNames, filters, binder.scope, binder.bm, batchSize)
			},
		}, nil
	}
	source, err := newRuntimeSource(files, prepare)
	if err != nil {
		return nil, err
	}
	source.outputTypes = outputTypes
	source.projectionIndexes = projectionIndexes
	source.scope = binder.scope
	source.buffers = binder.bm
	source.limit = cfg.Pushdown.Limit
	return source, nil
}

// resolveRuntimePushdown maps bound relation indexes through the concrete
// Parquet schema. Projection is accepted only for top-level scalar columns,
// because LIST/STRUCT may expand to multiple physical leaves. Filter columns
// are included in the physical projection so residual filters retain their
// original ColumnIndex inputs after the source re-expands its output shape.
func resolveRuntimePushdown(schema ResolvedSchema, pushdown formatscan.Pushdown) ([]int, []string, []formatscan.Filter, error) {
	filters := make([]formatscan.Filter, 0, len(pushdown.Filters))
	filterIndexes := make([]int, 0, len(pushdown.Filters))
	for _, filter := range pushdown.Filters {
		if filter.ColumnIndex < 0 || filter.ColumnIndex >= len(schema.Fields) {
			return nil, nil, nil, fmt.Errorf("parquet: filter column index %d out of range", filter.ColumnIndex)
		}
		field := schema.Fields[filter.ColumnIndex]
		filters = append(filters, formatscan.Filter{Column: field.Name, Op: filter.Op, Value: filter.Value})
		filterIndexes = append(filterIndexes, filter.ColumnIndex)
	}
	if len(pushdown.Projection) == 0 {
		return nil, nil, filters, nil
	}
	selected := make(map[int]struct{}, len(pushdown.Projection)+len(filterIndexes))
	for _, index := range pushdown.Projection {
		if index < 0 || index >= len(schema.Fields) {
			return nil, nil, nil, fmt.Errorf("parquet: projection column index %d out of range", index)
		}
		selected[index] = struct{}{}
	}
	for _, index := range filterIndexes {
		selected[index] = struct{}{}
	}
	indexes := make([]int, 0, len(selected))
	for index := range selected {
		field := schema.Fields[index]
		if field.Type.ID == types.List || field.Type.ID == types.Struct {
			// Keep the full schema path for nested columns until a leaf-to-logical
			// reconstruction contract is available. This is fail-safe, not a
			// semantic downgrade: residual operators still see the full source.
			return nil, nil, filters, nil
		}
		indexes = append(indexes, index)
	}
	sort.Ints(indexes)
	names := make([]string, len(indexes))
	for i, index := range indexes {
		names[i] = schema.Fields[index].Name
	}
	return indexes, names, filters, nil
}

// newRuntimeSource 创建已完成 bind/snapshot gate 的游标状态机。
func newRuntimeSource(files []fileset.FileSnapshot, prepare runtimePrepareFileFunc) (*RuntimeSource, error) {
	if len(files) == 0 || prepare == nil {
		return nil, fmt.Errorf("parquet: runtime source requires files and prepare callback")
	}
	copyFiles := make([]fileset.FileSnapshot, len(files))
	copy(copyFiles, files)
	return &RuntimeSource{files: copyFiles, prepareFile: prepare, notify: make(chan struct{})}, nil
}

// Next claims one row-group scanner under the ownership mutex, performs its
// I/O/decode outside the mutex, then requeues the scanner if more batches remain.
// A safe LIMIT candidate uses limitMu so early-stop remains deterministic and
// never allows concurrent row groups to race past the candidate boundary.
func (s *RuntimeSource) Next(ctx context.Context) (*vector.DataChunk, bool, error) {
	if s == nil {
		return nil, false, fmt.Errorf("parquet: nil runtime source")
	}
	if err := ctx.Err(); err != nil {
		return nil, false, err
	}
	if s.limit > 0 {
		s.limitMu.Lock()
		defer s.limitMu.Unlock()
	}
	for {
		task, ok, err := s.claimTask(ctx)
		if err != nil {
			return nil, false, err
		}
		if !ok {
			return nil, false, nil
		}
		chunk, more, err := s.runClaimedTask(ctx, task)
		if err != nil {
			return nil, false, err
		}
		if more {
			return chunk, true, nil
		}
	}
}

func (s *RuntimeSource) claimTask(ctx context.Context) (*runtimeScanTask, bool, error) {
	for {
		s.mu.Lock()
		if s.closed {
			s.mu.Unlock()
			return nil, false, fmt.Errorf("parquet: runtime source is closed")
		}
		if s.terminalErr != nil {
			err := s.terminalErr
			s.mu.Unlock()
			return nil, false, err
		}
		if s.limit > 0 && s.emitted >= s.limit {
			s.mu.Unlock()
			return nil, false, nil
		}
		if n := len(s.ready); n != 0 {
			task := s.ready[n-1]
			s.ready = s.ready[:n-1]
			s.inflight++
			s.active.Add(1)
			s.mu.Unlock()
			return task, true, nil
		}

		for s.fileIndex < len(s.files) {
			if err := ctx.Err(); err != nil {
				s.mu.Unlock()
				return nil, false, err
			}
			if s.prepared == nil {
				prepared, err := s.prepareFile(ctx, s.files[s.fileIndex])
				if err != nil {
					err = s.failAndCloseReadyLocked(err)
					s.signalLocked()
					s.mu.Unlock()
					return nil, false, err
				}
				if prepared.newScanner == nil {
					err := s.failAndCloseReadyLocked(fmt.Errorf("parquet: prepared file %s has no scanner factory", s.files[s.fileIndex].URI.String()))
					s.signalLocked()
					s.mu.Unlock()
					return nil, false, err
				}
				s.prepared = &prepared
			}
			if s.rowGroup >= len(s.prepared.rowGroups) {
				s.fileIndex++
				s.rowGroup = 0
				s.prepared = nil
				continue
			}
			rg := s.prepared.rowGroups[s.rowGroup]
			s.rowGroup++ // claim ownership before scanner I/O can run concurrently.
			scanner, err := s.prepared.newScanner(rg)
			if err != nil {
				err = s.failAndCloseReadyLocked(err)
				s.signalLocked()
				s.mu.Unlock()
				return nil, false, err
			}
			if scanner == nil {
				err := s.failAndCloseReadyLocked(fmt.Errorf("parquet: row-group scanner factory returned nil"))
				s.signalLocked()
				s.mu.Unlock()
				return nil, false, err
			}
			task := &runtimeScanTask{scanner: scanner}
			s.inflight++
			s.active.Add(1)
			s.mu.Unlock()
			return task, true, nil
		}

		if s.inflight == 0 {
			s.mu.Unlock()
			return nil, false, nil
		}
		wait := s.notify
		s.mu.Unlock()
		select {
		case <-wait:
			continue
		case <-ctx.Done():
			return nil, false, ctx.Err()
		}
	}
}

func (s *RuntimeSource) runClaimedTask(ctx context.Context, task *runtimeScanTask) (*vector.DataChunk, bool, error) {
	defer s.active.Done()
	if task == nil || task.scanner == nil {
		s.mu.Lock()
		if s.inflight > 0 {
			s.inflight--
		}
		s.signalLocked()
		err := s.failAndCloseReadyLocked(fmt.Errorf("parquet: claimed nil row-group scanner"))
		s.mu.Unlock()
		return nil, false, err
	}

	chunk, scanErr := task.scanner.Next(ctx)
	if scanErr == nil && chunk == nil {
		scanErr = fmt.Errorf("parquet: row-group scanner returned nil chunk without EOF")
	}
	if scanErr == nil && len(s.projectionIndexes) != 0 {
		var err error
		chunk, err = s.expandProjectedChunk(chunk)
		if err != nil {
			scanErr = err
		}
	}

	s.mu.Lock()
	defer s.mu.Unlock()
	if s.inflight > 0 {
		s.inflight--
	}
	s.signalLocked()
	if s.closed {
		if chunk != nil {
			chunk.Release()
		}
		task.scanner.Close()
		return nil, false, fmt.Errorf("parquet: runtime source is closed")
	}
	if s.terminalErr != nil {
		if chunk != nil {
			chunk.Release()
		}
		task.scanner.Close()
		return nil, false, s.terminalErr
	}
	switch {
	case scanErr == nil && chunk != nil:
		s.ready = append(s.ready, task)
		s.emitted += uint64(chunk.Cardinality)
		return chunk, true, nil
	case scanErr == io.EOF:
		task.scanner.Close()
		return nil, false, nil
	default:
		if chunk != nil {
			chunk.Release()
		}
		task.scanner.Close()
		return nil, false, s.failAndCloseReadyLocked(scanErr)
	}
}

func (s *RuntimeSource) expandProjectedChunk(chunk *vector.DataChunk) (*vector.DataChunk, error) {
	if chunk == nil || len(s.projectionIndexes) == 0 {
		return chunk, nil
	}
	if len(chunk.Columns) != len(s.projectionIndexes) {
		chunk.Release()
		return nil, fmt.Errorf("parquet: projected scanner returned %d columns, want %d", len(chunk.Columns), len(s.projectionIndexes))
	}
	if len(s.outputTypes) == 0 || s.scope == nil || s.buffers == nil {
		chunk.Release()
		return nil, fmt.Errorf("parquet: projected runtime source is missing output schema ownership")
	}
	columns := make([]*vector.Vector, len(s.outputTypes))
	for i, index := range s.projectionIndexes {
		if index < 0 || index >= len(columns) || columns[index] != nil {
			chunk.Release()
			return nil, fmt.Errorf("parquet: invalid projected output index %d", index)
		}
		columns[index] = chunk.Columns[i]
	}
	// Transfer vector ownership out of the narrow chunk before allocating
	// placeholders; on failure release every transferred/allocated column once.
	chunk.Columns = nil
	cardinality := chunk.Cardinality
	chunk.Cardinality = 0
	for i := range columns {
		if columns[i] != nil {
			continue
		}
		placeholder, err := vector.NewConstant(s.scope, s.buffers, s.outputTypes[i], cardinality)
		if err != nil {
			releaseRuntimeColumns(columns)
			return nil, err
		}
		placeholder.SetNull(0)
		columns[i] = placeholder
	}
	out, err := vector.NewDataChunk(columns...)
	if err != nil {
		releaseRuntimeColumns(columns)
		return nil, err
	}
	return out, nil
}

func releaseRuntimeColumns(columns []*vector.Vector) {
	for _, column := range columns {
		if column != nil {
			column.Release()
		}
	}
}

// Close stops new claims, closes every idle scanner, and waits for scanners
// already inside Next to leave their I/O/decode call before returning.
func (s *RuntimeSource) Close() error {
	if s == nil {
		return nil
	}
	s.mu.Lock()
	if !s.closed {
		s.closed = true
		s.closeReadyLocked()
		s.prepared = nil
		s.signalLocked()
	}
	s.mu.Unlock()
	s.active.Wait()
	return nil
}

func (s *RuntimeSource) failLocked(err error) error {
	if err == nil {
		err = fmt.Errorf("parquet: runtime source failed")
	}
	if s.terminalErr == nil {
		s.terminalErr = err
	}
	return s.terminalErr
}

func (s *RuntimeSource) failAndCloseReadyLocked(err error) error {
	err = s.failLocked(err)
	s.closeReadyLocked()
	return err
}

func (s *RuntimeSource) closeReadyLocked() {
	for _, task := range s.ready {
		if task != nil && task.scanner != nil {
			task.scanner.Close()
		}
	}
	s.ready = nil
}

func (s *RuntimeSource) signalLocked() {
	if s.notify == nil {
		s.notify = make(chan struct{})
		return
	}
	close(s.notify)
	s.notify = make(chan struct{})
}
