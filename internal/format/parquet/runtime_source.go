package parquet

import (
	"context"
	"fmt"
	"io"
	"sync"

	"github.com/steamwo/fssql/internal/core/vector"
	"github.com/steamwo/fssql/internal/storage/fileset"
)

// RuntimeSourceConfig 描述 read_parquet runtime source；Pattern/Fingerprint 必须来自已绑定 ScanSpec。
type RuntimeSourceConfig struct {
	Binder              *SourceBinder // Binder 必须是 Query/Inspect 共用的真实 Parquet SourceBinder。
	Pattern             string        // Pattern 是 Stable read_parquet 的 source literal。
	ExpectedFingerprint string        // ExpectedFingerprint 是 Statement 冻结的 DatasetFingerprint。
	BatchSize           int           // BatchSize 为 0 时使用 vector.DefaultVectorSize。
	MaxFooterBytes      int64         // MaxFooterBytes 沿用 SourceBinder/MetadataCache 的 footer 上限。
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

// runtimePrepareFileFunc 在文件真正被访问时加载 footer、做严格 schema gate，并返回 row-group scanner factory。
type runtimePrepareFileFunc func(context.Context, fileset.FileSnapshot) (runtimePreparedFile, error)

// RuntimeSource 串行管理一个 frozen ScanSet 中的多文件、多 row-group Parquet reader。
// Next 受互斥锁保护，因此 Push Pipeline 多 worker 不会重复消费同一个 row group。
type RuntimeSource struct {
	mu          sync.Mutex             // mu 保护 file/row-group/scanner 游标与 Close。
	files       []fileset.FileSnapshot // files 来自 runtime Bind 后再次验证过 fingerprint 的 immutable ScanSet。
	prepareFile runtimePrepareFileFunc // prepareFile 只在真正访问文件时执行，避免偷跑 Experimental 全文件预验证。
	fileIndex   int                    // fileIndex 是当前文件序号。
	rowGroup    int                    // rowGroup 是当前文件下一 row group 序号。
	prepared    *runtimePreparedFile   // prepared 是当前文件已加载 metadata。
	scanner     runtimeRowGroupScanner // scanner 是当前 row group 的唯一 reader owner。
	terminalErr error                  // terminalErr 记录不可恢复 reader/schema 错误，防止直接调用者重试同一损坏状态。
	closed      bool                   // closed 防止终态后继续读取。
}

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
				// Projection/Filter pushdown 目前只是 optimizer candidate；在 column-index remap contract 完成前保持完整 source schema。
				return NewRowGroupScanner(driver, file.URI, file.Identity, footer.Schema, rg, nil, nil, binder.scope, binder.bm, batchSize)
			},
		}, nil
	}
	return newRuntimeSource(files, prepare)
}

// newRuntimeSource 创建已完成 bind/snapshot gate 的游标状态机。
func newRuntimeSource(files []fileset.FileSnapshot, prepare runtimePrepareFileFunc) (*RuntimeSource, error) {
	if len(files) == 0 || prepare == nil {
		return nil, fmt.Errorf("parquet: runtime source requires files and prepare callback")
	}
	copyFiles := make([]fileset.FileSnapshot, len(files))
	copy(copyFiles, files)
	return &RuntimeSource{files: copyFiles, prepareFile: prepare}, nil
}

// Next 输出一个 RowGroupScanner batch；row-group EOF 时自动关闭旧 scanner 并推进到下一 row group/file。
func (s *RuntimeSource) Next(ctx context.Context) (*vector.DataChunk, bool, error) {
	if s == nil {
		return nil, false, fmt.Errorf("parquet: nil runtime source")
	}
	if err := ctx.Err(); err != nil {
		return nil, false, err
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.closed {
		return nil, false, fmt.Errorf("parquet: runtime source is closed")
	}
	if s.terminalErr != nil {
		return nil, false, s.terminalErr
	}
	for {
		if s.scanner != nil {
			chunk, err := s.scanner.Next(ctx)
			switch {
			case err == nil && chunk != nil:
				return chunk, true, nil
			case err == nil:
				s.closeScannerLocked()
				return nil, false, s.failLocked(fmt.Errorf("parquet: row-group scanner returned nil chunk without EOF"))
			case err == io.EOF:
				s.closeScannerLocked()
				s.rowGroup++
				continue
			default:
				s.closeScannerLocked()
				return nil, false, s.failLocked(err)
			}
		}

		if s.fileIndex >= len(s.files) {
			return nil, false, nil
		}
		if s.prepared == nil {
			prepared, err := s.prepareFile(ctx, s.files[s.fileIndex])
			if err != nil {
				return nil, false, s.failLocked(err)
			}
			if prepared.newScanner == nil {
				return nil, false, s.failLocked(fmt.Errorf("parquet: prepared file %s has no scanner factory", s.files[s.fileIndex].URI.String()))
			}
			s.prepared = &prepared
		}
		if s.rowGroup >= len(s.prepared.rowGroups) {
			s.fileIndex++
			s.rowGroup = 0
			s.prepared = nil
			continue
		}
		scanner, err := s.prepared.newScanner(s.prepared.rowGroups[s.rowGroup])
		if err != nil {
			return nil, false, s.failLocked(err)
		}
		if scanner == nil {
			return nil, false, s.failLocked(fmt.Errorf("parquet: row-group scanner factory returned nil"))
		}
		s.scanner = scanner
	}
}

// Close 终止当前 reader ownership；可重复调用。
func (s *RuntimeSource) Close() error {
	if s == nil {
		return nil
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.closed {
		return nil
	}
	s.closeScannerLocked()
	s.prepared = nil
	s.closed = true
	return nil
}

// failLocked 固化不可恢复错误；调用方必须持有 mu。
func (s *RuntimeSource) failLocked(err error) error {
	if err == nil {
		err = fmt.Errorf("parquet: runtime source failed")
	}
	if s.terminalErr == nil {
		s.terminalErr = err
	}
	return s.terminalErr
}

// closeScannerLocked 释放当前 RowGroupScanner；调用方必须持有 mu。
func (s *RuntimeSource) closeScannerLocked() {
	if s.scanner != nil {
		s.scanner.Close()
		s.scanner = nil
	}
}
