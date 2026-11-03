package export

import (
	"context"
	"errors"
	"fmt"
	"strings"

	"github.com/steamwo/fssql/internal/core/memory"
	"github.com/steamwo/fssql/internal/storage/vfs"
)

// FileFormat 表示 Phase 4 已授权的目标文件格式。
type FileFormat string

const (
	FormatCSV     FileFormat = "csv"
	FormatJSON    FileFormat = "json"
	FormatNDJSON  FileFormat = "ndjson"
	FormatParquet FileFormat = "parquet"
)

// FileWriterConfig 固定一次导出作业的格式、压缩和 writer 参数。
type FileWriterConfig struct {
	Schema      []Column
	Format      FileFormat
	Compression string
	CSV         CSVWriteOptions
	JSON        JSONWriteOptions
	Parquet     ParquetWriteOptions
}

// FileWriterFactory 负责按 Format → Compression → VFS 的冻结顺序组装单文件 writer。
type FileWriterFactory struct {
	config   FileWriterConfig
	registry *vfs.Registry
	scope    *memory.Scope
	buffers  *memory.BufferManager
	pool     *WriterPool
}

// NewFileWriterFactory validates the complete format configuration before any output object is opened.
func NewFileWriterFactory(config FileWriterConfig, registry *vfs.Registry, scope *memory.Scope, buffers *memory.BufferManager, pool *WriterPool) (*FileWriterFactory, error) {
	if registry == nil || scope == nil || buffers == nil || pool == nil {
		return nil, errors.New("export: writer factory requires registry, memory and writer pool")
	}
	if len(config.Schema) == 0 {
		return nil, errors.New("export: writer factory schema is empty")
	}
	config.Format = FileFormat(strings.ToLower(strings.TrimSpace(string(config.Format))))
	config.Compression = normalizeCompression(config.Compression)
	switch config.Format {
	case FormatCSV, FormatJSON, FormatNDJSON:
		switch config.Compression {
		case "identity", "gzip", "zstd":
		default:
			return nil, fmt.Errorf("export: unsupported compression %q for %s", config.Compression, config.Format)
		}
		if config.Parquet.Encryption != nil {
			return nil, errors.New("export: Parquet encryption is only valid for Parquet output")
		}
	case FormatParquet:
		// Compression is a Parquet page codec here, never a whole-file compression layer.
		switch config.Compression {
		case "identity", "snappy":
		default:
			return nil, fmt.Errorf("export: unsupported Parquet page compression %q", config.Compression)
		}
		config.Parquet.Compression = config.Compression
		if err := ValidateParquetEncryption(config.Parquet.Encryption); err != nil {
			return nil, err
		}
	default:
		return nil, fmt.Errorf("export: unsupported file format %q", config.Format)
	}
	config.Schema = append([]Column(nil), config.Schema...)
	return &FileWriterFactory{config: config, registry: registry, scope: scope, buffers: buffers, pool: pool}, nil
}

// Open 通过 WriterPool 获取 bounded slot 后组装一个目标文件 writer；任一构造失败都会 Abort 已打开的下层对象。
func (f *FileWriterFactory) Open(ctx context.Context, uri vfs.URI, opts vfs.WriteOptions) (*PooledWriter, error) {
	if f == nil || f.pool == nil {
		return nil, errors.New("export: nil file writer factory")
	}
	return f.pool.Acquire(ctx, func(ctx context.Context) (FormatWriter, error) {
		base, err := NewVFSByteSink(ctx, f.registry, uri, opts, f.scope, f.buffers)
		if err != nil {
			return nil, err
		}
		return f.compose(ctx, base)
	})
}

func (f *FileWriterFactory) compose(ctx context.Context, base ByteSink) (FormatWriter, error) {
	if f.config.Format == FormatParquet {
		writer, err := NewParquetWriter(f.config.Schema, f.config.Parquet, f.scope, f.buffers, base)
		if err != nil {
			return nil, errors.Join(err, base.Abort(context.Background()))
		}
		return writer, nil
	}

	var sink ByteSink = base
	if f.config.Compression != "identity" {
		compressed, err := NewCompressionSink(f.config.Compression, f.scope, f.buffers, base)
		if err != nil {
			return nil, errors.Join(err, base.Abort(context.Background()))
		}
		sink = compressed
	}

	var (
		writer FormatWriter
		err    error
	)
	switch f.config.Format {
	case FormatCSV:
		writer, err = NewCSVWriter(f.config.Schema, f.config.CSV, f.scope, f.buffers, sink)
	case FormatJSON:
		opts := f.config.JSON
		opts.Mode = JSONModeArray
		writer, err = NewJSONWriter(f.config.Schema, opts, f.scope, f.buffers, sink)
	case FormatNDJSON:
		opts := f.config.JSON
		opts.Mode = JSONModeNDJSON
		writer, err = NewJSONWriter(f.config.Schema, opts, f.scope, f.buffers, sink)
	default:
		err = fmt.Errorf("export: unsupported file format %q", f.config.Format)
	}
	if err != nil {
		return nil, errors.Join(err, sink.Abort(context.Background()))
	}
	return writer, nil
}

func normalizeCompression(value string) string {
	value = strings.ToLower(strings.TrimSpace(value))
	if value == "" || value == "none" {
		return "identity"
	}
	return value
}
