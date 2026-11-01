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
	// FormatCSV 写出 RFC4180 风格 CSV。
	FormatCSV FileFormat = "csv"
	// FormatJSON 写出单个 JSON array。
	FormatJSON FileFormat = "json"
	// FormatNDJSON 写出逐行 JSON object。
	FormatNDJSON FileFormat = "ndjson"
	// FormatParquet 写出 Controlled Parquet Engine 文件。
	FormatParquet FileFormat = "parquet"
)

// FileWriterConfig 固定一次导出作业的格式、压缩和 writer 参数。
type FileWriterConfig struct {
	// Schema 是绑定后不可变的查询输出 schema。
	Schema []Column
	// Format 是目标逻辑文件格式。
	Format FileFormat
	// Compression 是独立压缩层；空值和 none 等价于 identity。
	Compression string
	// CSV 保存 CSV 文件级选项。
	CSV CSVWriteOptions
	// JSON 保存 JSON/NDJSON 文件级选项。
	JSON JSONWriteOptions
	// Parquet 保存 row-group/page 等 Parquet 内部边界参数。
	Parquet ParquetWriteOptions
}

// FileWriterFactory 负责按 Format → Compression → VFS 的冻结顺序组装单文件 writer。
type FileWriterFactory struct {
	// config 是创建时已验证的不可变配置副本。
	config FileWriterConfig
	// registry 是所有远端写入唯一允许经过的 VFS capability gate。
	registry *vfs.Registry
	// scope/buffers 管理格式、压缩和协议层的大块内存。
	scope   *memory.Scope
	buffers *memory.BufferManager
	// pool 限制同时存活 writer 数量和每 writer 基础 reservation。
	pool *WriterPool
}

// NewFileWriterFactory 校验组合是否合法；Parquet whole-file compression 在打开任何远端对象前即被拒绝。
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
	case FormatParquet:
		// Parquet footer/page offset 属于格式内部；whole-file gzip/zstd 会破坏随机访问和 footer discovery。
		if config.Compression != "identity" {
			return nil, fmt.Errorf("export: Parquet whole-file compression %q is not supported; page compression belongs inside Parquet", config.Compression)
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

// compose 在已经打开的 VFS sink 上添加压缩层和格式层；失败时统一向下 Abort。
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

// normalizeCompression 统一用户输入，保持协议、压缩和格式三个维度独立组合。
func normalizeCompression(value string) string {
	value = strings.ToLower(strings.TrimSpace(value))
	if value == "" || value == "none" {
		return "identity"
	}
	return value
}
