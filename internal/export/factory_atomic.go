package export

import (
	"context"
	"errors"

	"github.com/steamwo/fssql/internal/storage/vfs"
)

// OpenAtomicPublish is the SingleFile SQL COPY writer path. It preserves the
// existing WriterPool/format/compression composition and changes only the VFS
// opening contract from ordinary streaming write to AtomicPublishWrite.
func (f *FileWriterFactory) OpenAtomicPublish(ctx context.Context, uri vfs.URI, opts vfs.WriteOptions) (*PooledWriter, error) {
	if f == nil || f.pool == nil {
		return nil, errors.New("export: nil file writer factory")
	}
	return f.pool.Acquire(ctx, func(ctx context.Context) (FormatWriter, error) {
		base, err := NewAtomicVFSByteSink(ctx, f.registry, uri, opts, f.scope, f.buffers)
		if err != nil {
			return nil, err
		}
		return f.compose(ctx, base)
	})
}
