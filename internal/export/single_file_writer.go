package export

import (
	"context"
	"errors"

	"github.com/steamwo/fssql/internal/storage/vfs"
)

// NewAtomicSingleFileDatasetWriter reuses DatasetWriter routing/state updates
// while forcing its only file through AtomicPublishWrite. MultiFile and
// Partitioned callers continue to use NewDatasetWriter and existing commit semantics.
func NewAtomicSingleFileDatasetWriter(manifest *Manifest, router *FileRouter, factory *FileWriterFactory) (*DatasetWriter, error) {
	if router == nil || router.config.Mode != LayoutSingleFile {
		return nil, errors.New("export: atomic single-file writer requires SingleFile routing")
	}
	if factory == nil {
		return nil, errors.New("export: atomic single-file writer requires file writer factory")
	}
	return newDatasetWriter(manifest, router, func(ctx context.Context, uri vfs.URI, opts vfs.WriteOptions) (FormatWriter, error) {
		opts.IfNoneMatch = true
		return factory.OpenAtomicPublish(ctx, uri, opts)
	})
}
