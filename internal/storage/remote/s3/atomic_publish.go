package s3

import (
	"context"

	"github.com/steamwo/fssql/internal/core/memory"
	"github.com/steamwo/fssql/internal/storage/vfs"
)

var _ vfs.AtomicPublishDriver = (*Driver)(nil)

// OpenAtomicPublishWrite exposes the S3 writer through the stronger single-object
// publish contract. Multipart parts remain invisible as the final object until
// CompleteMultipartUpload succeeds; the empty-object path is a single PutObject.
// Both paths therefore leave any previous complete object unchanged before the
// successful publish point. This method is intentionally explicit rather than a
// Registry fallback from AtomicPublishWrite to ordinary OpenWrite.
func (d *Driver) OpenAtomicPublishWrite(ctx context.Context, u vfs.URI, opts vfs.WriteOptions, scope *memory.Scope, bm *memory.BufferManager) (vfs.WriteHandle, error) {
	return d.OpenWrite(ctx, u, opts, scope, bm)
}
