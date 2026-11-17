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
	job, err := vfs.NewPublishJobID()
	if err != nil {
		return nil, err
	}
	intent, err := d.PlanPublish(ctx, u, job, vfs.PublishStrict)
	if err != nil {
		return nil, err
	}
	h, err := d.BeginPublish(ctx, intent, func(context.Context, []byte) error { return nil }, scope, bm)
	if err != nil {
		if h != nil {
			_ = h.Release()
		}
		return nil, err
	}
	// Legacy AtomicPublishWrite supports explicit overwrite/CAS as before;
	// coordinated COPY always retains its independently validated no-replace mode.
	h.(*s3PublishHandle).writer.opts = opts
	writer := h.(*s3PublishHandle).writer
	writer.legacy = vfs.NewPublishWriteAdapter(job, h, func(ctx context.Context) (vfs.ObjectIdentity, error) {
		info, err := d.Stat(ctx, u)
		return info.Identity, err
	})
	return writer, nil
}
