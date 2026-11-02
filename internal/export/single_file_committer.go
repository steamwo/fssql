package export

import (
	"context"
	"errors"
	"fmt"
	pathpkg "path"

	"github.com/steamwo/fssql/internal/storage/vfs"
)

var ErrSingleFileCommit = errors.New("export: single-file commit failed")

// SingleFileCommitter advances the existing Manifest state machine after the
// atomic VFS writer has published exactly one final object. It creates no
// public sidecar and no second export state machine.
type SingleFileCommitter struct {
	Manifest *Manifest
	Store    ManifestStore
	Registry *vfs.Registry
}

// Commit reconciles the final object identity and advances
// WRITING -> COMMITTING -> COMMITTED. COMMITTING is restartable: a caller may
// invoke Commit again after loading the durable Manifest.
func (c *SingleFileCommitter) Commit(ctx context.Context) error {
	entry, finalURI, err := c.validate(ctx)
	if err != nil {
		return err
	}
	if c.Manifest.ExportState == ExportStateWriting {
		if err := transitionExportPersisted(ctx, c.Manifest, c.Store, ExportStateCommitting); err != nil {
			return fmt.Errorf("%w: persist COMMITTING: %v", ErrSingleFileCommit, err)
		}
	}
	if err := c.reconcile(ctx, entry, finalURI); err != nil {
		return err
	}
	if c.Manifest.ExportState == ExportStateCommitted {
		return nil
	}
	if err := transitionExportPersisted(ctx, c.Manifest, c.Store, ExportStateCommitted); err != nil {
		return fmt.Errorf("%w: persist COMMITTED: %v", ErrSingleFileCommit, err)
	}
	return nil
}

func (c *SingleFileCommitter) validate(ctx context.Context) (FileEntry, vfs.URI, error) {
	if c == nil || c.Manifest == nil || c.Registry == nil {
		return FileEntry{}, vfs.URI{}, fmt.Errorf("%w: manifest and VFS registry are required", ErrSingleFileCommit)
	}
	if err := ctx.Err(); err != nil {
		return FileEntry{}, vfs.URI{}, err
	}
	if err := c.Manifest.Validate(); err != nil {
		return FileEntry{}, vfs.URI{}, err
	}
	if c.Manifest.SegmentPlan != nil || len(c.Manifest.FileStates) != 1 {
		return FileEntry{}, vfs.URI{}, fmt.Errorf("%w: expected exactly one non-segmented file", ErrSingleFileCommit)
	}
	switch c.Manifest.ExportState {
	case ExportStateWriting, ExportStateCommitting, ExportStateCommitted:
	default:
		return FileEntry{}, vfs.URI{}, fmt.Errorf("%w: invalid export state %s", ErrSingleFileCommit, c.Manifest.ExportState)
	}
	entry := c.Manifest.FileStates[0]
	if entry.State != FileStateComplete || entry.PublishedIdentity == nil || !entry.PublishedIdentity.Stable() {
		return FileEntry{}, vfs.URI{}, fmt.Errorf("%w: final file has no durable stable published identity; redo required", ErrSingleFileCommit)
	}
	root, err := vfs.ParseURI(c.Manifest.Destination.URI)
	if err != nil {
		return FileEntry{}, vfs.URI{}, err
	}
	root.Path = pathpkg.Join(root.Path, entry.Path)
	root.Raw = root.String()
	caps, err := c.Registry.EffectiveCapabilities(ctx, root)
	if err != nil {
		return FileEntry{}, vfs.URI{}, err
	}
	if !caps.Has(vfs.AtomicPublishWrite) {
		return FileEntry{}, vfs.URI{}, fmt.Errorf("%w: final scheme %s no longer provides AtomicPublishWrite", ErrSingleFileCommit, root.Scheme)
	}
	return entry, root, nil
}

func (c *SingleFileCommitter) reconcile(ctx context.Context, entry FileEntry, finalURI vfs.URI) error {
	driver, err := c.Registry.DriverFor(finalURI)
	if err != nil {
		return err
	}
	info, err := driver.Stat(ctx, finalURI)
	if err != nil {
		// Transport failure is retriable; keep COMMITTING rather than claiming FAILED.
		return fmt.Errorf("%w: stat final object: %v", ErrSingleFileCommit, err)
	}
	recorded := vfs.ObjectIdentity{
		ETag:      entry.PublishedIdentity.ETag,
		VersionID: entry.PublishedIdentity.VersionID,
		Size:      entry.PublishedIdentity.Size,
	}
	if !info.Identity.Stable() || info.Size != entry.Size || !recorded.Matches(info.Identity) {
		// A deterministic identity mismatch means the visible final is not the
		// object recorded by this Manifest. Never silently accept it.
		if c.Manifest.ExportState != ExportStateCommitted {
			_ = transitionExportPersisted(context.Background(), c.Manifest, c.Store, ExportStateFailed)
		}
		return fmt.Errorf("%w: published final identity mismatch", ErrSingleFileCommit)
	}
	return nil
}
