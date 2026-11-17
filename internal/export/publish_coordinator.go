package export

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"hash"
	"io"
	"os"
	"time"

	"github.com/steamwo/fssql/internal/core/memory"
	"github.com/steamwo/fssql/internal/storage/vfs"
)

type ExportRecoveryResult struct {
	JobID       string
	Destination string
	PublishMode vfs.PublishMode
	Bytes       int64
	Checksum    string
}

// PublishCoordinator owns the lock from the first load through final cleanup.
// Store must resolve only validated JobIDs within ManifestDir.
type PublishCoordinator struct {
	ManifestDir string
	Store       func(string) (ManifestStore, error)
	Registry    *vfs.Registry
	Scope       *memory.Scope
	Buffers     *memory.BufferManager
}

func (c *PublishCoordinator) driver(ctx context.Context, m *Manifest) (vfs.StagedPublishDriver, error) {
	if err := ValidateManifestJob(m, m.JobID); err != nil {
		return nil, err
	}
	u, err := vfs.ParseURI(m.Publish.Intent.Destination)
	if err != nil {
		return nil, err
	}
	d, err := c.Registry.DriverFor(u)
	if err != nil {
		return nil, err
	}
	p, ok := d.(vfs.StagedPublishDriver)
	if !ok {
		return nil, ErrExportNotRecoverable
	}
	return p, nil
}

func (c *PublishCoordinator) open(ctx context.Context, job string) (ManifestStore, func() error, error) {
	if c == nil || c.Store == nil || c.Registry == nil || c.Scope == nil || c.Buffers == nil || c.ManifestDir == "" {
		return nil, nil, ErrExportNotRecoverable
	}
	unlock, err := LockJob(ctx, c.ManifestDir, job)
	if err != nil {
		return nil, nil, err
	}
	store, err := c.Store(job)
	if err != nil || store == nil {
		_ = unlock()
		if err == nil {
			err = ErrExportNotRecoverable
		}
		return nil, nil, err
	}
	return store, unlock, nil
}

// save only changes the in-process snapshot after durable success. A save error
// may occur after replacement; reload under the lock before choosing cleanup.
func savePublish(ctx context.Context, store ManifestStore, m, next *Manifest) error {
	next.Revision = m.Revision + 1
	if err := next.Validate(); err != nil {
		return err
	}
	if err := validatePublishUpdate(m, next); err != nil {
		return err
	}
	if err := store.Save(ctx, next); err != nil {
		check, cancel := context.WithTimeout(context.WithoutCancel(ctx), 5*time.Second)
		defer cancel()
		if current, loadErr := store.Load(check); loadErr == nil && ValidateManifestJob(current, m.JobID) == nil && current.Revision >= m.Revision {
			*m = *current
		}
		return err
	}
	*m = *next
	return nil
}

func jobError(m *Manifest, err error) error  { return &ExportJobError{JobID: m.JobID, Err: err} }
func uncertain(m *Manifest, err error) error { return &CommitUncertainError{JobID: m.JobID, Err: err} }

// Run provides an explicit Seal boundary after the producer finishes all format
// and compression tails. The producer can use FileWriterFactory.OpenStaged.
func (c *PublishCoordinator) Run(ctx context.Context, manifest *Manifest, produce func(context.Context, ByteSink) error) (ExportRecoveryResult, error) {
	if manifest == nil || manifest.ManifestVersion != 2 || manifest.ExportState != ExportStatePlanned || produce == nil {
		return ExportRecoveryResult{}, ErrInvalidManifest
	}
	if err := manifest.Validate(); err != nil {
		return ExportRecoveryResult{}, err
	}
	m := cloneManifest(manifest)
	store, unlock, err := c.open(ctx, m.JobID)
	if err != nil {
		return ExportRecoveryResult{}, err
	}
	defer unlock()
	upper, err := manifestJSONUpperBound(m)
	if err != nil {
		return ExportRecoveryResult{}, jobError(m, err)
	}
	metadata, err := c.Scope.Reserve(memory.TagMetadata, upper*2)
	if err != nil {
		return ExportRecoveryResult{}, jobError(m, err)
	}
	defer metadata.Release()
	if _, err := store.Load(ctx); !errors.Is(err, os.ErrNotExist) {
		if err == nil {
			err = ErrStaleRevision
		}
		return ExportRecoveryResult{}, jobError(m, err)
	}
	driver, err := c.driver(ctx, m)
	if err != nil {
		return ExportRecoveryResult{}, jobError(m, err)
	}
	if err := store.Save(ctx, m); err != nil {
		return ExportRecoveryResult{}, jobError(m, err)
	}
	next := cloneManifest(m)
	next.ExportState = ExportStateWriting
	next.FileStates[0].State = FileStateWriting
	next.Publish.CleanupPending = true
	if err := savePublish(ctx, store, m, next); err != nil {
		return ExportRecoveryResult{}, jobError(m, err)
	}
	h, err := driver.BeginPublish(ctx, m.Publish.Intent, func(ctx context.Context, ownership []byte) error {
		if len(ownership) == 0 || len(ownership) > MaxPublishTokenBytes {
			return ErrInvalidManifest
		}
		if err := metadata.Resize(upper*2 + int64(len(ownership))*12); err != nil {
			return err
		}
		next := cloneManifest(m)
		next.Publish.Ownership = append([]byte(nil), ownership...)
		return savePublish(ctx, store, m, next)
	}, c.Scope, c.Buffers)
	if err != nil {
		// Resume may find resources by a previously persisted exclusive scope;
		// it must refuse cleanup if ownership cannot be proved.
		if h != nil {
			defer h.Release()
			c.fail(ctx, store, m, h, "not_recoverable")
		}
		return ExportRecoveryResult{}, jobError(m, err)
	}
	defer h.Release()
	if len(m.Publish.Ownership) == 0 {
		c.fail(ctx, store, m, h, "not_recoverable")
		return ExportRecoveryResult{}, jobError(m, ErrInvalidManifest)
	}
	sink := &stagedByteSink{handle: h, digest: sha256.New()}
	if err := produce(ctx, sink); err != nil {
		c.fail(ctx, store, m, h, "not_recoverable")
		return ExportRecoveryResult{}, jobError(m, err)
	}
	if _, err := sink.Close(ctx); err != nil {
		c.fail(ctx, store, m, h, "not_recoverable")
		return ExportRecoveryResult{}, jobError(m, err)
	}
	if len(sink.sealed.RecoveryToken) > MaxPublishTokenBytes {
		return ExportRecoveryResult{}, jobError(m, ErrInvalidManifest)
	}
	if err := metadata.Resize(upper*2 + int64(len(m.Publish.Ownership)+len(sink.sealed.RecoveryToken))*12); err != nil {
		return ExportRecoveryResult{}, jobError(m, err)
	}
	next = cloneManifest(m)
	next.ExportState = ExportStatePrepared
	sealed := sink.sealed
	next.Publish.Sealed = &sealed
	next.FileStates[0].State = FileStateComplete
	next.FileStates[0].Size = sealed.Size
	next.FileStates[0].Checksum = sealed.Checksum
	// Never delete sealed staging on a persistence failure: an on-disk snapshot
	// may already reference it even when Save returned an error.
	if err := savePublish(ctx, store, m, next); err != nil {
		return ExportRecoveryResult{}, jobError(m, err)
	}
	return c.commit(ctx, store, m, h)
}

func (c *PublishCoordinator) Recover(ctx context.Context, job string) (ExportRecoveryResult, error) {
	store, unlock, err := c.open(ctx, job)
	if err != nil {
		return ExportRecoveryResult{}, err
	}
	defer unlock()
	m, err := store.Load(ctx)
	if err != nil {
		return ExportRecoveryResult{}, &ExportJobError{JobID: job, Err: err}
	}
	if err := ValidateManifestJob(m, job); err != nil {
		return ExportRecoveryResult{}, jobError(m, err)
	}
	upper, err := manifestJSONUpperBound(m)
	if err != nil {
		return ExportRecoveryResult{}, jobError(m, err)
	}
	metadata, err := c.Scope.Reserve(memory.TagMetadata, upper*2)
	if err != nil {
		return ExportRecoveryResult{}, jobError(m, err)
	}
	defer metadata.Release()
	if m.ManifestVersion == 1 {
		return c.recoverV1(ctx, m)
	}
	driver, err := c.driver(ctx, m)
	if err != nil {
		return ExportRecoveryResult{}, jobError(m, err)
	}
	switch m.ExportState {
	case ExportStatePlanned:
		return ExportRecoveryResult{}, jobError(m, ErrExportNotRecoverable)
	case ExportStateFailed, ExportStateWriting:
		h, err := driver.ResumePublish(ctx, m.Publish.Intent, m.Publish.Ownership, m.Publish.Sealed, c.Scope, c.Buffers)
		if err == nil {
			c.cleanup(ctx, store, m, h)
			_ = h.Release()
		}
		if m.Publish.Failure == "conflict" {
			return ExportRecoveryResult{}, jobError(m, ErrCommitConflict)
		}
		return ExportRecoveryResult{}, jobError(m, errors.Join(ErrExportNotRecoverable, err))
	}
	h, err := driver.ResumePublish(ctx, m.Publish.Intent, m.Publish.Ownership, m.Publish.Sealed, c.Scope, c.Buffers)
	if err != nil {
		if errors.Is(err, vfs.ErrInvalidSealedObject) || errors.Is(err, os.ErrNotExist) {
			return ExportRecoveryResult{}, jobError(m, errors.Join(ErrExportNotRecoverable, err))
		}
		if m.ExportState == ExportStateCommitting {
			return ExportRecoveryResult{}, uncertain(m, err)
		}
		return ExportRecoveryResult{}, jobError(m, err)
	}
	defer h.Release()
	if pin, ok := h.(vfs.PublishedIdentityPinner); ok && m.FileStates[0].PublishedIdentity != nil {
		id := m.FileStates[0].PublishedIdentity
		pin.PinPublishedIdentity(vfs.ObjectIdentity{Size: id.Size, ETag: id.ETag, VersionID: id.VersionID})
	}
	return c.commit(ctx, store, m, h)
}

func (c *PublishCoordinator) commit(ctx context.Context, store ManifestStore, m *Manifest, h vfs.PublishHandle) (ExportRecoveryResult, error) {
	sealed := *m.Publish.Sealed
	identity, err := h.Verify(ctx, sealed, true)
	if m.ExportState == ExportStateCommitted {
		if err != nil {
			if errors.Is(err, vfs.ErrPublishConflict) {
				err = ErrCommitConflict
			}
			if errors.Is(err, os.ErrNotExist) {
				err = ErrExportNotRecoverable
			}
			return ExportRecoveryResult{}, jobError(m, err)
		}
		c.cleanup(ctx, store, m, h)
		return publishResult(m), nil
	}
	if err == nil {
		if m.ExportState == ExportStatePrepared {
			c.fail(ctx, store, m, h, "conflict")
			return ExportRecoveryResult{}, jobError(m, ErrCommitConflict)
		}
		return c.confirm(ctx, store, m, h, identity)
	}
	if errors.Is(err, vfs.ErrPublishConflict) {
		c.fail(ctx, store, m, h, "conflict")
		return ExportRecoveryResult{}, jobError(m, ErrCommitConflict)
	}
	if !errors.Is(err, os.ErrNotExist) {
		if m.ExportState == ExportStateCommitting {
			return ExportRecoveryResult{}, uncertain(m, err)
		}
		return ExportRecoveryResult{}, jobError(m, err)
	}
	if _, err := h.Verify(ctx, sealed, false); err != nil {
		if errors.Is(err, os.ErrNotExist) || errors.Is(err, vfs.ErrInvalidSealedObject) || errors.Is(err, vfs.ErrPublishConflict) {
			return ExportRecoveryResult{}, jobError(m, ErrExportNotRecoverable)
		}
		if m.ExportState == ExportStateCommitting {
			return ExportRecoveryResult{}, uncertain(m, err)
		}
		return ExportRecoveryResult{}, jobError(m, err)
	}
	if m.ExportState == ExportStatePrepared {
		next := cloneManifest(m)
		next.ExportState = ExportStateCommitting
		if err := savePublish(ctx, store, m, next); err != nil {
			return ExportRecoveryResult{}, jobError(m, err)
		}
	}
	if _, err := h.Publish(ctx, sealed); err != nil {
		if errors.Is(err, vfs.ErrPublishConflict) {
			c.fail(ctx, store, m, h, "conflict")
			return ExportRecoveryResult{}, jobError(m, ErrCommitConflict)
		}
		if errors.Is(err, vfs.ErrPublishFailed) || errors.Is(err, vfs.ErrInvalidSealedObject) {
			c.fail(ctx, store, m, h, "not_recoverable")
			return ExportRecoveryResult{}, jobError(m, err)
		}
		return ExportRecoveryResult{}, uncertain(m, err)
	}
	identity, err = h.Verify(ctx, sealed, true)
	if err != nil {
		if errors.Is(err, vfs.ErrPublishConflict) {
			c.fail(ctx, store, m, h, "conflict")
			return ExportRecoveryResult{}, jobError(m, ErrCommitConflict)
		}
		return ExportRecoveryResult{}, uncertain(m, err)
	}
	return c.confirm(ctx, store, m, h, identity)
}

func (c *PublishCoordinator) confirm(ctx context.Context, store ManifestStore, m *Manifest, h vfs.PublishHandle, identity vfs.ObjectIdentity) (ExportRecoveryResult, error) {
	next := cloneManifest(m)
	next.ExportState = ExportStateCommitted
	if identity.Stable() {
		next.FileStates[0].PublishedIdentity = &PublishedObjectIdentity{Size: identity.Size, ETag: identity.ETag, VersionID: identity.VersionID}
	}
	if err := savePublish(ctx, store, m, next); err != nil {
		return ExportRecoveryResult{}, uncertain(m, err)
	}
	c.cleanup(ctx, store, m, h)
	return publishResult(m), nil
}

func (c *PublishCoordinator) fail(ctx context.Context, store ManifestStore, m *Manifest, h vfs.PublishHandle, reason string) {
	ctx, cancel := context.WithTimeout(context.WithoutCancel(ctx), 5*time.Second)
	defer cancel()
	next := cloneManifest(m)
	next.ExportState = ExportStateFailed
	next.Publish.Failure = reason
	if err := savePublish(ctx, store, m, next); err != nil {
		return
	}
	c.cleanup(ctx, store, m, h)
}

func (c *PublishCoordinator) cleanup(ctx context.Context, store ManifestStore, m *Manifest, h vfs.PublishHandle) {
	if !m.Publish.CleanupPending {
		return
	}
	ctx, cancel := context.WithTimeout(context.WithoutCancel(ctx), 5*time.Second)
	defer cancel()
	if err := h.Abort(ctx, m.Publish.Sealed); err != nil {
		return
	}
	next := cloneManifest(m)
	next.Publish.CleanupPending = false
	_ = savePublish(ctx, store, m, next)
}

func publishResult(m *Manifest) ExportRecoveryResult {
	return ExportRecoveryResult{JobID: m.JobID, Destination: m.Publish.Intent.Destination, PublishMode: m.Publish.Intent.Mode, Bytes: m.Publish.Sealed.Size, Checksum: m.Publish.Sealed.Checksum}
}

type stagedByteSink struct {
	handle  vfs.PublishHandle
	digest  hash.Hash
	written int64
	sealed  vfs.SealedObject
	closed  bool
	failed  error
}

func (s *stagedByteSink) Write(ctx context.Context, p []byte) (int, error) {
	if s.closed {
		return 0, ErrWriterClosed
	}
	if s.failed != nil {
		return 0, s.failed
	}
	n, err := s.handle.Write(ctx, p)
	if n < 0 || n > len(p) {
		s.failed = io.ErrShortWrite
		return 0, s.failed
	}
	if n > 0 {
		s.digest.Write(p[:n])
		s.written += int64(n)
	}
	if err == nil && n != len(p) {
		err = io.ErrShortWrite
	}
	s.failed = err
	return n, err
}
func (s *stagedByteSink) Flush(ctx context.Context) error {
	if s.closed {
		return ErrWriterClosed
	}
	return errors.Join(ctx.Err(), s.failed)
}
func (s *stagedByteSink) Close(ctx context.Context) (WriteResult, error) {
	if s.closed {
		return WriteResult{Bytes: s.sealed.Size, Checksum: s.sealed.Checksum}, s.failed
	}
	s.closed = true
	if s.failed != nil {
		return WriteResult{}, s.failed
	}
	s.sealed, s.failed = s.handle.Seal(ctx, vfs.WriteEvidence{Size: s.written, Checksum: hex.EncodeToString(s.digest.Sum(nil))})
	if s.failed == nil && (s.sealed.Size != s.written || s.sealed.Checksum != hex.EncodeToString(s.digest.Sum(nil))) {
		s.failed = vfs.ErrInvalidSealedObject
	}
	return WriteResult{Bytes: s.sealed.Size, Checksum: s.sealed.Checksum}, s.failed
}

// Ownership cleanup belongs to the locked coordinator, even if a format writer
// aborts during its own Close. Releasing format buffers cannot delete staging.
func (s *stagedByteSink) Abort(context.Context) error {
	s.closed = true
	s.failed = ErrWriterClosed
	return nil
}
