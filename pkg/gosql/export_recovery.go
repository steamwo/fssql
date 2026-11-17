package gosql

import (
	"context"
	"errors"
	"path/filepath"

	exportpkg "github.com/steamwo/fssql/internal/export"
	"github.com/steamwo/fssql/internal/query"
)

type CopyPublishMode string

const (
	CopyPublishStrict     CopyPublishMode = "strict"
	CopyPublishCompatible CopyPublishMode = "compatible"
)

var (
	ErrCommitUncertain      = exportpkg.ErrCommitUncertain
	ErrCommitConflict       = exportpkg.ErrCommitConflict
	ErrExportNotRecoverable = exportpkg.ErrExportNotRecoverable
)

type CommitUncertainError = exportpkg.CommitUncertainError
type ExportJobError = exportpkg.ExportJobError

type ExportRecoveryResult struct {
	JobID       string
	Destination string
	PublishMode CopyPublishMode
	Bytes       int64
	Checksum    string
}

// RecoverExport only reconciles durable single-file export evidence. It never
// prepares or executes SQL and uses current configured drivers and credentials.
func (db *DB) RecoverExport(ctx context.Context, jobID string) (ExportRecoveryResult, error) {
	if db == nil || db.memory == nil || db.exportRegistry == nil {
		return ExportRecoveryResult{}, errors.New("gosql: DB is uninitialized")
	}
	if db.closed.Load() || db.owner != nil && db.owner.closed.Load() {
		return ExportRecoveryResult{}, errors.New("gosql: DB is closed")
	}
	if _, err := exportpkg.ParseJobID(jobID); err != nil {
		return ExportRecoveryResult{}, err
	}
	q := query.New(ctx, db.memory)
	c := exportpkg.PublishCoordinator{ManifestDir: db.manifestDir, Registry: db.exportRegistry, Scope: q.Memory, Buffers: db.buffers, Store: func(job string) (exportpkg.ManifestStore, error) {
		return exportpkg.NewLocalManifestStore(filepath.Join(db.manifestDir, job+".json"), 0, q.Memory, db.buffers)
	}}
	r, err := c.Recover(ctx, jobID)
	err = errors.Join(err, q.Close())
	if err != nil {
		return ExportRecoveryResult{}, err
	}
	return ExportRecoveryResult{JobID: r.JobID, Destination: r.Destination, PublishMode: CopyPublishMode(r.PublishMode), Bytes: r.Bytes, Checksum: r.Checksum}, nil
}
