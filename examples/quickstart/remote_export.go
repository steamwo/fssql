package main

import (
	"context"
	"errors"

	"github.com/steamwo/fssql/pkg/gosql"
)

// CopyRemote returns a JobID even when an export error exposes one. Persist that
// ID with config.ManifestDir; resolve uncertainty with RecoverRemote, using a
// fresh context if the copy context timed out. Do not rerun SQL to recover.
// FTP/FTPS requires CopyPublishCompatible. SFTP/WebDAV strict prerequisites are
// configured on the supplied endpoint, as described in docs/public-api.md.
func CopyRemote(ctx context.Context, config gosql.Config, querySQL, destination string, mode gosql.CopyPublishMode) (string, error) {
	db, err := gosql.Open(config)
	if err != nil {
		return "", err
	}
	defer db.Close()
	result, err := db.CopyTo(ctx, querySQL, destination, gosql.CopyOptions{Format: gosql.CopyFormatCSV, PublishMode: mode})
	if err == nil {
		return result.JobID, nil
	}
	var uncertain *gosql.CommitUncertainError
	if errors.As(err, &uncertain) {
		return uncertain.JobID, err
	}
	var job *gosql.ExportJobError
	if errors.As(err, &job) {
		return job.JobID, err
	}
	return "", err
}

// RecoverRemote only reconciles the existing job using current authorization.
// config must retain the original ManifestDir and target publication scope.
func RecoverRemote(ctx context.Context, config gosql.Config, jobID string) (gosql.ExportRecoveryResult, error) {
	db, err := gosql.Open(config)
	if err != nil {
		return gosql.ExportRecoveryResult{}, err
	}
	defer db.Close()
	return db.RecoverExport(ctx, jobID)
}
