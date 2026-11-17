package export

import (
	"errors"
	"fmt"

	"github.com/steamwo/fssql/internal/storage/vfs"
)

var (
	ErrCommitUncertain      = vfs.ErrCommitUncertain
	ErrExportNotRecoverable = errors.New("export: not recoverable")
	ErrStaleRevision        = errors.New("export: stale manifest revision")
)

type ExportJobError struct {
	JobID string
	Err   error
}

func (e *ExportJobError) Error() string { return fmt.Sprintf("export job %s: %v", e.JobID, e.Err) }
func (e *ExportJobError) Unwrap() error { return e.Err }

type CommitUncertainError = vfs.CommitUncertainError
