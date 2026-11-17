package vfs

import (
	"context"
	"errors"
	"fmt"

	"github.com/steamwo/fssql/internal/core/memory"
)

type PublishMode string

const (
	PublishStrict     PublishMode = "strict"
	PublishCompatible PublishMode = "compatible"
)

var (
	ErrPublishConflict     = errors.New("vfs: publish destination conflicts")
	ErrInvalidSealedObject = errors.New("vfs: sealed payload unavailable or invalid")
	ErrPublishFailed       = errors.New("vfs: publish rejected")
	ErrCommitUncertain     = errors.New("export: commit uncertain")
)

// Four MiB accommodates the existing 10,000-part S3 limit, including checksums.
const MaxPublishTokenBytes = 4 << 20

type CommitUncertainError struct {
	JobID string
	Err   error
}

func (e *CommitUncertainError) Error() string {
	return fmt.Sprintf("export job %s: commit uncertain: %v", e.JobID, e.Err)
}
func (e *CommitUncertainError) Unwrap() error        { return e.Err }
func (e *CommitUncertainError) Is(target error) bool { return target == ErrCommitUncertain }

// PublishIntent is persisted before BeginPublish may create any resources.
// Token and ownership are driver-private, bounded, credential-free metadata.
type PublishIntent struct {
	JobID       string      `json:"job_id"`
	Destination string      `json:"destination"`
	Protocol    string      `json:"protocol"`
	Mode        PublishMode `json:"mode"`
	ScopeKey    string      `json:"scope_key"`
	Token       []byte      `json:"token"`
}

type WriteEvidence struct {
	Size     int64  `json:"size"`
	Checksum string `json:"checksum"`
}

type SealedObject struct {
	Size          int64  `json:"size"`
	Checksum      string `json:"checksum"`
	RecoveryToken []byte `json:"recovery_token"`
}

type RecordOwnership func(context.Context, []byte) error

// StagedPublishDriver never publishes through Close. A successful ownership
// callback is required before payload writes. Resume must reauthorize all tokens.
type StagedPublishDriver interface {
	PlanPublish(context.Context, URI, string, PublishMode) (PublishIntent, error)
	BeginPublish(context.Context, PublishIntent, RecordOwnership, *memory.Scope, *memory.BufferManager) (PublishHandle, error)
	ResumePublish(context.Context, PublishIntent, []byte, *SealedObject, *memory.Scope, *memory.BufferManager) (PublishHandle, error)
}

type PublishHandle interface {
	Write(context.Context, []byte) (int, error)
	Seal(context.Context, WriteEvidence) (SealedObject, error)
	Publish(context.Context, SealedObject) (ObjectIdentity, error)
	// Verify checks physical bytes, including complete SHA-256 and stable path
	// identity. final=false checks staging. Only explicit not-found means absent.
	Verify(context.Context, SealedObject, bool) (ObjectIdentity, error)
	// Abort may remove only proven job-owned staging, including after commit.
	Abort(context.Context, *SealedObject) error
	// Release closes process resources without deleting recoverable staging.
	Release() error
}

// PublishedIdentityPinner optionally restores a final protocol identity from a
// COMMITTED manifest. It cannot authorize rebuilding or overwriting a target.
type PublishedIdentityPinner interface{ PinPublishedIdentity(ObjectIdentity) }
