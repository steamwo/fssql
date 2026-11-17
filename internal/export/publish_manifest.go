package export

import (
	"bytes"
	"encoding/json"
	"fmt"
	"net/url"
	"path"
	"strings"

	"github.com/steamwo/fssql/internal/storage/vfs"
)

const MaxPublishTokenBytes = vfs.MaxPublishTokenBytes

type PublishRecord struct {
	Intent         vfs.PublishIntent `json:"intent"`
	Ownership      []byte            `json:"ownership,omitempty"`
	Sealed         *vfs.SealedObject `json:"sealed,omitempty"`
	CleanupPending bool              `json:"cleanup_pending,omitempty"`
	Failure        string            `json:"failure,omitempty"`
}

func NewPublishManifest(jobID, query, source, format, compression string, intent vfs.PublishIntent) (*Manifest, error) {
	u, err := vfs.ParseURI(intent.Destination)
	if err != nil {
		return nil, err
	}
	file := path.Base(u.Path)
	u.Path = path.Dir(u.Path)
	m, err := NewManifest(jobID, query, source, u.String(), format, compression, "staged_publish", []string{file})
	if err != nil {
		return nil, err
	}
	m.ManifestVersion, m.Revision = 2, 1
	m.Publish = &PublishRecord{Intent: intent}
	return m, m.Validate()
}

func validChecksum(s string) bool {
	if len(s) != 64 {
		return false
	}
	for _, c := range s {
		if !(c >= '0' && c <= '9' || c >= 'a' && c <= 'f') {
			return false
		}
	}
	return true
}

func canTransitionPublish(from, to ExportState) bool {
	switch from {
	case ExportStatePlanned:
		return to == ExportStateWriting || to == ExportStateFailed
	case ExportStateWriting:
		return to == ExportStatePrepared || to == ExportStateFailed
	case ExportStatePrepared:
		return to == ExportStateCommitting || to == ExportStateFailed
	case ExportStateCommitting:
		return to == ExportStateCommitted || to == ExportStateFailed
	}
	return false
}

func (m *Manifest) validatePublish() error {
	bad := func() error { return fmt.Errorf("%w: invalid staged publish evidence", ErrInvalidManifest) }
	if _, err := parseJobID(m.JobID); err != nil {
		return err
	}
	if len(m.JobID) != 32 || m.Revision == 0 || m.SegmentPlan != nil || len(m.FileStates) != 1 || m.Publish == nil {
		return bad()
	}
	p := m.Publish
	i := p.Intent
	if i.JobID != m.JobID || i.Protocol == "" || i.ScopeKey == "" || (i.Mode != vfs.PublishStrict && i.Mode != vfs.PublishCompatible) || len(i.Token) == 0 || len(i.Token) > MaxPublishTokenBytes || len(p.Ownership) > MaxPublishTokenBytes {
		return bad()
	}
	u, err := url.Parse(i.Destination)
	if err != nil || u.User != nil || u.RawQuery != "" || u.ForceQuery || u.Fragment != "" || strings.Contains(i.Destination, "#") {
		return bad()
	}
	dest, err := vfs.ParseURI(i.Destination)
	if err != nil || dest.IsHTTP() || dest.Scheme != i.Protocol {
		return bad()
	}
	root, err := vfs.ParseURI(m.Destination.URI)
	f := m.FileStates[0]
	if err != nil || f.Path != path.Base(f.Path) || strings.ContainsAny(f.Path, "\\") || f.Path == "." || f.Path == ".." {
		return bad()
	}
	root.Path = path.Join(root.Path, f.Path)
	if root.IdentityKey() != dest.IdentityKey() {
		return bad()
	}
	if p.Failure != "" && p.Failure != "conflict" && p.Failure != "not_recoverable" {
		return bad()
	}
	if p.Sealed != nil {
		s := p.Sealed
		if len(p.Ownership) == 0 || s.Size < 0 || !validChecksum(s.Checksum) || len(s.RecoveryToken) == 0 || len(s.RecoveryToken) > MaxPublishTokenBytes || f.State != FileStateComplete || f.Size != s.Size || f.Checksum != s.Checksum {
			return bad()
		}
	}
	if m.ExportState == ExportStatePrepared || m.ExportState == ExportStateCommitting || m.ExportState == ExportStateCommitted {
		if p.Sealed == nil || p.Failure != "" {
			return bad()
		}
	}
	if m.ExportState == ExportStatePlanned && (len(p.Ownership) != 0 || p.Sealed != nil || p.CleanupPending) {
		return bad()
	}
	return nil
}

// validatePublishUpdate runs under the job lock; equality of immutable metadata
// and monotonic revisions prevent stale cleanup paths from reverting a commit.
func validatePublishUpdate(old, next *Manifest) error {
	if old.ManifestVersion != 2 || next.ManifestVersion != 2 || next.Revision != old.Revision+1 {
		return ErrStaleRevision
	}
	if old.ExportState != next.ExportState && !canTransitionPublish(old.ExportState, next.ExportState) {
		return ErrInvalidTransition
	}
	a, b := cloneManifest(old), cloneManifest(next)
	a.Revision, b.Revision = 0, 0
	a.ExportState, b.ExportState = "", ""
	a.FileStates, b.FileStates = nil, nil
	a.Publish.Ownership, b.Publish.Ownership = nil, nil
	a.Publish.Sealed, b.Publish.Sealed = nil, nil
	a.Publish.CleanupPending, b.Publish.CleanupPending = false, false
	a.Publish.Failure, b.Publish.Failure = "", ""
	aj, _ := json.Marshal(a)
	bj, _ := json.Marshal(b)
	if !bytes.Equal(aj, bj) {
		return fmt.Errorf("%w: immutable publish identity changed", ErrInvalidManifest)
	}
	if old.Publish.Sealed != nil {
		a, _ := json.Marshal(old.Publish.Sealed)
		b, _ := json.Marshal(next.Publish.Sealed)
		if !bytes.Equal(a, b) || !bytes.Equal(old.Publish.Ownership, next.Publish.Ownership) {
			return ErrInvalidTransition
		}
	}
	if (old.ExportState == ExportStateCommitted || old.ExportState == ExportStateFailed) && old.Publish.Failure != next.Publish.Failure {
		return ErrInvalidTransition
	}
	return nil
}
