package export

import (
	"encoding/json"
	"errors"
	"fmt"
	"strings"
)

const CurrentManifestVersion = 1

var (
	ErrInvalidManifest   = errors.New("export: invalid manifest")
	ErrInvalidTransition = errors.New("export: invalid state transition")
)

type ExportState string

const (
	ExportStatePlanned    ExportState = "PLANNED"
	ExportStateWriting    ExportState = "WRITING"
	ExportStateCommitting ExportState = "COMMITTING"
	ExportStateCommitted  ExportState = "COMMITTED"
	ExportStateFailed     ExportState = "FAILED"
)

type FileState string

const (
	FileStatePlanned  FileState = "PLANNED"
	FileStateWriting  FileState = "WRITING"
	FileStateComplete FileState = "COMPLETE"
	FileStateFailed   FileState = "FAILED"
)

type SourceSnapshotRef struct {
	Fingerprint string `json:"fingerprint"`
}

type Destination struct {
	URI string `json:"uri"`
}

type PublishedObjectIdentity struct {
	ETag      string `json:"etag,omitempty"`
	VersionID string `json:"version_id,omitempty"`
	Size      int64  `json:"size"`
}

func (i PublishedObjectIdentity) Stable() bool { return i.ETag != "" || i.VersionID != "" }

type FileEntry struct {
	Path              string                   `json:"path"`
	State             FileState                `json:"state"`
	Size              int64                    `json:"size,omitempty"`
	Checksum          string                   `json:"checksum,omitempty"`
	PublishedIdentity *PublishedObjectIdentity `json:"published_identity,omitempty"`
}

// Manifest is durable recovery state. Encryption identity is persisted, key material never is.
type Manifest struct {
	ManifestVersion     int               `json:"manifest_version"`
	JobID               string            `json:"job_id"`
	QueryFingerprint    string            `json:"query_fingerprint"`
	SourceSnapshot      SourceSnapshotRef `json:"source_snapshot"`
	Destination         Destination       `json:"destination"`
	Format              string            `json:"format"`
	Compression         string            `json:"compression"`
	Codec               string            `json:"codec,omitempty"`
	EncryptionAlgorithm string            `json:"encryption_algorithm,omitempty"`
	KeyID               string            `json:"key_id,omitempty"`
	Consistency         string            `json:"consistency"`
	SegmentPlan         *SegmentPlan      `json:"segment_plan,omitempty"`
	FileStates          []FileEntry       `json:"file_states"`
	ExportState         ExportState       `json:"export_state"`
}

func NewManifest(jobID, queryFingerprint, sourceFingerprint, destinationURI, format, compression, consistency string, files []string) (*Manifest, error) {
	m := &Manifest{
		ManifestVersion:  CurrentManifestVersion,
		JobID:            strings.TrimSpace(jobID),
		QueryFingerprint: strings.TrimSpace(queryFingerprint),
		SourceSnapshot:   SourceSnapshotRef{Fingerprint: strings.TrimSpace(sourceFingerprint)},
		Destination:      Destination{URI: strings.TrimSpace(destinationURI)},
		Format:           strings.ToLower(strings.TrimSpace(format)),
		Compression:      strings.ToLower(strings.TrimSpace(compression)),
		Consistency:      strings.TrimSpace(consistency),
		ExportState:      ExportStatePlanned,
		FileStates:       make([]FileEntry, len(files)),
	}
	for i, file := range files {
		m.FileStates[i] = FileEntry{Path: strings.TrimSpace(file), State: FileStatePlanned}
	}
	if err := m.Validate(); err != nil {
		return nil, err
	}
	return m, nil
}

func (m *Manifest) Validate() error {
	if m == nil {
		return fmt.Errorf("%w: nil manifest", ErrInvalidManifest)
	}
	if m.ManifestVersion != CurrentManifestVersion {
		return fmt.Errorf("%w: unsupported manifest version %d", ErrInvalidManifest, m.ManifestVersion)
	}
	if strings.TrimSpace(m.JobID) == "" || strings.TrimSpace(m.QueryFingerprint) == "" || strings.TrimSpace(m.SourceSnapshot.Fingerprint) == "" {
		return fmt.Errorf("%w: missing job/query/source identity", ErrInvalidManifest)
	}
	if strings.TrimSpace(m.Destination.URI) == "" || strings.TrimSpace(m.Format) == "" || strings.TrimSpace(m.Consistency) == "" {
		return fmt.Errorf("%w: missing destination/format/consistency", ErrInvalidManifest)
	}
	if (strings.TrimSpace(m.EncryptionAlgorithm) == "") != (strings.TrimSpace(m.KeyID) == "") {
		return fmt.Errorf("%w: encryption algorithm and key_id must be present together", ErrInvalidManifest)
	}
	if m.SegmentPlan != nil {
		if err := m.SegmentPlan.Validate(); err != nil {
			return err
		}
	}
	if !validExportState(m.ExportState) {
		return fmt.Errorf("%w: unknown export state %q", ErrInvalidManifest, m.ExportState)
	}
	seen := make(map[string]struct{}, len(m.FileStates))
	for i := range m.FileStates {
		f := m.FileStates[i]
		if strings.TrimSpace(f.Path) == "" {
			return fmt.Errorf("%w: file %d has empty path", ErrInvalidManifest, i)
		}
		if _, ok := seen[f.Path]; ok {
			return fmt.Errorf("%w: duplicate file path %q", ErrInvalidManifest, f.Path)
		}
		seen[f.Path] = struct{}{}
		if !validFileState(f.State) {
			return fmt.Errorf("%w: file %q has unknown state %q", ErrInvalidManifest, f.Path, f.State)
		}
		if f.Size < 0 {
			return fmt.Errorf("%w: file %q has negative size", ErrInvalidManifest, f.Path)
		}
		if f.PublishedIdentity != nil {
			if f.State != FileStateComplete {
				return fmt.Errorf("%w: file %q has published identity before COMPLETE", ErrInvalidManifest, f.Path)
			}
			if f.PublishedIdentity.Size < 0 || f.PublishedIdentity.Size != f.Size {
				return fmt.Errorf("%w: file %q published identity size mismatch", ErrInvalidManifest, f.Path)
			}
		}
	}
	if m.ExportState == ExportStateCommitted && !m.AllFilesComplete() {
		return fmt.Errorf("%w: committed export contains incomplete files", ErrInvalidManifest)
	}
	return nil
}

func (m *Manifest) TransitionExport(next ExportState) error {
	if m == nil {
		return fmt.Errorf("%w: nil manifest", ErrInvalidTransition)
	}
	if !canTransitionExport(m.ExportState, next) {
		return fmt.Errorf("%w: export %s -> %s", ErrInvalidTransition, m.ExportState, next)
	}
	if next == ExportStateCommitting && !m.AllFilesComplete() {
		return fmt.Errorf("%w: cannot commit before every file is complete", ErrInvalidTransition)
	}
	m.ExportState = next
	return nil
}

func (m *Manifest) TransitionFile(path string, next FileState, size int64, checksum string) error {
	return m.transitionFile(path, next, size, checksum, nil)
}

func (m *Manifest) TransitionFilePublished(path string, size int64, checksum string, identity PublishedObjectIdentity) error {
	if !identity.Stable() {
		return fmt.Errorf("%w: published identity is not stable", ErrInvalidTransition)
	}
	if identity.Size != size {
		return fmt.Errorf("%w: published identity size %d != file size %d", ErrInvalidTransition, identity.Size, size)
	}
	copy := identity
	return m.transitionFile(path, FileStateComplete, size, checksum, &copy)
}

func (m *Manifest) transitionFile(path string, next FileState, size int64, checksum string, identity *PublishedObjectIdentity) error {
	if m == nil {
		return fmt.Errorf("%w: nil manifest", ErrInvalidTransition)
	}
	if size < 0 {
		return fmt.Errorf("%w: negative file size", ErrInvalidTransition)
	}
	for i := range m.FileStates {
		f := &m.FileStates[i]
		if f.Path != path {
			continue
		}
		if !canTransitionFile(f.State, next) {
			return fmt.Errorf("%w: file %q %s -> %s", ErrInvalidTransition, path, f.State, next)
		}
		f.State = next
		if next == FileStateComplete {
			f.Size = size
			f.Checksum = checksum
			f.PublishedIdentity = identity
		} else {
			f.Size = 0
			f.Checksum = ""
			f.PublishedIdentity = nil
		}
		return nil
	}
	return fmt.Errorf("%w: unknown file %q", ErrInvalidTransition, path)
}

func (m *Manifest) AllFilesComplete() bool {
	if m == nil || len(m.FileStates) == 0 {
		return false
	}
	for i := range m.FileStates {
		if m.FileStates[i].State != FileStateComplete {
			return false
		}
	}
	return true
}

func (m *Manifest) MarshalBinary() ([]byte, error) {
	if err := m.Validate(); err != nil {
		return nil, err
	}
	return json.Marshal(m)
}

func UnmarshalManifest(data []byte) (*Manifest, error) {
	var m Manifest
	if err := json.Unmarshal(data, &m); err != nil {
		return nil, fmt.Errorf("%w: decode: %v", ErrInvalidManifest, err)
	}
	if err := m.Validate(); err != nil {
		return nil, err
	}
	return &m, nil
}

func validExportState(s ExportState) bool {
	switch s {
	case ExportStatePlanned, ExportStateWriting, ExportStateCommitting, ExportStateCommitted, ExportStateFailed:
		return true
	default:
		return false
	}
}

func validFileState(s FileState) bool {
	switch s {
	case FileStatePlanned, FileStateWriting, FileStateComplete, FileStateFailed:
		return true
	default:
		return false
	}
}

func canTransitionExport(from, to ExportState) bool {
	switch from {
	case ExportStatePlanned:
		return to == ExportStateWriting || to == ExportStateFailed
	case ExportStateWriting:
		return to == ExportStateCommitting || to == ExportStateFailed
	case ExportStateCommitting:
		return to == ExportStateCommitted || to == ExportStateFailed
	case ExportStateFailed:
		return to == ExportStateWriting
	default:
		return false
	}
}

func canTransitionFile(from, to FileState) bool {
	switch from {
	case FileStatePlanned:
		return to == FileStateWriting || to == FileStateFailed
	case FileStateWriting:
		return to == FileStateComplete || to == FileStateFailed
	case FileStateFailed:
		return to == FileStateWriting
	default:
		return false
	}
}
