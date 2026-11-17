package export

import (
	"crypto/rand"
	"encoding/hex"
	"fmt"
)

type JobID string

func parseJobID(value string) (JobID, error) {
	if len(value) != 32 && len(value) != 64 {
		return "", fmt.Errorf("%w: invalid JobID", ErrInvalidManifest)
	}
	for _, c := range value {
		if !(c >= '0' && c <= '9' || c >= 'a' && c <= 'f') {
			return "", fmt.Errorf("%w: invalid JobID", ErrInvalidManifest)
		}
	}
	return JobID(value), nil
}

func ParseJobID(value string) (JobID, error) { return parseJobID(value) }

func NewJobID() (JobID, error) {
	var id [16]byte
	if _, err := rand.Read(id[:]); err != nil {
		return "", err
	}
	return JobID(hex.EncodeToString(id[:])), nil
}

// ValidateManifestJob prevents a valid filename from selecting a different job
// or a manifest with the wrong generation of identifiers.
func ValidateManifestJob(m *Manifest, jobID string) error {
	if _, err := parseJobID(jobID); err != nil {
		return err
	}
	if err := m.Validate(); err != nil {
		return err
	}
	if m.JobID != jobID || (m.ManifestVersion == 1 && len(jobID) != 64) || (m.ManifestVersion == 2 && len(jobID) != 32) {
		return fmt.Errorf("%w: manifest JobID/version mismatch", ErrInvalidManifest)
	}
	return nil
}
