package export

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"os"
	"path"

	"github.com/steamwo/fssql/internal/core/memory"
	"github.com/steamwo/fssql/internal/storage/vfs"
)

// v1 has no sealed staging evidence. It can only confirm an already visible
// object with a recorded stable protocol identity, never resume an upload.
func (c *PublishCoordinator) recoverV1(ctx context.Context, m *Manifest) (ExportRecoveryResult, error) {
	if m.SegmentPlan != nil || len(m.FileStates) != 1 || (m.ExportState != ExportStateCommitted && m.ExportState != ExportStateCommitting && m.ExportState != ExportStateWriting) {
		return ExportRecoveryResult{}, jobError(m, ErrExportNotRecoverable)
	}
	f := m.FileStates[0]
	if f.State != FileStateComplete || f.PublishedIdentity == nil || !f.PublishedIdentity.Stable() || f.Path != path.Base(f.Path) {
		return ExportRecoveryResult{}, jobError(m, ErrExportNotRecoverable)
	}
	u, err := vfs.ParseURI(m.Destination.URI)
	if err != nil || u.IsHTTP() {
		return ExportRecoveryResult{}, jobError(m, ErrExportNotRecoverable)
	}
	u.Path = path.Join(u.Path, f.Path)
	d, err := c.Registry.DriverFor(u)
	if err != nil {
		return ExportRecoveryResult{}, jobError(m, err)
	}
	info, err := d.Stat(ctx, u)
	if err != nil {
		if errors.Is(err, os.ErrNotExist) {
			err = ErrExportNotRecoverable
		}
		return ExportRecoveryResult{}, jobError(m, err)
	}
	expected := vfs.ObjectIdentity{ETag: f.PublishedIdentity.ETag, VersionID: f.PublishedIdentity.VersionID, Size: f.Size}
	if info.Size != f.Size || !info.Identity.Stable() || !expected.Matches(info.Identity) {
		return ExportRecoveryResult{}, jobError(m, ErrCommitConflict)
	}
	// The legacy local ETag encodes size/mtime, which is insufficient to prove
	// byte equality. Require its recorded full checksum and verify raw bytes.
	if u.Scheme == "file" {
		if !validChecksum(f.Checksum) {
			return ExportRecoveryResult{}, jobError(m, ErrExportNotRecoverable)
		}
		digest := sha256.New()
		for offset := int64(0); offset < f.Size; {
			length := int64(64 << 10)
			if length > f.Size-offset {
				length = f.Size - offset
			}
			r, err := d.ReadRange(ctx, vfs.RangeRequest{URI: u, Offset: offset, Length: length, Expected: &info.Identity, Tag: memory.TagExportBuffer}, c.Scope, c.Buffers)
			if err != nil {
				return ExportRecoveryResult{}, jobError(m, err)
			}
			digest.Write(r.Buffer.Bytes())
			r.Buffer.Release()
			offset += length
		}
		if hex.EncodeToString(digest.Sum(nil)) != f.Checksum {
			return ExportRecoveryResult{}, jobError(m, ErrCommitConflict)
		}
	}
	return ExportRecoveryResult{JobID: m.JobID, Destination: u.String(), PublishMode: vfs.PublishStrict, Bytes: f.Size, Checksum: f.Checksum}, nil
}
