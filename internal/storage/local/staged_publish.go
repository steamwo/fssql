package local

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"io"
	"os"
	"path/filepath"
	"strings"

	"github.com/steamwo/fssql/internal/core/memory"
	"github.com/steamwo/fssql/internal/storage/vfs"
)

type localPublishToken struct {
	Mode        vfs.PublishMode `json:"mode"`
	Version     int             `json:"version"`
	JobID       string          `json:"job_id"`
	Destination string          `json:"destination"`
	Staging     string          `json:"staging"`
}

func (d *Driver) PlanPublish(ctx context.Context, u vfs.URI, job string, mode vfs.PublishMode) (vfs.PublishIntent, error) {
	if err := ctx.Err(); err != nil {
		return vfs.PublishIntent{}, err
	}
	if len(job) != 32 {
		return vfs.PublishIntent{}, vfs.ErrInvalidSealedObject
	}
	for _, c := range job {
		if !(c >= 'a' && c <= 'f' || c >= '0' && c <= '9') {
			return vfs.PublishIntent{}, vfs.ErrInvalidSealedObject
		}
	}
	if mode == "" {
		mode = vfs.PublishStrict
	}
	if mode != vfs.PublishStrict && mode != vfs.PublishCompatible {
		return vfs.PublishIntent{}, vfs.ErrWriteUnsupported
	}
	p, err := d.localPath(u, false)
	if err != nil {
		return vfs.PublishIntent{}, err
	}
	scope := sha256.Sum256([]byte(d.root))
	stage := filepath.Join(filepath.Dir(p), ".fssql-"+job+".staging")
	if strings.EqualFold(stage, p) {
		return vfs.PublishIntent{}, vfs.ErrInvalidSealedObject
	}
	token, _ := json.Marshal(localPublishToken{Mode: mode, Version: 1, JobID: job, Destination: u.String(), Staging: stage})
	return vfs.PublishIntent{JobID: job, Destination: u.String(), Protocol: "file", Mode: mode, ScopeKey: hex.EncodeToString(scope[:]), Token: token}, nil
}

func (d *Driver) publishPaths(ctx context.Context, intent vfs.PublishIntent) (string, string, error) {
	u, err := vfs.ParseURI(intent.Destination)
	if err != nil {
		return "", "", err
	}
	expected, err := d.PlanPublish(ctx, u, intent.JobID, intent.Mode)
	if err != nil {
		return "", "", err
	}
	if intent.Protocol != expected.Protocol || intent.ScopeKey != expected.ScopeKey || !bytes.Equal(intent.Token, expected.Token) {
		return "", "", vfs.ErrInvalidSealedObject
	}
	var token localPublishToken
	if err := json.Unmarshal(expected.Token, &token); err != nil {
		return "", "", err
	}
	final, err := d.localPath(u, false)
	return final, token.Staging, err
}

func (d *Driver) BeginPublish(ctx context.Context, intent vfs.PublishIntent, record vfs.RecordOwnership, scope *memory.Scope, bm *memory.BufferManager) (vfs.PublishHandle, error) {
	final, stage, err := d.publishPaths(ctx, intent)
	if err != nil {
		return nil, err
	}
	if record == nil || scope == nil || bm == nil {
		return nil, vfs.ErrWriteUnsupported
	}
	f, err := os.OpenFile(stage, os.O_CREATE|os.O_EXCL|os.O_RDWR, 0600)
	if err != nil {
		return nil, err
	}
	h := &localPublishHandle{driver: d, intent: intent, final: final, stage: stage, file: f, scope: scope, bm: bm}
	ready := false
	defer func() {
		if !ready {
			_ = h.Release()
		}
	}()
	h.owner, err = publishFileID(f)
	if err == nil {
		err = record(ctx, []byte(h.owner))
	}
	if err != nil {
		_ = h.Release()
		return h, err
	}
	ready = true
	return h, nil
}

func (d *Driver) ResumePublish(ctx context.Context, intent vfs.PublishIntent, owner []byte, sealed *vfs.SealedObject, scope *memory.Scope, bm *memory.BufferManager) (vfs.PublishHandle, error) {
	final, stage, err := d.publishPaths(ctx, intent)
	if err != nil {
		return nil, err
	}
	if scope == nil || bm == nil || len(owner) == 0 || len(owner) > 256 {
		return nil, vfs.ErrInvalidSealedObject
	}
	if sealed != nil && !bytes.Equal(sealed.RecoveryToken, intent.Token) {
		return nil, vfs.ErrInvalidSealedObject
	}
	return &localPublishHandle{driver: d, intent: intent, final: final, stage: stage, owner: string(owner), scope: scope, bm: bm}, nil
}

type localPublishHandle struct {
	driver              *Driver
	intent              vfs.PublishIntent
	final, stage, owner string
	file                *os.File
	scope               *memory.Scope
	bm                  *memory.BufferManager
	written             int64
	sealed              bool
	legacyReplace       bool // only the retained AtomicPublishWrite adapter may overwrite
	legacyWrite         bool // retains the legacy fsync/identity contract without a read buffer
}

func (h *localPublishHandle) Write(ctx context.Context, p []byte) (int, error) {
	if err := ctx.Err(); err != nil {
		return 0, err
	}
	if h.file == nil || h.sealed {
		return 0, vfs.ErrWriteClosed
	}
	n, err := h.file.Write(p)
	h.written += int64(n)
	return n, err
}

func (h *localPublishHandle) Seal(ctx context.Context, e vfs.WriteEvidence) (vfs.SealedObject, error) {
	if err := ctx.Err(); err != nil {
		return vfs.SealedObject{}, err
	}
	if h.file == nil || h.sealed || e.Size != h.written {
		return vfs.SealedObject{}, vfs.ErrInvalidSealedObject
	}
	if err := h.file.Sync(); err != nil {
		return vfs.SealedObject{}, err
	}
	if err := h.Release(); err != nil {
		return vfs.SealedObject{}, err
	}
	if err := syncPublishDirectory(filepath.Dir(h.stage)); err != nil {
		return vfs.SealedObject{}, err
	}
	h.sealed = true
	sealed := vfs.SealedObject{Size: e.Size, Checksum: e.Checksum, RecoveryToken: append([]byte(nil), h.intent.Token...)}
	if !h.legacyWrite {
		if _, err := h.Verify(ctx, sealed, false); err != nil {
			return vfs.SealedObject{}, err
		}
	}
	return sealed, nil
}

func (h *localPublishHandle) authorized(ctx context.Context) error {
	_, _, err := h.driver.publishPaths(ctx, h.intent)
	return err
}

func (h *localPublishHandle) openOwned() (*os.File, error) {
	info, err := os.Lstat(h.stage)
	if err != nil {
		return nil, err
	}
	if !info.Mode().IsRegular() {
		return nil, vfs.ErrInvalidSealedObject
	}
	f, err := os.Open(h.stage)
	if err != nil {
		return nil, err
	}
	id, err := publishFileID(f)
	if err != nil || h.owner == "" || id != h.owner {
		_ = f.Close()
		return nil, vfs.ErrInvalidSealedObject
	}
	return f, nil
}

func (h *localPublishHandle) Verify(ctx context.Context, s vfs.SealedObject, final bool) (vfs.ObjectIdentity, error) {
	if err := h.authorized(ctx); err != nil {
		return vfs.ObjectIdentity{}, err
	}
	if !bytes.Equal(s.RecoveryToken, h.intent.Token) {
		return vfs.ObjectIdentity{}, vfs.ErrInvalidSealedObject
	}
	p := h.stage
	var f *os.File
	var err error
	if final {
		p = h.final
		info, e := os.Lstat(p)
		if e != nil {
			return vfs.ObjectIdentity{}, e
		}
		if !info.Mode().IsRegular() {
			return vfs.ObjectIdentity{}, vfs.ErrPublishConflict
		}
		f, err = os.Open(p)
	} else {
		f, err = h.openOwned()
	}
	if err != nil {
		return vfs.ObjectIdentity{}, err
	}
	defer f.Close()
	before, err := f.Stat()
	if err != nil {
		return vfs.ObjectIdentity{}, err
	}
	conflict := vfs.ErrInvalidSealedObject
	if final {
		conflict = vfs.ErrPublishConflict
	}
	if before.Size() != s.Size {
		return vfs.ObjectIdentity{}, conflict
	}
	buf, err := h.bm.Allocate(h.scope, memory.TagExportBuffer, 64<<10)
	if err != nil {
		return vfs.ObjectIdentity{}, err
	}
	defer buf.Release()
	digest := sha256.New()
	var size int64
	for {
		if err := ctx.Err(); err != nil {
			return vfs.ObjectIdentity{}, err
		}
		n, e := f.Read(buf.Bytes())
		if n > 0 {
			size += int64(n)
			digest.Write(buf.Bytes()[:n])
			if size > s.Size {
				return vfs.ObjectIdentity{}, conflict
			}
		}
		if e == io.EOF {
			break
		}
		if e != nil {
			return vfs.ObjectIdentity{}, e
		}
		if n == 0 {
			return vfs.ObjectIdentity{}, io.ErrNoProgress
		}
	}
	after, err := f.Stat()
	if err != nil {
		return vfs.ObjectIdentity{}, err
	}
	current, err := os.Lstat(p)
	if err != nil {
		return vfs.ObjectIdentity{}, err
	}
	if !os.SameFile(before, current) || !os.SameFile(before, after) || !before.ModTime().Equal(after.ModTime()) || after.Size() != size || size != s.Size || hex.EncodeToString(digest.Sum(nil)) != s.Checksum {
		return vfs.ObjectIdentity{}, conflict
	}
	return vfs.ObjectIdentity{Size: size, SnapshotID: "sha256:" + s.Checksum}, nil
}

func (h *localPublishHandle) Publish(ctx context.Context, s vfs.SealedObject) (vfs.ObjectIdentity, error) {
	if !h.legacyWrite {
		if _, err := h.Verify(ctx, s, false); err != nil {
			return vfs.ObjectIdentity{}, err
		}
	} else {
		if err := h.authorized(ctx); err != nil {
			return vfs.ObjectIdentity{}, err
		}
		f, err := h.openOwned()
		if err != nil {
			return vfs.ObjectIdentity{}, err
		}
		_ = f.Close()
	}
	// Keep staging until COMMITTED is durable. Linking provides atomic no-replace.
	publish := publishStagedFile
	if h.legacyReplace {
		publish = publishFile
	}
	if err := publish(h.stage, h.final); err != nil {
		if errors.Is(err, os.ErrExist) {
			return vfs.ObjectIdentity{}, errors.Join(vfs.ErrPublishConflict, err)
		}
		return vfs.ObjectIdentity{}, err
	}
	if err := syncPublishDirectory(filepath.Dir(h.final)); err != nil {
		return vfs.ObjectIdentity{}, err
	}
	return vfs.ObjectIdentity{Size: s.Size, SnapshotID: "sha256:" + s.Checksum}, nil
}

func (h *localPublishHandle) Abort(ctx context.Context, _ *vfs.SealedObject) error {
	if err := h.authorized(ctx); err != nil {
		return err
	}
	if err := h.Release(); err != nil {
		return err
	}
	f, err := h.openOwned()
	if errors.Is(err, os.ErrNotExist) {
		return nil
	}
	if err != nil {
		return err
	}
	_ = f.Close()
	if err := os.Remove(h.stage); err != nil && !errors.Is(err, os.ErrNotExist) {
		return err
	}
	return syncPublishDirectory(filepath.Dir(h.stage))
}

func (h *localPublishHandle) Release() error {
	if h.file == nil {
		return nil
	}
	err := h.file.Close()
	h.file = nil
	return err
}
