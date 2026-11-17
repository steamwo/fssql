package remote

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"hash"
	"io"
	"os"
	"path"
	"strings"
	"sync"

	"github.com/steamwo/fssql/internal/core/memory"
	"github.com/steamwo/fssql/internal/storage/vfs"
)

// FileUpload only finishes the private staging stream. Release never deletes it.
type FileUpload interface {
	Write(context.Context, []byte) (int, error)
	Close(context.Context) error
	Release() error
}

// FilePublisher shares the file-based protocols' ownership and physical-byte
// checks. Protocol adapters retain control of creation and publication semantics.
type FilePublisher struct {
	Key                ClientKey
	Protocol, Evidence string
	Create             func(context.Context, vfs.URI) (FileUpload, vfs.ObjectIdentity, error)
	Stat               func(context.Context, vfs.URI) (vfs.ObjectInfo, error)
	Read               func(context.Context, vfs.URI, vfs.ObjectIdentity, io.Writer, []byte) error
	Move               func(context.Context, vfs.URI, vfs.URI, vfs.PublishMode, vfs.ObjectIdentity) error
	Delete             func(context.Context, vfs.URI, vfs.ObjectIdentity) error
	// Optional protocol ownership proof. It must contain no bearer token or
	// credentials. RestoreProof binds it to the validated staging path.
	OwnershipProof func() string
	RestoreProof   func(vfs.URI, string) error
	// Optional metadata equivalence, in addition to full physical SHA-256.
	IdentityMatches func(vfs.ObjectIdentity, vfs.ObjectIdentity) bool
}

// PublishScope hashes length-delimited configuration; credentials never enter tokens.
func PublishScope(values ...string) string {
	b, _ := json.Marshal(values)
	h := sha256.Sum256(b)
	return hex.EncodeToString(h[:])
}

type fileIntent struct {
	Version  int             `json:"version"`
	Staging  string          `json:"staging"`
	Evidence string          `json:"protocol_evidence"`
	Mode     vfs.PublishMode `json:"mode"`
}

func (p FilePublisher) Plan(u vfs.URI, job string, mode vfs.PublishMode) (vfs.PublishIntent, error) {
	if len(job) != 32 || strings.Trim(job, "0123456789abcdef") != "" ||
		(mode != vfs.PublishStrict && mode != vfs.PublishCompatible) ||
		u.Scheme != p.Protocol || u.Host != p.Key.Endpoint || u.Path == "/" ||
		u.Path != path.Clean(u.Path) || !strings.HasPrefix(u.Path, "/") ||
		strings.ContainsAny(u.Path, "\r\n\x00") || u.HasGlob() || len(u.String()) > 8192 {
		return vfs.PublishIntent{}, vfs.ErrInvalidSealedObject
	}
	stage := u
	stage.Path = path.Join(path.Dir(u.Path), ".fssql-"+job+".staging")
	if stage.Path == u.Path {
		return vfs.PublishIntent{}, vfs.ErrInvalidSealedObject
	}
	token, _ := json.Marshal(fileIntent{1, stage.String(), p.Evidence, mode})
	return vfs.PublishIntent{JobID: job, Destination: u.String(), Protocol: p.Protocol, Mode: mode,
		ScopeKey: PublishScope(p.Protocol, p.Key.Endpoint, p.Key.CredentialScope, p.Key.CredentialVersion, p.Key.SecurityProfile, p.Evidence), Token: token}, nil
}

func (p FilePublisher) validate(i vfs.PublishIntent) (vfs.URI, error) {
	u, err := vfs.ParseURI(i.Destination)
	if err != nil {
		return vfs.URI{}, vfs.ErrInvalidSealedObject
	}
	want, err := p.Plan(u, i.JobID, i.Mode)
	if err != nil || want.Destination != i.Destination || want.Protocol != i.Protocol || want.ScopeKey != i.ScopeKey || !bytes.Equal(want.Token, i.Token) {
		return vfs.URI{}, vfs.ErrInvalidSealedObject
	}
	var t fileIntent
	_ = json.Unmarshal(want.Token, &t)
	return vfs.ParseURI(t.Staging)
}

type fileOwnership struct {
	Version  int                `json:"version"`
	Intent   string             `json:"intent"`
	Owned    bool               `json:"owned"`
	Identity vfs.ObjectIdentity `json:"identity"`
	Proof    string             `json:"proof,omitempty"`
}

type fileSealed struct {
	Ownership fileOwnership `json:"ownership"`
	Size      int64         `json:"size"`
	Checksum  string        `json:"checksum"`
}

type filePublishHandle struct {
	mu           sync.Mutex
	p            FilePublisher
	intent       vfs.PublishIntent
	stage, final vfs.URI
	own          fileOwnership
	upload       FileUpload
	record       vfs.RecordOwnership
	scope        *memory.Scope
	bm           *memory.BufferManager
	metadata     *memory.Reservation
	digest       hash.Hash
	written      int64
	sealed       *vfs.SealedObject
	pinned       *vfs.ObjectIdentity
	released     bool
	failed       error
}

func (p FilePublisher) handle(i vfs.PublishIntent, scope *memory.Scope, bm *memory.BufferManager) (*filePublishHandle, error) {
	stage, err := p.validate(i)
	if err != nil {
		return nil, err
	}
	if scope == nil || bm == nil {
		return nil, errors.New("remote: managed publish memory required")
	}
	// Covers bounded wire packets, control metadata and token serialization in
	// addition to the separately managed 64 KiB readback buffer.
	r, err := scope.Reserve(memory.TagNetworkBuffer, 512<<10)
	if err != nil {
		return nil, err
	}
	final, _ := vfs.ParseURI(i.Destination)
	return &filePublishHandle{p: p, intent: i, stage: stage, final: final, scope: scope, bm: bm, metadata: r, digest: sha256.New(), own: fileOwnership{Version: 1, Intent: PublishScope(i.JobID, i.Destination, i.ScopeKey, string(i.Token))}}, nil
}

func (p FilePublisher) Begin(ctx context.Context, i vfs.PublishIntent, record vfs.RecordOwnership, scope *memory.Scope, bm *memory.BufferManager) (vfs.PublishHandle, error) {
	h, err := p.handle(i, scope, bm)
	if err != nil {
		return nil, err
	}
	h.record = record
	if record == nil {
		_ = h.Release()
		return nil, vfs.ErrInvalidSealedObject
	}
	if err = h.saveOwnership(ctx); err != nil {
		_ = h.Release()
		return nil, err
	}
	h.upload, h.own.Identity, err = p.Create(ctx, h.stage)
	if err != nil {
		h.failed = err
		return h, err
	} // pending ownership cannot authorize deletion
	h.own.Owned = true
	if p.OwnershipProof != nil {
		h.own.Proof = p.OwnershipProof()
	}
	if err = h.saveOwnership(ctx); err != nil {
		h.failed = err
		return h, err
	}
	return h, nil
}

func (h *filePublishHandle) saveOwnership(ctx context.Context) error {
	b, err := json.Marshal(h.own)
	if err != nil {
		return err
	}
	return h.record(ctx, b)
}

func decodeFileToken(b []byte, out any) error {
	if len(b) == 0 || len(b) > 32<<10 {
		return vfs.ErrInvalidSealedObject
	}
	d := json.NewDecoder(bytes.NewReader(b))
	d.DisallowUnknownFields()
	if d.Decode(out) != nil {
		return vfs.ErrInvalidSealedObject
	}
	var trailing any
	if d.Decode(&trailing) != io.EOF {
		return vfs.ErrInvalidSealedObject
	}
	return nil
}

func (p FilePublisher) Resume(ctx context.Context, i vfs.PublishIntent, b []byte, s *vfs.SealedObject, scope *memory.Scope, bm *memory.BufferManager) (vfs.PublishHandle, error) {
	h, err := p.handle(i, scope, bm)
	if err != nil {
		return nil, err
	}
	var own fileOwnership
	if len(b) > 0 {
		if err = decodeFileToken(b, &own); err != nil || own.Version != 1 || own.Intent != h.own.Intent {
			_ = h.Release()
			return nil, vfs.ErrInvalidSealedObject
		}
		h.own = own
	}
	if h.own.Owned && p.RestoreProof != nil {
		if err = p.RestoreProof(h.stage, h.own.Proof); err != nil {
			_ = h.Release()
			return nil, err
		}
	} else if h.own.Proof != "" {
		_ = h.Release()
		return nil, vfs.ErrInvalidSealedObject
	}
	if s != nil {
		var t fileSealed
		if decodeFileToken(s.RecoveryToken, &t) != nil || !h.own.Owned || t.Ownership != h.own || t.Size != s.Size || t.Checksum != s.Checksum || !validFileChecksum(s.Checksum) || s.Size < 0 {
			_ = h.Release()
			return nil, vfs.ErrInvalidSealedObject
		}
		copyS := *s
		copyS.RecoveryToken = append([]byte(nil), s.RecoveryToken...)
		h.sealed = &copyS
	}
	if err = ctx.Err(); err != nil {
		_ = h.Release()
		return nil, err
	}
	return h, nil
}

func validFileChecksum(s string) bool {
	return len(s) == 64 && strings.Trim(s, "0123456789abcdef") == ""
}

func (p FilePublisher) matches(a, b vfs.ObjectIdentity) bool {
	if p.IdentityMatches != nil {
		return p.IdentityMatches(a, b)
	}
	return a.Matches(b)
}

func (h *filePublishHandle) Write(ctx context.Context, b []byte) (int, error) {
	h.mu.Lock()
	defer h.mu.Unlock()
	if h.released || h.upload == nil || h.sealed != nil {
		return 0, vfs.ErrWriteClosed
	}
	if h.failed != nil {
		return 0, h.failed
	}
	n, err := h.upload.Write(ctx, b)
	if n < 0 || n > len(b) {
		h.failed = io.ErrShortWrite
		return 0, io.ErrShortWrite
	}
	if n > 0 {
		_, _ = h.digest.Write(b[:n])
		h.written += int64(n)
	}
	if err == nil && n != len(b) {
		err = io.ErrShortWrite
	}
	if err != nil {
		h.failed = err
	}
	return n, err
}

func (h *filePublishHandle) Seal(ctx context.Context, e vfs.WriteEvidence) (vfs.SealedObject, error) {
	h.mu.Lock()
	defer h.mu.Unlock()
	if h.failed != nil {
		return vfs.SealedObject{}, h.failed
	}
	if h.released || h.upload == nil || h.sealed != nil || e.Size != h.written || e.Checksum != hex.EncodeToString(h.digest.Sum(nil)) {
		return vfs.SealedObject{}, vfs.ErrInvalidSealedObject
	}
	if err := h.upload.Close(ctx); err != nil {
		return vfs.SealedObject{}, err
	}
	_ = h.upload.Release()
	h.upload = nil
	s := vfs.SealedObject{Size: e.Size, Checksum: e.Checksum}
	// Replace the creation identity with the fully uploaded physical identity.
	info, err := h.p.Stat(ctx, h.stage)
	if err != nil {
		return s, err
	}
	h.own.Identity = info.Identity
	if err = h.saveOwnership(ctx); err != nil {
		return s, err
	}
	s.RecoveryToken, _ = json.Marshal(fileSealed{h.own, e.Size, e.Checksum})
	h.sealed = &s
	if _, err = h.verify(ctx, s, false); err != nil {
		return vfs.SealedObject{}, err
	}
	return s, nil
}

func (h *filePublishHandle) validSealed(s vfs.SealedObject) bool {
	return !h.released && h.sealed != nil && h.own.Owned && s.Size == h.sealed.Size && s.Checksum == h.sealed.Checksum && bytes.Equal(s.RecoveryToken, h.sealed.RecoveryToken)
}

func (h *filePublishHandle) Publish(ctx context.Context, s vfs.SealedObject) (vfs.ObjectIdentity, error) {
	h.mu.Lock()
	defer h.mu.Unlock()
	if !h.validSealed(s) {
		return vfs.ObjectIdentity{}, vfs.ErrInvalidSealedObject
	}
	if _, err := h.verify(ctx, s, false); err != nil {
		return vfs.ObjectIdentity{}, err
	}
	if err := h.p.Move(ctx, h.stage, h.final, h.intent.Mode, h.own.Identity); err != nil {
		return vfs.ObjectIdentity{}, err
	}
	// The final Stat/readback belongs to Verify: a failed response here cannot
	// turn a successful rename into an ordinary pre-publication failure.
	return vfs.ObjectIdentity{}, nil
}

func (h *filePublishHandle) PinPublishedIdentity(i vfs.ObjectIdentity) {
	h.mu.Lock()
	defer h.mu.Unlock()
	h.pinned = &i
}
func (h *filePublishHandle) Verify(ctx context.Context, s vfs.SealedObject, final bool) (vfs.ObjectIdentity, error) {
	h.mu.Lock()
	defer h.mu.Unlock()
	return h.verify(ctx, s, final)
}
func (h *filePublishHandle) verify(ctx context.Context, s vfs.SealedObject, final bool) (vfs.ObjectIdentity, error) {
	if !h.validSealed(s) {
		return vfs.ObjectIdentity{}, vfs.ErrInvalidSealedObject
	}
	u := h.stage
	mismatch := vfs.ErrInvalidSealedObject
	if final {
		u = h.final
		mismatch = vfs.ErrPublishConflict
	}
	info, err := h.p.Stat(ctx, u)
	if err != nil {
		if !final && errors.Is(err, os.ErrNotExist) {
			return vfs.ObjectIdentity{}, vfs.ErrInvalidSealedObject
		}
		return vfs.ObjectIdentity{}, err
	}
	if info.Size != s.Size || (!final && !h.p.matches(info.Identity, h.own.Identity)) || (final && h.pinned != nil && !h.p.matches(info.Identity, *h.pinned)) {
		return vfs.ObjectIdentity{}, mismatch
	}
	b, err := h.bm.Allocate(h.scope, memory.TagNetworkBuffer, 64<<10)
	if err != nil {
		return vfs.ObjectIdentity{}, err
	}
	defer b.Release()
	digest := sha256.New()
	w := &boundedDigest{hash: digest, max: s.Size}
	if err = h.p.Read(ctx, u, info.Identity, w, b.Bytes()); err != nil {
		if errors.Is(err, vfs.ErrObjectChanged) {
			return vfs.ObjectIdentity{}, mismatch
		}
		return vfs.ObjectIdentity{}, err
	}
	if w.n != s.Size || hex.EncodeToString(digest.Sum(nil)) != s.Checksum {
		return vfs.ObjectIdentity{}, mismatch
	}
	after, err := h.p.Stat(ctx, u)
	if err != nil {
		return vfs.ObjectIdentity{}, err
	}
	if after.Size != info.Size || !h.p.matches(after.Identity, info.Identity) {
		return vfs.ObjectIdentity{}, mismatch
	}
	return info.Identity, nil
}

type boundedDigest struct {
	hash   hash.Hash
	n, max int64
}

func (w *boundedDigest) Write(b []byte) (int, error) {
	if int64(len(b)) > w.max-w.n {
		return 0, vfs.ErrObjectChanged
	}
	n, e := w.hash.Write(b)
	w.n += int64(n)
	return n, e
}

func (h *filePublishHandle) Abort(ctx context.Context, _ *vfs.SealedObject) error {
	h.mu.Lock()
	defer h.mu.Unlock()
	if h.upload != nil {
		_ = h.upload.Release()
		h.upload = nil
	}
	if !h.own.Owned {
		return vfs.ErrInvalidSealedObject
	}
	info, err := h.p.Stat(ctx, h.stage)
	if errors.Is(err, os.ErrNotExist) {
		return nil
	}
	if err != nil {
		return err
	}
	// Unsealed streams may have changed their initial size/mtime. Their unique,
	// successfully created name proves ownership; sealed files must still match.
	if h.sealed != nil && !h.p.matches(info.Identity, h.own.Identity) {
		return vfs.ErrInvalidSealedObject
	}
	return h.p.Delete(ctx, h.stage, info.Identity)
}

func (h *filePublishHandle) Release() error {
	h.mu.Lock()
	defer h.mu.Unlock()
	if h.released {
		return nil
	}
	h.released = true
	var err error
	if h.upload != nil {
		err = h.upload.Release()
		h.upload = nil
	}
	h.metadata.Release()
	return err
}

// CopyPhysical uses the supplied managed buffer and checks cancellation even
// for local/mock streams. It avoids io.Copy's optional unaccounted fast paths.
func CopyPhysical(ctx context.Context, r io.Reader, w io.Writer, b []byte) error {
	for {
		if err := ctx.Err(); err != nil {
			return err
		}
		n, err := r.Read(b)
		if n > 0 {
			written, e := w.Write(b[:n])
			if e != nil {
				return e
			}
			if written != n {
				return io.ErrShortWrite
			}
		}
		if ctx.Err() != nil {
			return ctx.Err()
		}
		if err == io.EOF {
			return nil
		}
		if err != nil {
			return err
		}
		if n == 0 {
			return io.ErrNoProgress
		}
	}
}
