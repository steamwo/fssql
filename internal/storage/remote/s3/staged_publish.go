package s3

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/url"
	"strings"
	"sync"

	"github.com/steamwo/fssql/internal/core/memory"
	"github.com/steamwo/fssql/internal/storage/vfs"
)

var _ vfs.StagedPublishDriver = (*Driver)(nil)

type s3PublishState struct {
	Version    int             `json:"version"`
	IntentHash string          `json:"intent_hash"`
	Kind       string          `json:"kind"`
	UploadID   string          `json:"upload_id,omitempty"`
	Parts      []multipartPart `json:"parts,omitempty"`
	Size       int64           `json:"size,omitempty"`
	Checksum   string          `json:"checksum,omitempty"`
}

func (d *Driver) PlanPublish(ctx context.Context, u vfs.URI, job string, mode vfs.PublishMode) (vfs.PublishIntent, error) {
	if err := ctx.Err(); err != nil {
		return vfs.PublishIntent{}, err
	}
	if u.Scheme != "s3" || u.Host == "" || u.ObjectKey() == "" || len(job) != 32 {
		return vfs.PublishIntent{}, vfs.ErrInvalidSealedObject
	}
	for _, c := range job {
		if !(c >= '0' && c <= '9' || c >= 'a' && c <= 'f') {
			return vfs.PublishIntent{}, vfs.ErrInvalidSealedObject
		}
	}
	parsed, err := url.Parse(u.String())
	if err != nil || parsed.User != nil || parsed.RawQuery != "" || parsed.ForceQuery || parsed.Fragment != "" {
		return vfs.PublishIntent{}, vfs.ErrInvalidSealedObject
	}
	if mode == "" {
		mode = vfs.PublishStrict
	}
	if mode != vfs.PublishStrict && mode != vfs.PublishCompatible {
		return vfs.PublishIntent{}, vfs.ErrWriteUnsupported
	}
	// Only digests enter durable state. Length-prefix raw bytes so arbitrary
	// credential bytes cannot collide through JSON's invalid-UTF8 replacement.
	digest := sha256.New()
	for _, s := range []string{d.endpoint.String(), d.cfg.Region, fmt.Sprint(d.cfg.ForcePathStyle), d.cfg.CredentialScope, d.cfg.CredentialVersion, d.cfg.SecurityProfile, d.creds.AccessKey, d.creds.SecretKey, d.creds.SessionToken} {
		fmt.Fprintf(digest, "%d:", len(s))
		_, _ = digest.Write([]byte(s))
	}
	scope := hex.EncodeToString(digest.Sum(nil))
	i := vfs.PublishIntent{JobID: job, Destination: u.String(), Protocol: "s3", Mode: mode, ScopeKey: scope}
	token, _ := json.Marshal(struct {
		Version                   int
		JobID, Destination, Scope string
		Mode                      vfs.PublishMode
	}{1, job, u.String(), scope, mode})
	i.Token = token
	return i, nil
}

func (d *Driver) validatePublishIntent(ctx context.Context, i vfs.PublishIntent) (vfs.URI, error) {
	u, err := vfs.ParseURI(i.Destination)
	if err != nil {
		return vfs.URI{}, err
	}
	expected, err := d.PlanPublish(ctx, u, i.JobID, i.Mode)
	if err != nil {
		return vfs.URI{}, err
	}
	if i.Protocol != expected.Protocol || i.ScopeKey != expected.ScopeKey || !bytes.Equal(i.Token, expected.Token) {
		return vfs.URI{}, vfs.ErrInvalidSealedObject
	}
	return u, nil
}

func publishIntentHash(i vfs.PublishIntent) string {
	sum := sha256.Sum256(i.Token)
	return hex.EncodeToString(sum[:])
}

func (d *Driver) BeginPublish(ctx context.Context, i vfs.PublishIntent, record vfs.RecordOwnership, scope *memory.Scope, bm *memory.BufferManager) (vfs.PublishHandle, error) {
	u, err := d.validatePublishIntent(ctx, i)
	if err != nil {
		return nil, err
	}
	if record == nil {
		return nil, vfs.ErrWriteUnsupported
	}
	writer, err := d.openMultipartWriter(ctx, u, vfs.WriteOptions{IfNoneMatch: true}, scope, bm)
	if err != nil {
		return nil, err
	}
	meta, err := scope.Reserve(memory.TagMetadata, 0)
	if err != nil {
		_ = writer.Abort(ctx)
		return nil, err
	}
	h := &s3PublishHandle{intent: i, writer: writer.(*s3WriteHandle), record: record, metadata: meta}
	ready := false
	defer func() {
		if !ready {
			_ = h.Release()
		}
	}()
	h.writer.stagedRecord = h.recordOwnership
	h.state = s3PublishState{Version: 1, IntentHash: publishIntentHash(i), Kind: "pending"}
	if err := h.recordOwnership(ctx); err != nil {
		_ = h.Release()
		return h, err
	}
	ready = true
	return h, nil
}

func decodePublishState(data []byte) (s3PublishState, error) {
	var s s3PublishState
	if len(data) == 0 || len(data) > vfs.MaxPublishTokenBytes {
		return s, vfs.ErrInvalidSealedObject
	}
	d := json.NewDecoder(bytes.NewReader(data))
	d.DisallowUnknownFields()
	if err := d.Decode(&s); err != nil {
		return s, vfs.ErrInvalidSealedObject
	}
	if err := d.Decode(new(any)); err != io.EOF {
		return s, vfs.ErrInvalidSealedObject
	}
	return s, nil
}

func validPublishChecksum(s string) bool {
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

func validatePublishState(s s3PublishState, i vfs.PublishIntent, sealed bool) error {
	if s.Version != 1 || s.IntentHash != publishIntentHash(i) || len(s.UploadID) > 4096 || len(s.Parts) > maxMultipartParts {
		return vfs.ErrInvalidSealedObject
	}
	switch s.Kind {
	case "pending":
		if sealed || s.UploadID != "" || len(s.Parts) != 0 {
			return vfs.ErrInvalidSealedObject
		}
	case "empty_put":
		if s.UploadID != "" || len(s.Parts) != 0 || sealed && (s.Size != 0 || s.Checksum != emptyPayloadSHA256) {
			return vfs.ErrInvalidSealedObject
		}
	case "multipart":
		if s.UploadID == "" || sealed && len(s.Parts) == 0 {
			return vfs.ErrInvalidSealedObject
		}
	default:
		return vfs.ErrInvalidSealedObject
	}
	var size int64
	for index, p := range s.Parts {
		if p.Number != index+1 || len(p.ETag) == 0 || len(p.ETag) > 256 || strings.ContainsAny(p.ETag, "\r\n") || p.Size <= 0 || p.Size > defaultMultipartPartSize {
			return vfs.ErrInvalidSealedObject
		}
		for _, s := range p.checksums() {
			if len(s) > 128 {
				return vfs.ErrInvalidSealedObject
			}
		}
		if index < len(s.Parts)-1 && p.Size < 5<<20 {
			return vfs.ErrInvalidSealedObject
		}
		size += p.Size
	}
	if sealed && (size != s.Size || !validPublishChecksum(s.Checksum)) {
		return vfs.ErrInvalidSealedObject
	}
	return nil
}

func (d *Driver) ResumePublish(ctx context.Context, i vfs.PublishIntent, ownership []byte, sealed *vfs.SealedObject, scope *memory.Scope, bm *memory.BufferManager) (vfs.PublishHandle, error) {
	u, err := d.validatePublishIntent(ctx, i)
	if err != nil {
		return nil, err
	}
	if scope == nil || bm == nil {
		return nil, vfs.ErrWriteUnsupported
	}
	meta, err := scope.Reserve(memory.TagMetadata, int64(len(ownership))*6+4096)
	if err != nil {
		return nil, err
	}
	keep := false
	defer func() {
		if !keep {
			meta.Release()
		}
	}()
	state, err := decodePublishState(ownership)
	if err != nil {
		return nil, err
	}
	if err := validatePublishState(state, i, false); err != nil {
		return nil, err
	}
	if sealed != nil {
		if err := meta.Resize(int64(len(ownership)+len(sealed.RecoveryToken))*6 + 4096); err != nil {
			return nil, err
		}
		full, err := decodePublishState(sealed.RecoveryToken)
		if err != nil {
			return nil, err
		}
		if err := validatePublishState(full, i, true); err != nil {
			return nil, err
		}
		if full.Size != sealed.Size || full.Checksum != sealed.Checksum {
			return nil, vfs.ErrInvalidSealedObject
		}
		full.Size = 0
		full.Checksum = ""
		canonical, _ := json.Marshal(full)
		own, _ := json.Marshal(state)
		if !bytes.Equal(canonical, own) {
			return nil, vfs.ErrInvalidSealedObject
		}
		state.Size = sealed.Size
		state.Checksum = sealed.Checksum
	}
	w := &s3WriteHandle{driver: d, uri: u, opts: vfs.WriteOptions{IfNoneMatch: true}, scope: scope, bm: bm, uploadID: state.UploadID, parts: state.Parts, total: state.Size, terminal: true}
	keep = true
	return &s3PublishHandle{intent: i, writer: w, state: state, metadata: meta, sealed: sealed != nil}, nil
}

type s3PublishHandle struct {
	mu       sync.Mutex
	intent   vfs.PublishIntent
	writer   *s3WriteHandle
	state    s3PublishState
	record   vfs.RecordOwnership
	metadata *memory.Reservation
	sealed   bool
	// finalIdentity pins the version produced by this Publish when available.
	finalIdentity vfs.ObjectIdentity
}

func (h *s3PublishHandle) PinPublishedIdentity(id vfs.ObjectIdentity) {
	h.mu.Lock()
	defer h.mu.Unlock()
	h.finalIdentity = id
}

func (h *s3PublishHandle) recordOwnership(ctx context.Context) error {
	h.state.UploadID = h.writer.uploadID
	h.state.Parts = h.writer.parts
	if h.writer.uploadID != "" {
		h.state.Kind = "multipart"
	}
	payload, err := h.encodeState(h.state)
	if err != nil {
		return err
	}
	return h.record(ctx, payload)
}

func (h *s3PublishHandle) encodeState(s s3PublishState) ([]byte, error) {
	estimate := int64(8192 + len(s.UploadID)*6)
	for _, p := range s.Parts {
		estimate += int64(1024 + len(p.ETag)*6)
		for _, checksum := range p.checksums() {
			estimate += int64(len(checksum)) * 6
		}
	}
	if err := h.metadata.Resize(estimate * 2); err != nil {
		return nil, err
	}
	p, err := json.Marshal(s)
	if err != nil {
		return nil, err
	}
	if len(p) > vfs.MaxPublishTokenBytes {
		return nil, vfs.ErrInvalidSealedObject
	}
	return p, nil
}

func (h *s3PublishHandle) Write(ctx context.Context, p []byte) (int, error) {
	h.mu.Lock()
	defer h.mu.Unlock()
	if h.sealed {
		return 0, vfs.ErrWriteClosed
	}
	return h.writer.writePayload(ctx, p)
}

func (h *s3PublishHandle) Seal(ctx context.Context, e vfs.WriteEvidence) (vfs.SealedObject, error) {
	h.mu.Lock()
	defer h.mu.Unlock()
	w := h.writer
	w.mu.Lock()
	defer w.mu.Unlock()
	if err := ctx.Err(); err != nil {
		return vfs.SealedObject{}, err
	}
	if h.sealed || w.terminal || w.failed != nil || w.total != e.Size || !validPublishChecksum(e.Checksum) {
		return vfs.SealedObject{}, errors.Join(vfs.ErrInvalidSealedObject, w.failed)
	}
	if w.used > 0 {
		if err := w.uploadCurrentPartLocked(ctx); err != nil {
			w.failLocked(err)
			return vfs.SealedObject{}, err
		}
	}
	if w.total == 0 {
		if e.Checksum != emptyPayloadSHA256 {
			return vfs.SealedObject{}, vfs.ErrInvalidSealedObject
		}
		h.state.Kind = "empty_put"
		if err := h.recordOwnership(ctx); err != nil {
			return vfs.SealedObject{}, err
		}
	}
	h.state.Size = e.Size
	h.state.Checksum = e.Checksum
	if err := validatePublishState(h.state, h.intent, true); err != nil {
		return vfs.SealedObject{}, err
	}
	token, err := h.encodeState(h.state)
	if err != nil {
		return vfs.SealedObject{}, err
	}
	h.sealed = true
	w.terminal = true
	if w.partBuffer != nil {
		w.partBuffer.Release()
		w.partBuffer = nil
	}
	return vfs.SealedObject{Size: e.Size, Checksum: e.Checksum, RecoveryToken: token}, nil
}

func (h *s3PublishHandle) validateSealed(s vfs.SealedObject) error {
	if !h.sealed || s.Size != h.state.Size || s.Checksum != h.state.Checksum {
		return vfs.ErrInvalidSealedObject
	}
	state, err := decodePublishState(s.RecoveryToken)
	if err != nil {
		return err
	}
	if err := validatePublishState(state, h.intent, true); err != nil {
		return err
	}
	a, _ := json.Marshal(state)
	b, _ := json.Marshal(h.state)
	if !bytes.Equal(a, b) {
		return vfs.ErrInvalidSealedObject
	}
	return nil
}

func (h *s3PublishHandle) Release() error {
	h.mu.Lock()
	defer h.mu.Unlock()
	h.writer.mu.Lock()
	defer h.writer.mu.Unlock()
	h.writer.terminal = true
	h.writer.releaseLocked()
	h.state.Parts = nil
	if h.metadata != nil {
		h.metadata.Release()
		h.metadata = nil
	}
	return nil
}
