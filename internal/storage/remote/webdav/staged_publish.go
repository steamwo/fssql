package webdav

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"strings"

	"github.com/steamwo/fssql/internal/core/memory"
	"github.com/steamwo/fssql/internal/storage/remote"
	"github.com/steamwo/fssql/internal/storage/vfs"
)

func (d *Driver) publisher(ctx context.Context, mode vfs.PublishMode) (remote.FilePublisher, error) {
	if mode == vfs.PublishStrict && !d.AtomicMoveVerified {
		return remote.FilePublisher{}, fmt.Errorf("%w: WebDAV strict requires AtomicMoveVerified", vfs.ErrWriteUnsupported)
	}
	target, err := d.targetURL(vfs.URI{Scheme: d.SchemeName, Host: d.Key.Endpoint, Path: "/"})
	if err != nil {
		return remote.FilePublisher{}, err
	}
	if _, err = d.Policy.ValidateURL(ctx, target.String()); err != nil {
		return remote.FilePublisher{}, err
	}
	credentials, err := d.Credentials.Credentials(ctx, d.Key, d.SchemeName)
	if err != nil {
		return remote.FilePublisher{}, err
	}
	key := d.Key
	key.CredentialVersion = remote.PublishScope(key.CredentialVersion, credentials.Username, credentials.Password)
	guard := &publishLock{driver: d}
	p := remote.FilePublisher{Key: key, Protocol: d.SchemeName, Evidence: fmt.Sprintf("LOCK-or-ETag:v2;MOVE Overwrite:F;AtomicMoveVerified=%t", d.AtomicMoveVerified)}
	p.OwnershipProof = func() string { return guard.proof }
	p.RestoreProof = guard.restore
	p.IdentityMatches = publishIdentityMatches
	p.Stat = func(ctx context.Context, u vfs.URI) (vfs.ObjectInfo, error) {
		condition, err := guard.condition(ctx, u)
		if err != nil {
			return vfs.ObjectInfo{}, err
		}
		return d.publishStatIf(ctx, u, condition)
	}
	p.Create = func(ctx context.Context, u vfs.URI) (remote.FileUpload, vfs.ObjectIdentity, error) {
		locked, err := guard.create(ctx, u)
		if err != nil {
			return nil, vfs.ObjectIdentity{}, err
		}
		if locked {
			condition, err := guard.condition(ctx, u)
			if err != nil {
				return nil, vfs.ObjectIdentity{}, err
			}
			h, err := d.openPUT(ctx, u, u, http.Header{"If": []string{condition}}, true)
			if err != nil {
				return nil, vfs.ObjectIdentity{}, err
			}
			return &publishUpload{h: h}, vfs.ObjectIdentity{}, nil
		}
		// Acquire an exclusive name before accepting any payload. A lost reply
		// leaves pending ownership, which never authorizes deletion.
		r, err := d.newRequest(ctx, http.MethodPut, u, http.NoBody)
		if err != nil {
			return nil, vfs.ObjectIdentity{}, err
		}
		r.Header.Set("If-None-Match", "*")
		resp, err := d.doPublish(r)
		if err != nil {
			return nil, vfs.ObjectIdentity{}, err
		}
		_ = resp.Body.Close()
		if resp.StatusCode < 200 || resp.StatusCode >= 300 {
			return nil, vfs.ObjectIdentity{}, statusError("PUT exclusive staging", resp.StatusCode)
		}
		id := vfs.ObjectIdentity{ETag: resp.Header.Get("ETag")}
		// If-Match guards the staging upload against replacement. Weak or
		// missing validators cannot establish this exclusive-create contract.
		if id.ETag == "" || len(id.ETag) > 4096 || strings.HasPrefix(id.ETag, "W/") {
			return nil, id, fmt.Errorf("%w: WebDAV exclusive staging PUT must return a strong ETag", vfs.ErrInvalidSealedObject)
		}
		guard.proof = "etag"
		h, err := d.openPUT(ctx, u, u, http.Header{"If-Match": []string{id.ETag}}, true)
		if err != nil {
			return nil, id, err
		}
		return &publishUpload{h: h}, id, nil
	}
	p.Read = func(ctx context.Context, u vfs.URI, id vfs.ObjectIdentity, w io.Writer, b []byte) error {
		r, err := d.newRequest(ctx, http.MethodGet, u, nil)
		if err != nil {
			return err
		}
		r.Header.Set("Accept-Encoding", "identity")
		condition, err := guard.condition(ctx, u)
		if err != nil {
			return err
		}
		if condition != "" {
			r.Header.Set("If", condition)
		}
		if id.ETag != "" && !strings.HasPrefix(id.ETag, "W/") {
			r.Header.Set("If-Match", id.ETag)
		}
		resp, err := d.doPublish(r)
		if err != nil {
			return err
		}
		defer resp.Body.Close()
		if resp.StatusCode == http.StatusPreconditionFailed {
			return vfs.ErrObjectChanged
		}
		if resp.StatusCode != http.StatusOK {
			return statusError("GET verification", resp.StatusCode)
		}
		if id.ETag != "" && strings.TrimPrefix(resp.Header.Get("ETag"), "W/") != strings.TrimPrefix(id.ETag, "W/") {
			return vfs.ErrObjectChanged
		}
		return remote.CopyPhysical(ctx, resp.Body, w, b)
	}
	p.Move = func(ctx context.Context, source, dest vfs.URI, _ vfs.PublishMode, id vfs.ObjectIdentity) error {
		r, err := d.newRequest(ctx, "MOVE", source, nil)
		if err != nil {
			return err
		}
		target, err := d.targetURL(dest)
		if err != nil {
			return err
		}
		r.Header.Set("Destination", target.String())
		r.Header.Set("Overwrite", "F")
		condition, err := guard.condition(ctx, source)
		if err != nil {
			return err
		}
		if condition != "" {
			r.Header.Set("If", condition)
		}
		if condition == "" && id.ETag != "" && !strings.HasPrefix(id.ETag, "W/") {
			r.Header.Set("If-Match", id.ETag)
		}
		resp, err := d.doPublish(r)
		if err != nil {
			return err
		}
		defer resp.Body.Close()
		if resp.StatusCode == http.StatusPreconditionFailed {
			return vfs.ErrPublishConflict
		}
		if resp.StatusCode == http.StatusCreated || resp.StatusCode == http.StatusNoContent || resp.StatusCode == http.StatusOK {
			return nil
		}
		if resp.StatusCode >= 400 && resp.StatusCode < 500 {
			return errors.Join(vfs.ErrPublishFailed, statusError("MOVE", resp.StatusCode))
		}
		return statusError("MOVE", resp.StatusCode)
	}
	p.Delete = func(ctx context.Context, u vfs.URI, id vfs.ObjectIdentity) error {
		condition, err := guard.condition(ctx, u)
		if err != nil {
			return err
		}
		if condition == "" && (id.ETag == "" || strings.HasPrefix(id.ETag, "W/")) {
			return vfs.ErrInvalidSealedObject
		}
		r, err := d.newRequest(ctx, http.MethodDelete, u, nil)
		if err != nil {
			return err
		}
		if condition != "" {
			r.Header.Set("If", condition)
		} else {
			r.Header.Set("If-Match", id.ETag)
		}
		resp, err := d.doPublish(r)
		if err != nil {
			return err
		}
		defer resp.Body.Close()
		if resp.StatusCode == http.StatusNotFound || resp.StatusCode >= 200 && resp.StatusCode < 300 {
			return nil
		}
		return statusError("DELETE staging", resp.StatusCode)
	}
	return p, nil
}

// Apache may promote a fresh weak validator to a strong one without changing
// its opaque tag or bytes. This is metadata equivalence only: lock conditions
// protect staging and every verification still hashes the complete payload.
func publishIdentityMatches(a, b vfs.ObjectIdentity) bool {
	a.ETag = strings.TrimPrefix(a.ETag, "W/")
	b.ETag = strings.TrimPrefix(b.ETag, "W/")
	return a.Matches(b)
}

func (d *Driver) doPublish(r *http.Request) (*http.Response, error) {
	client, err := d.Clients.Client(d.Key)
	if err != nil {
		return nil, err
	}
	copyClient := *client
	copyClient.CheckRedirect = func(*http.Request, []*http.Request) error { return http.ErrUseLastResponse }
	return copyClient.Do(r)
}

func (d *Driver) publishStat(ctx context.Context, u vfs.URI) (vfs.ObjectInfo, error) {
	return d.publishStatIf(ctx, u, "")
}

func (d *Driver) publishStatIf(ctx context.Context, u vfs.URI, condition string) (vfs.ObjectInfo, error) {
	r, err := d.newRequest(ctx, http.MethodHead, u, nil)
	if err != nil {
		return vfs.ObjectInfo{}, err
	}
	r.Header.Set("Accept-Encoding", "identity")
	if condition != "" {
		r.Header.Set("If", condition)
	}
	resp, err := d.doPublish(r)
	if err != nil {
		return vfs.ObjectInfo{}, err
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		if resp.StatusCode == http.StatusPreconditionFailed {
			return vfs.ObjectInfo{}, vfs.ErrInvalidSealedObject
		}
		return vfs.ObjectInfo{}, statusError("HEAD verification", resp.StatusCode)
	}
	if resp.ContentLength < 0 {
		return vfs.ObjectInfo{}, vfs.ErrInvalidSealedObject
	}
	id, err := identityFromResponse(resp)
	return vfs.ObjectInfo{URI: u, Size: id.Size, Identity: id}, err
}

func (d *Driver) PlanPublish(ctx context.Context, u vfs.URI, job string, mode vfs.PublishMode) (vfs.PublishIntent, error) {
	if err := d.validateURI(u); err != nil {
		return vfs.PublishIntent{}, err
	}
	p, e := d.publisher(ctx, mode)
	if e != nil {
		return vfs.PublishIntent{}, e
	}
	return p.Plan(u, job, mode)
}
func (d *Driver) BeginPublish(ctx context.Context, i vfs.PublishIntent, r vfs.RecordOwnership, s *memory.Scope, b *memory.BufferManager) (vfs.PublishHandle, error) {
	p, e := d.publisher(ctx, i.Mode)
	if e != nil {
		return nil, e
	}
	return p.Begin(ctx, i, r, s, b)
}
func (d *Driver) ResumePublish(ctx context.Context, i vfs.PublishIntent, o []byte, z *vfs.SealedObject, s *memory.Scope, b *memory.BufferManager) (vfs.PublishHandle, error) {
	p, e := d.publisher(ctx, i.Mode)
	if e != nil {
		return nil, e
	}
	// Existing v1 file intents retain their original strong-ETag protocol.
	// FilePublisher still recomputes and validates the entire scoped intent.
	var legacy struct {
		Evidence string `json:"protocol_evidence"`
	}
	if json.Unmarshal(i.Token, &legacy) == nil && legacy.Evidence == fmt.Sprintf("MOVE Overwrite:F;AtomicMoveVerified=%t", d.AtomicMoveVerified) {
		p.Evidence = legacy.Evidence
		p.RestoreProof = nil
	}
	return p.Resume(ctx, i, o, z, s, b)
}

type publishUpload struct{ h *writeHandle }

func (u *publishUpload) Write(ctx context.Context, b []byte) (int, error) { return u.h.Write(ctx, b) }
func (u *publishUpload) Close(ctx context.Context) error                  { _, err := u.h.Close(ctx); return err }
func (u *publishUpload) Release() error                                   { return u.h.Release() }
