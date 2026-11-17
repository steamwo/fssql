package sftp

import (
	"context"
	"errors"
	"fmt"
	"io"
	"path"
	"sort"
	"strconv"
	"strings"
	"time"

	"github.com/steamwo/fssql/internal/core/memory"
	"github.com/steamwo/fssql/internal/storage/remote"
	"github.com/steamwo/fssql/internal/storage/vfs"
)

type Driver struct {
	Key         remote.ClientKey
	Pools       *Manager
	MaxAttempts int
}

func NewDriver(key remote.ClientKey, pools *Manager, maxAttempts int) (*Driver, error) {
	if err := key.Validate(); err != nil {
		return nil, err
	}
	if pools == nil {
		return nil, errors.New("sftp: pool manager required")
	}
	if maxAttempts <= 0 {
		maxAttempts = 2
	}
	return &Driver{Key: key, Pools: pools, MaxAttempts: maxAttempts}, nil
}
func (d *Driver) Scheme() string { return "sftp" }

// StaticCapabilities 只声明驱动已真正实现的能力；SFTP v3 流式写由 WritableSession/WriteHandle 提供，
// 但 ConditionalWrite/AtomicRename 仍未实现，因此不得在这里虚假宣称。
func (d *Driver) StaticCapabilities() vfs.CapabilitySet {
	// NoReplacePublish is an upper bound; Probe requires the negotiated hardlink
	// extension. It does not imply the legacy AtomicPublishWrite interface.
	caps := vfs.Capabilities(vfs.RandomRead, vfs.StreamingRead, vfs.StreamingWrite, vfs.ServerSideList, vfs.NoReplacePublish)
	if d.Pools != nil && d.Pools.MaxSessions() > 1 {
		caps = caps.With(vfs.ParallelRandomRead)
	}
	return caps
}
func (d *Driver) Probe(ctx context.Context) (vfs.ProbeResult, error) {
	p, err := d.Pools.Pool(d.Key)
	if err != nil {
		return vfs.ProbeResult{}, err
	}
	s, err := p.Acquire(ctx)
	if err != nil {
		return vfs.ProbeResult{}, err
	}
	supported := d.StaticCapabilities().Without(vfs.NoReplacePublish)
	if publisher, ok := s.(PublishSession); ok {
		version, link := publisher.PublishExtensions()
		if version == 3 && link == "1" {
			supported = supported.With(vfs.NoReplacePublish)
		}
	}
	p.Release(s, false)
	return vfs.ProbeResult{Known: d.StaticCapabilities(), Supported: supported, Endpoint: d.Key.Endpoint, CredentialScope: d.Key.CredentialScope, CredentialVersion: d.Key.CredentialVersion, SecurityProfile: d.Key.SecurityProfile}, nil
}
func infoIdentity(info FileInfo) vfs.ObjectIdentity {
	return vfs.ObjectIdentity{Size: info.Size, ModifiedAt: time.Unix(info.ModUnix, 0).UTC()}
}
func (d *Driver) validateURI(u vfs.URI) error {
	if strings.ToLower(u.Scheme) != "sftp" {
		return vfs.ErrUnsupportedScheme
	}
	if u.Host != d.Key.Endpoint {
		return fmt.Errorf("sftp: endpoint mismatch: uri=%s pool=%s", u.Host, d.Key.Endpoint)
	}
	return nil
}

func uriPath(u vfs.URI) string {
	p := u.Path
	if p == "" {
		p = "/"
	}
	return path.Clean(p)
}
func (d *Driver) pool() (*Pool, error) { return d.Pools.Pool(d.Key) }
func (d *Driver) withSession(ctx context.Context, fn func(Session) error) error {
	p, err := d.pool()
	if err != nil {
		return err
	}
	var last error
	for attempt := 0; attempt < d.MaxAttempts; attempt++ {
		if err := ctx.Err(); err != nil {
			return err
		}
		s, err := p.Acquire(ctx)
		if err != nil {
			if errors.Is(err, ErrHostKey) || errors.Is(err, context.Canceled) || errors.Is(err, context.DeadlineExceeded) {
				return err
			}
			last = err
			if !IsRetryable(err) {
				return err
			}
			continue
		}
		err = fn(s)
		poison := IsRetryable(err) || errors.Is(err, context.Canceled) || errors.Is(err, context.DeadlineExceeded)
		p.Release(s, poison)
		if err == nil {
			return nil
		}
		if errors.Is(err, ErrHostKey) || errors.Is(err, context.Canceled) || errors.Is(err, context.DeadlineExceeded) {
			return err
		}
		last = err
		if !IsRetryable(err) {
			return err
		}
	}
	return last
}
func (d *Driver) Stat(ctx context.Context, u vfs.URI) (out vfs.ObjectInfo, err error) {
	if err := d.validateURI(u); err != nil {
		return out, err
	}
	var fi FileInfo
	err = d.withSession(ctx, func(s Session) error {
		v, e := s.Stat(ctx, uriPath(u))
		if e == nil {
			fi = v
		}
		return e
	})
	if err != nil {
		return out, err
	}
	return vfs.ObjectInfo{URI: u, Size: fi.Size, Identity: infoIdentity(fi)}, nil
}
func (d *Driver) List(ctx context.Context, u vfs.URI, opts vfs.ListOptions) (vfs.ListPage, error) {
	if err := d.validateURI(u); err != nil {
		return vfs.ListPage{}, err
	}
	prefix := strings.TrimPrefix(opts.Prefix, "/")
	dir := "/"
	basePrefix := prefix
	if slash := strings.LastIndex(prefix, "/"); slash >= 0 {
		dir = "/" + prefix[:slash]
		basePrefix = prefix[slash+1:]
	}
	var entries []DirEntry
	err := d.withSession(ctx, func(s Session) error {
		v, e := s.List(ctx, dir)
		if e == nil {
			entries = v
		}
		return e
	})
	if err != nil {
		return vfs.ListPage{}, err
	}
	sort.Slice(entries, func(i, j int) bool { return entries[i].Name < entries[j].Name })
	start := 0
	if opts.ContinuationToken != "" {
		n, e := strconv.Atoi(opts.ContinuationToken)
		if e != nil || n < 0 {
			return vfs.ListPage{}, fmt.Errorf("sftp: invalid continuation token")
		}
		start = n
	}
	max := opts.MaxKeys
	if max <= 0 {
		max = 1000
	}
	out := vfs.ListPage{}
	for i := start; i < len(entries) && len(out.Entries) < max; i++ {
		e := entries[i]
		if !strings.HasPrefix(e.Name, basePrefix) {
			continue
		}
		raw := "sftp://" + u.Host + path.Join(dir, e.Name)
		entryURI, parseErr := vfs.ParseURI(raw)
		if parseErr != nil {
			return vfs.ListPage{}, parseErr
		}
		out.Entries = append(out.Entries, vfs.ListEntry{URI: entryURI, Size: e.Info.Size, Identity: infoIdentity(e.Info)})
		start = i + 1
	}
	for i := start; i < len(entries); i++ {
		if strings.HasPrefix(entries[i].Name, basePrefix) {
			out.Truncated = true
			out.NextContinuationToken = strconv.Itoa(start)
			break
		}
	}
	return out, nil
}
func (d *Driver) ReadRange(ctx context.Context, r vfs.RangeRequest, scope *memory.Scope, bm *memory.BufferManager) (vfs.RangeResult, error) {
	if err := d.validateURI(r.URI); err != nil {
		return vfs.RangeResult{}, err
	}
	if err := r.Validate(); err != nil {
		return vfs.RangeResult{}, err
	}
	if scope == nil || bm == nil {
		return vfs.RangeResult{}, errors.New("sftp: managed memory required")
	}
	tag := r.Tag
	if tag == "" {
		tag = memory.TagSFTPRead
	}
	buf, err := bm.Allocate(scope, tag, int(r.Length))
	if err != nil {
		return vfs.RangeResult{}, err
	}
	release := func(e error) (vfs.RangeResult, error) { buf.Release(); return vfs.RangeResult{}, e }
	var identity vfs.ObjectIdentity
	err = d.withSession(ctx, func(s Session) error {
		before, e := s.Stat(ctx, uriPath(r.URI))
		if e != nil {
			return e
		}
		identity = infoIdentity(before)
		if r.Expected != nil && !r.Expected.Matches(identity) {
			return vfs.ErrObjectChanged
		}
		n, e := s.ReadAt(ctx, uriPath(r.URI), buf.Bytes(), r.Offset)
		if e != nil && !errors.Is(e, io.EOF) {
			return e
		}
		if int64(n) != r.Length {
			return vfs.ErrShortRead
		}
		after, e := s.Stat(ctx, uriPath(r.URI))
		if e != nil {
			return e
		}
		if !identity.Matches(infoIdentity(after)) {
			return vfs.ErrObjectChanged
		}
		return nil
	})
	if err != nil {
		return release(err)
	}
	return vfs.RangeResult{Buffer: buf, Identity: identity}, nil
}
