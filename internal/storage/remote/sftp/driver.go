package sftp

import (
	"context"
	"errors"
	"fmt"
	"net/url"
	"path"
	"strings"
	"time"

	"github.com/steamwo/fssql/internal/core/memory"
	"github.com/steamwo/fssql/internal/storage/vfs"
)

var ErrHostKeyMismatch = errors.New("sftp: host key mismatch")
var ErrConnectionReset = errors.New("sftp: connection reset")

type RemoteInfo struct {
	Path     string
	Size     int64
	Modified time.Time
}

type RemoteEntry struct {
	Path     string
	Size     int64
	Modified time.Time
}

type RemotePage struct {
	Entries   []RemoteEntry
	NextToken string
	Truncated bool
}

type Config struct {
	Endpoint          string
	CredentialScope   string
	CredentialVersion string
	SecurityProfile   string
	HostKeySHA256     string
	MaxSessions       int
	MaxRetries        int
}

type Driver struct {
	cfg      Config
	endpoint *url.URL
	pool     *sessionPool
}

func NewDriver(cfg Config, dialer Dialer) (*Driver, error) {
	if dialer == nil {
		return nil, errors.New("sftp: dialer is required")
	}
	u, err := url.Parse(cfg.Endpoint)
	if err != nil {
		return nil, fmt.Errorf("sftp: parse endpoint: %w", err)
	}
	if strings.ToLower(u.Scheme) != "sftp" || u.Host == "" {
		return nil, errors.New("sftp: endpoint must use sftp:// with a host")
	}
	if u.User != nil || u.RawQuery != "" || u.Fragment != "" {
		return nil, errors.New("sftp: endpoint must not embed credentials, query, or fragment")
	}
	if cfg.CredentialScope == "" || cfg.SecurityProfile == "" {
		return nil, errors.New("sftp: credential scope and security profile are required")
	}
	if cfg.HostKeySHA256 == "" {
		return nil, errors.New("sftp: pinned host-key SHA256 is required")
	}
	if cfg.MaxSessions <= 0 {
		cfg.MaxSessions = 8
	}
	if cfg.MaxRetries < 0 {
		return nil, errors.New("sftp: negative retry count")
	}
	dialCfg := DialConfig{Endpoint: u.String(), CredentialScope: cfg.CredentialScope, CredentialVersion: cfg.CredentialVersion, SecurityProfile: cfg.SecurityProfile}
	return &Driver{cfg: cfg, endpoint: u, pool: newSessionPool(cfg.MaxSessions, dialer, dialCfg)}, nil
}

func (d *Driver) Close() { d.pool.close() }
func (d *Driver) Scheme() string { return "sftp" }
func (d *Driver) StaticCapabilities() vfs.CapabilitySet {
	return vfs.Capabilities(vfs.RandomRead, vfs.ParallelRandomRead, vfs.ServerSideList)
}
func (d *Driver) Probe(context.Context) (vfs.ProbeResult, error) {
	return vfs.ProbeResult{Supported: d.StaticCapabilities(), Endpoint: d.endpoint.String(), CredentialScope: d.cfg.CredentialScope, CredentialVersion: d.cfg.CredentialVersion, SecurityProfile: d.cfg.SecurityProfile}, nil
}

func (d *Driver) acquireVerified(ctx context.Context) (*lease, error) {
	l, hostKey, err := d.pool.acquire(ctx)
	if err != nil {
		return nil, err
	}
	if hostKey != "" && hostKey != d.cfg.HostKeySHA256 {
		l.Poison()
		l.Release()
		return nil, fmt.Errorf("%w: got=%s", ErrHostKeyMismatch, hostKey)
	}
	return l, nil
}

func (d *Driver) Stat(ctx context.Context, uri vfs.URI) (vfs.ObjectInfo, error) {
	if err := d.checkURI(uri); err != nil {
		return vfs.ObjectInfo{}, err
	}
	var out RemoteInfo
	err := d.retry(ctx, func(l *lease) error {
		var err error
		out, err = l.session.Stat(ctx, uri.Path)
		return err
	})
	if err != nil {
		return vfs.ObjectInfo{}, err
	}
	id := vfs.ObjectIdentity{Size: out.Size, ModifiedAt: out.Modified}
	return vfs.ObjectInfo{URI: uri, Size: out.Size, Identity: id}, nil
}

func (d *Driver) List(ctx context.Context, base vfs.URI, opts vfs.ListOptions) (vfs.ListPage, error) {
	if err := d.checkURI(base); err != nil {
		return vfs.ListPage{}, err
	}
	maxKeys := opts.MaxKeys
	if maxKeys <= 0 {
		maxKeys = 1000
	}
	prefix := path.Join(base.Path, opts.Prefix)
	var remote RemotePage
	err := d.retry(ctx, func(l *lease) error {
		var err error
		remote, err = l.session.List(ctx, prefix, opts.ContinuationToken, maxKeys)
		return err
	})
	if err != nil {
		return vfs.ListPage{}, err
	}
	page := vfs.ListPage{NextContinuationToken: remote.NextToken, Truncated: remote.Truncated}
	for _, entry := range remote.Entries {
		u, err := vfs.ParseURI("sftp://" + base.Host + "/" + strings.TrimPrefix(entry.Path, "/"))
		if err != nil {
			return vfs.ListPage{}, err
		}
		id := vfs.ObjectIdentity{Size: entry.Size, ModifiedAt: entry.Modified}
		page.Entries = append(page.Entries, vfs.ListEntry{URI: u, Size: entry.Size, Identity: id})
	}
	return page, nil
}

func (d *Driver) ReadRange(ctx context.Context, r vfs.RangeRequest, scope *memory.Scope, bm *memory.BufferManager) (vfs.RangeResult, error) {
	if err := r.Validate(); err != nil {
		return vfs.RangeResult{}, err
	}
	if err := d.checkURI(r.URI); err != nil {
		return vfs.RangeResult{}, err
	}
	if scope == nil || bm == nil {
		return vfs.RangeResult{}, errors.New("sftp: managed memory required")
	}
	if r.Length > int64(^uint(0)>>1) {
		return vfs.RangeResult{}, fmt.Errorf("sftp: range too large")
	}
	var result vfs.RangeResult
	err := d.retry(ctx, func(l *lease) error {
		info, err := l.session.Stat(ctx, r.URI.Path)
		if err != nil {
			return err
		}
		identity := vfs.ObjectIdentity{Size: info.Size, ModifiedAt: info.Modified}
		if r.Expected != nil && !r.Expected.Matches(identity) {
			return vfs.ErrObjectChanged
		}
		tag := r.Tag
		if tag == "" {
			tag = memory.TagSFTPRead
		}
		buf, err := bm.Allocate(scope, tag, int(r.Length))
		if err != nil {
			return err
		}
		n, readErr := l.session.ReadAt(ctx, r.URI.Path, r.Offset, buf.Bytes())
		if readErr != nil || int64(n) != r.Length {
			buf.Release()
			if readErr != nil {
				return readErr
			}
			return fmt.Errorf("%w: got=%d want=%d", vfs.ErrShortRead, n, r.Length)
		}
		result = vfs.RangeResult{Buffer: buf, Identity: identity}
		return nil
	})
	if err != nil {
		return vfs.RangeResult{}, err
	}
	return result, nil
}

func (d *Driver) retry(ctx context.Context, fn func(*lease) error) error {
	var last error
	for attempt := 0; attempt <= d.cfg.MaxRetries; attempt++ {
		if err := ctx.Err(); err != nil {
			return err
		}
		l, err := d.acquireVerified(ctx)
		if err != nil {
			return err
		}
		err = fn(l)
		if err == nil {
			l.Release()
			return nil
		}
		last = err
		if errors.Is(err, ErrConnectionReset) {
			l.Poison()
		}
		l.Release()
		if !errors.Is(err, ErrConnectionReset) || attempt == d.cfg.MaxRetries {
			return err
		}
	}
	return last
}

func (d *Driver) checkURI(uri vfs.URI) error {
	if strings.ToLower(uri.Scheme) != "sftp" {
		return fmt.Errorf("sftp: unexpected scheme %q", uri.Scheme)
	}
	if !strings.EqualFold(uri.Host, d.endpoint.Host) {
		return fmt.Errorf("sftp: endpoint mismatch %q", uri.Host)
	}
	return nil
}

func (d *Driver) PoolStats() (open, leased, idle int) { return d.pool.stats() }
