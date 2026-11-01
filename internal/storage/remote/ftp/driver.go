package ftp

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

// Driver 把 FTP/FTPS Session pool 暴露为统一 VFS capability driver。
type Driver struct {
	// SchemeName 只能是 ftp 或 ftps。
	SchemeName string
	// Key 固定 endpoint、credential scope/version 与 security profile。
	Key remote.ClientKey
	// Pools 提供 bounded endpoint-scoped session lease。
	Pools *Manager
	// MaxAttempts 限制 transient connection retry 次数。
	MaxAttempts int
}

// NewDriver 创建 FTP/FTPS VFS driver；plain FTP 是否允许必须由 SessionFactory 的 NetworkPolicy 决定。
func NewDriver(scheme string, key remote.ClientKey, pools *Manager, maxAttempts int) (*Driver, error) {
	scheme = strings.ToLower(scheme)
	if scheme != "ftp" && scheme != "ftps" {
		return nil, fmt.Errorf("ftp: unsupported scheme %q", scheme)
	}
	if err := key.Validate(); err != nil {
		return nil, err
	}
	if pools == nil {
		return nil, errors.New("ftp: pool manager required")
	}
	if maxAttempts <= 0 {
		maxAttempts = 2
	}
	return &Driver{SchemeName: scheme, Key: key, Pools: pools, MaxAttempts: maxAttempts}, nil
}

// Scheme 返回 VFS scheme。
func (d *Driver) Scheme() string { return d.SchemeName }

// StaticCapabilities 只声明实现真实提供的能力；条件写、rename、ETag 均不伪造。
func (d *Driver) StaticCapabilities() vfs.CapabilitySet {
	caps := vfs.Capabilities(vfs.RandomRead, vfs.StreamingRead, vfs.StreamingWrite, vfs.ServerSideList)
	if d.Pools != nil && d.Pools.MaxSessions() > 1 {
		caps = caps.With(vfs.ParallelRandomRead)
	}
	return caps
}

// Probe 通过真实 session 的 FEAT 结果收窄 REST/MLSD 能力，不能授予静态能力之外的新能力。
func (d *Driver) Probe(ctx context.Context) (vfs.ProbeResult, error) {
	pool, err := d.pool()
	if err != nil {
		return vfs.ProbeResult{}, err
	}
	session, err := pool.Acquire(ctx)
	if err != nil {
		return vfs.ProbeResult{}, err
	}
	features, err := session.Features(ctx)
	pool.Release(session, err != nil && IsRetryable(err))
	if err != nil {
		return vfs.ProbeResult{}, err
	}
	known := vfs.Capabilities(vfs.RandomRead, vfs.ParallelRandomRead, vfs.ServerSideList)
	supported := vfs.CapabilitySet(0)
	if features.RestStream {
		supported = supported.With(vfs.RandomRead)
		if d.Pools.MaxSessions() > 1 {
			supported = supported.With(vfs.ParallelRandomRead)
		}
	}
	if features.MLSD {
		supported = supported.With(vfs.ServerSideList)
	}
	return vfs.ProbeResult{
		Known: known, Supported: supported, Endpoint: d.Key.Endpoint,
		CredentialScope: d.Key.CredentialScope, CredentialVersion: d.Key.CredentialVersion,
		SecurityProfile: d.Key.SecurityProfile,
	}, nil
}

// validateURI 保证 VFS URI 与当前 endpoint-scoped pool 一致。
func (d *Driver) validateURI(uri vfs.URI) error {
	if strings.ToLower(uri.Scheme) != d.SchemeName {
		return vfs.ErrUnsupportedScheme
	}
	if uri.Host != d.Key.Endpoint {
		return fmt.Errorf("ftp: endpoint mismatch: uri=%s pool=%s", uri.Host, d.Key.Endpoint)
	}
	return nil
}

// uriPath 规范化 FTP 绝对路径。
func uriPath(uri vfs.URI) string {
	value := uri.Path
	if value == "" {
		value = "/"
	}
	return path.Clean(value)
}

// infoIdentity 将 FTP size/mtime 转换为 VFS 弱身份；FTP 不宣称 StableETag。
func infoIdentity(info FileInfo) vfs.ObjectIdentity {
	identity := vfs.ObjectIdentity{Size: info.Size}
	if info.ModUnix != 0 {
		identity.ModifiedAt = time.Unix(info.ModUnix, 0).UTC()
	}
	return identity
}

// pool 返回当前 ClientKey 的 endpoint-scoped pool。
func (d *Driver) pool() (*Pool, error) { return d.Pools.Pool(d.Key) }

// withSession 在 bounded retry 内执行 control 操作，并正确 poison 连接级失败。
func (d *Driver) withSession(ctx context.Context, fn func(Session) error) error {
	pool, err := d.pool()
	if err != nil {
		return err
	}
	var last error
	for attempt := 0; attempt < d.MaxAttempts; attempt++ {
		if err := ctx.Err(); err != nil {
			return err
		}
		session, err := pool.Acquire(ctx)
		if err != nil {
			last = err
			if !IsRetryable(err) {
				return err
			}
			continue
		}
		err = fn(session)
		poison := IsRetryable(err) || errors.Is(err, context.Canceled) || errors.Is(err, context.DeadlineExceeded)
		pool.Release(session, poison)
		if err == nil {
			return nil
		}
		last = err
		if !IsRetryable(err) || errors.Is(err, context.Canceled) || errors.Is(err, context.DeadlineExceeded) {
			return err
		}
	}
	return last
}

// Stat 查询远端文件元数据。
func (d *Driver) Stat(ctx context.Context, uri vfs.URI) (vfs.ObjectInfo, error) {
	if err := d.validateURI(uri); err != nil {
		return vfs.ObjectInfo{}, err
	}
	var info FileInfo
	err := d.withSession(ctx, func(session Session) error {
		value, err := session.Stat(ctx, uriPath(uri))
		if err == nil {
			info = value
		}
		return err
	})
	if err != nil {
		return vfs.ObjectInfo{}, err
	}
	return vfs.ObjectInfo{URI: uri, Size: info.Size, Identity: infoIdentity(info)}, nil
}

// List 使用 MLSD 结果构造稳定分页；不解析面向人的 LIST 文本作为 Stable 行为。
func (d *Driver) List(ctx context.Context, uri vfs.URI, opts vfs.ListOptions) (vfs.ListPage, error) {
	if err := d.validateURI(uri); err != nil {
		return vfs.ListPage{}, err
	}
	var entries []DirEntry
	if err := d.withSession(ctx, func(session Session) error {
		value, err := session.List(ctx, uriPath(uri))
		if err == nil {
			entries = value
		}
		return err
	}); err != nil {
		return vfs.ListPage{}, err
	}
	sort.Slice(entries, func(i, j int) bool { return entries[i].Name < entries[j].Name })
	start := 0
	if opts.ContinuationToken != "" {
		value, err := strconv.Atoi(opts.ContinuationToken)
		if err != nil || value < 0 {
			return vfs.ListPage{}, errors.New("ftp: invalid continuation token")
		}
		start = value
	}
	max := opts.MaxKeys
	if max <= 0 {
		max = 1000
	}
	prefix := strings.TrimPrefix(opts.Prefix, "/")
	out := vfs.ListPage{}
	for index := start; index < len(entries) && len(out.Entries) < max; index++ {
		entry := entries[index]
		if !strings.HasPrefix(entry.Name, prefix) {
			continue
		}
		raw := d.SchemeName + "://" + uri.Host + path.Join(uriPath(uri), entry.Name)
		entryURI, err := vfs.ParseURI(raw)
		if err != nil {
			return vfs.ListPage{}, err
		}
		out.Entries = append(out.Entries, vfs.ListEntry{URI: entryURI, Size: entry.Info.Size, Identity: infoIdentity(entry.Info)})
		start = index + 1
	}
	for index := start; index < len(entries); index++ {
		if strings.HasPrefix(entries[index].Name, prefix) {
			out.Truncated = true
			out.NextContinuationToken = strconv.Itoa(start)
			break
		}
	}
	return out, nil
}

// ReadRange 使用 Managed Buffer 做 exact range read；Session 不允许整文件读取再丢弃前缀。
func (d *Driver) ReadRange(ctx context.Context, request vfs.RangeRequest, scope *memory.Scope, bm *memory.BufferManager) (vfs.RangeResult, error) {
	if err := d.validateURI(request.URI); err != nil {
		return vfs.RangeResult{}, err
	}
	if err := request.Validate(); err != nil {
		return vfs.RangeResult{}, err
	}
	if scope == nil || bm == nil {
		return vfs.RangeResult{}, errors.New("ftp: managed memory required")
	}
	caps, err := d.Probe(ctx)
	if err != nil {
		return vfs.RangeResult{}, err
	}
	if !caps.Supported.Has(vfs.RandomRead) {
		return vfs.RangeResult{}, vfs.ErrRangeUnsupported
	}
	tag := request.Tag
	if tag == "" {
		tag = memory.TagNetworkBuffer
	}
	buffer, err := bm.Allocate(scope, tag, int(request.Length))
	if err != nil {
		return vfs.RangeResult{}, err
	}
	release := func(err error) (vfs.RangeResult, error) {
		buffer.Release()
		return vfs.RangeResult{}, err
	}
	var identity vfs.ObjectIdentity
	err = d.withSession(ctx, func(session Session) error {
		before, err := session.Stat(ctx, uriPath(request.URI))
		if err != nil {
			return err
		}
		identity = infoIdentity(before)
		if request.Expected != nil && !request.Expected.Matches(identity) {
			return vfs.ErrObjectChanged
		}
		n, err := session.ReadAt(ctx, uriPath(request.URI), buffer.Bytes(), request.Offset)
		if err != nil && !errors.Is(err, io.EOF) {
			return err
		}
		if int64(n) != request.Length {
			return vfs.ErrShortRead
		}
		after, err := session.Stat(ctx, uriPath(request.URI))
		if err != nil {
			return err
		}
		if !identity.Matches(infoIdentity(after)) {
			return vfs.ErrObjectChanged
		}
		return nil
	})
	if err != nil {
		return release(err)
	}
	return vfs.RangeResult{Buffer: buffer, Identity: identity}, nil
}
