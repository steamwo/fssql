package webdav

import (
	"context"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"path"
	"sort"
	"strconv"
	"strings"

	"github.com/steamwo/fssql/internal/core/memory"
	"github.com/steamwo/fssql/internal/security"
	"github.com/steamwo/fssql/internal/storage/remote"
	"github.com/steamwo/fssql/internal/storage/vfs"
)

const (
	// defaultMaxListBytes 限制单次 PROPFIND metadata body，避免远端目录导致无界 Go heap。
	defaultMaxListBytes = 4 << 20
	// defaultMaxListEntries 限制单次 Depth:1 目录元数据项数。
	defaultMaxListEntries = 4096
)

var (
	// ErrProtocol 表示 WebDAV/HTTP 响应不满足当前 VFS 操作语义。
	ErrProtocol = errors.New("webdav: protocol error")
	// ErrListLimit 表示 PROPFIND 响应超过受控 metadata 上限。
	ErrListLimit = errors.New("webdav: list limit exceeded")
)

// Driver 把 WebDAV endpoint 暴露为统一 VFS capability driver。
type Driver struct {
	// AtomicMoveVerified is an operator assertion, never inferred from OPTIONS.
	AtomicMoveVerified bool
	// SchemeName 只能是 webdav 或 webdavs。
	SchemeName string
	// Key 固定 endpoint、credential version 与 security profile。
	Key remote.ClientKey
	// Clients 按 AD-030 复用 endpoint-scoped HTTP client/transport。
	Clients *remote.ClientManager
	// Credentials 在每次请求前检查 ACL 与 credential version。
	Credentials CredentialProvider
	// Policy 在 secret 解析前先验证 endpoint，并由 transport 再次绑定实际拨号 IP。
	Policy *security.NetworkPolicy
	// MaxListBytes 是单次 PROPFIND response 上限。
	MaxListBytes int64
	// MaxListEntries 是单次目录 metadata 项数上限。
	MaxListEntries int
	// writeSlots 限制 streaming PUT goroutine 数量，防止无界 goroutine。
	writeSlots chan struct{}
}

// NewDriver 创建 WebDAV driver。
func NewDriver(scheme string, key remote.ClientKey, clients *remote.ClientManager, credentials CredentialProvider, policy *security.NetworkPolicy, maxWrites int) (*Driver, error) {
	scheme = strings.ToLower(scheme)
	if _, err := transportSchemeFor(scheme); err != nil {
		return nil, err
	}
	if err := key.Validate(); err != nil {
		return nil, err
	}
	if clients == nil {
		return nil, errors.New("webdav: client manager required")
	}
	if credentials == nil {
		return nil, errors.New("webdav: credential provider required")
	}
	if policy == nil {
		return nil, errors.New("webdav: NetworkPolicy required")
	}
	if maxWrites <= 0 {
		maxWrites = 4
	}
	return &Driver{
		SchemeName: scheme, Key: key, Clients: clients, Credentials: credentials, Policy: policy,
		MaxListBytes: defaultMaxListBytes, MaxListEntries: defaultMaxListEntries,
		writeSlots: make(chan struct{}, maxWrites),
	}, nil
}

// Scheme 返回 VFS scheme。
func (d *Driver) Scheme() string { return d.SchemeName }

// StaticCapabilities 只声明当前实现可严格保证的能力。
// 条件写、AtomicRename、StableETag 不因 WebDAV 标准“可能支持”而提前宣称。
func (d *Driver) StaticCapabilities() vfs.CapabilitySet {
	caps := vfs.Capabilities(vfs.RandomRead, vfs.ParallelRandomRead, vfs.StreamingRead, vfs.StreamingWrite, vfs.ServerSideList)
	if d.AtomicMoveVerified {
		caps = caps.With(vfs.NoReplacePublish)
	}
	return caps
}

// Probe 使用 OPTIONS 的 Allow/DAV header 收窄可直接探测的写和列表能力；Range 在真实 GET 时严格验证 206。
func (d *Driver) Probe(ctx context.Context) (vfs.ProbeResult, error) {
	root := vfs.URI{Scheme: d.SchemeName, Host: d.Key.Endpoint, Path: "/"}
	request, err := d.newRequest(ctx, http.MethodOptions, root, nil)
	if err != nil {
		return vfs.ProbeResult{}, err
	}
	response, err := d.do(request)
	if err != nil {
		return vfs.ProbeResult{}, err
	}
	defer response.Body.Close()
	if response.StatusCode < 200 || response.StatusCode >= 300 {
		return vfs.ProbeResult{}, statusError("OPTIONS", response.StatusCode)
	}

	// OPTIONS 已经是 endpoint runtime probe；缺失 Allow 时按“不支持”收窄，而不是保留静态能力。
	known := vfs.Capabilities(vfs.StreamingWrite, vfs.ServerSideList, vfs.NoReplacePublish)
	supported := vfs.CapabilitySet(0)
	allow := strings.ToUpper(response.Header.Get("Allow"))
	// 当前流式写依赖 PUT staging + MOVE final；两者缺一都不能宣称 StreamingWrite。
	if headerToken(allow, "PUT") && headerToken(allow, "MOVE") {
		supported = supported.With(vfs.StreamingWrite)
		if d.AtomicMoveVerified {
			supported = supported.With(vfs.NoReplacePublish)
		}
	}
	if headerToken(allow, "PROPFIND") || response.Header.Get("DAV") != "" {
		supported = supported.With(vfs.ServerSideList)
	}
	return vfs.ProbeResult{
		Known: known, Supported: supported, Endpoint: d.Key.Endpoint,
		CredentialScope: d.Key.CredentialScope, CredentialVersion: d.Key.CredentialVersion,
		SecurityProfile: d.Key.SecurityProfile,
	}, nil
}

// validateURI 保证 URI 只访问当前 endpoint-scoped driver。
func (d *Driver) validateURI(uri vfs.URI) error {
	if strings.ToLower(uri.Scheme) != d.SchemeName {
		return vfs.ErrUnsupportedScheme
	}
	if uri.Host != d.Key.Endpoint {
		return fmt.Errorf("webdav: endpoint mismatch: uri=%s driver=%s", uri.Host, d.Key.Endpoint)
	}
	return nil
}

// targetURL 把 VFS scheme 映射到真实 HTTP(S) URL。
func (d *Driver) targetURL(uri vfs.URI) (*url.URL, error) {
	if err := d.validateURI(uri); err != nil {
		return nil, err
	}
	transportScheme, err := transportSchemeFor(d.SchemeName)
	if err != nil {
		return nil, err
	}
	p := uri.Path
	if p == "" {
		p = "/"
	}
	return &url.URL{Scheme: transportScheme, Host: d.Key.Endpoint, Path: p}, nil
}

// newRequest 在解析 secret 前先做 NetworkPolicy endpoint 检查，然后附加 Basic Auth。
func (d *Driver) newRequest(ctx context.Context, method string, uri vfs.URI, body io.Reader) (*http.Request, error) {
	target, err := d.targetURL(uri)
	if err != nil {
		return nil, err
	}
	if _, err := d.Policy.ValidateURL(ctx, target.String()); err != nil {
		return nil, err
	}
	credentials, err := d.Credentials.Credentials(ctx, d.Key, d.SchemeName)
	if err != nil {
		return nil, err
	}
	request, err := http.NewRequestWithContext(ctx, method, target.String(), body)
	if err != nil {
		return nil, err
	}
	request.SetBasicAuth(credentials.Username, credentials.Password)
	return request, nil
}

// do 使用 endpoint-scoped client；client transport 会再次执行 DNS/IP 安全绑定。
func (d *Driver) do(request *http.Request) (*http.Response, error) {
	client, err := d.Clients.Client(d.Key)
	if err != nil {
		return nil, err
	}
	return client.Do(request)
}

// Stat 使用 HEAD 获取 size、ETag 与 Last-Modified；ETag 可增强 identity，但不因此宣称 StableETag capability。
func (d *Driver) Stat(ctx context.Context, uri vfs.URI) (vfs.ObjectInfo, error) {
	request, err := d.newRequest(ctx, http.MethodHead, uri, nil)
	if err != nil {
		return vfs.ObjectInfo{}, err
	}
	response, err := d.do(request)
	if err != nil {
		return vfs.ObjectInfo{}, err
	}
	defer response.Body.Close()
	if response.StatusCode < 200 || response.StatusCode >= 300 {
		return vfs.ObjectInfo{}, statusError("HEAD", response.StatusCode)
	}
	identity, err := identityFromResponse(response)
	if err != nil {
		return vfs.ObjectInfo{}, err
	}
	return vfs.ObjectInfo{URI: uri, Size: identity.Size, Identity: identity}, nil
}

// List 使用 Depth:1 PROPFIND，解析前先对 response bytes 设置硬上限。
func (d *Driver) List(ctx context.Context, uri vfs.URI, opts vfs.ListOptions) (vfs.ListPage, error) {
	request, err := d.newRequest(ctx, "PROPFIND", uri, strings.NewReader(propfindBody))
	if err != nil {
		return vfs.ListPage{}, err
	}
	request.Header.Set("Depth", "1")
	request.Header.Set("Content-Type", "application/xml; charset=utf-8")
	response, err := d.do(request)
	if err != nil {
		return vfs.ListPage{}, err
	}
	defer response.Body.Close()
	if response.StatusCode != http.StatusMultiStatus {
		return vfs.ListPage{}, statusError("PROPFIND", response.StatusCode)
	}
	limit := d.MaxListBytes
	if limit <= 0 {
		limit = defaultMaxListBytes
	}
	body, err := readBounded(response.Body, limit)
	if err != nil {
		return vfs.ListPage{}, err
	}
	entries, err := d.parseList(uri, body)
	if err != nil {
		return vfs.ListPage{}, err
	}
	maxEntries := d.MaxListEntries
	if maxEntries <= 0 {
		maxEntries = defaultMaxListEntries
	}
	if len(entries) > maxEntries {
		return vfs.ListPage{}, ErrListLimit
	}
	sort.Slice(entries, func(i, j int) bool { return entries[i].URI.Path < entries[j].URI.Path })

	start := 0
	if opts.ContinuationToken != "" {
		value, parseErr := strconv.Atoi(opts.ContinuationToken)
		if parseErr != nil || value < 0 || value > len(entries) {
			return vfs.ListPage{}, fmt.Errorf("webdav: invalid continuation token %q", opts.ContinuationToken)
		}
		start = value
	}
	maxKeys := opts.MaxKeys
	if maxKeys <= 0 {
		maxKeys = 1000
	}
	prefix := strings.TrimPrefix(opts.Prefix, "/")
	page := vfs.ListPage{}
	index := start
	for ; index < len(entries) && len(page.Entries) < maxKeys; index++ {
		entry := entries[index]
		relative := strings.TrimPrefix(strings.TrimPrefix(entry.URI.Path, path.Clean(uri.Path)), "/")
		if prefix != "" && !strings.HasPrefix(relative, prefix) {
			continue
		}
		page.Entries = append(page.Entries, entry)
	}
	for ; index < len(entries); index++ {
		entry := entries[index]
		relative := strings.TrimPrefix(strings.TrimPrefix(entry.URI.Path, path.Clean(uri.Path)), "/")
		if prefix == "" || strings.HasPrefix(relative, prefix) {
			page.Truncated = true
			page.NextContinuationToken = strconv.Itoa(index)
			break
		}
	}
	return page, nil
}

// ReadRange 只接受 HTTP 206 exact range；200 whole-object response 立即失败，禁止 silent fallback。
func (d *Driver) ReadRange(ctx context.Context, request vfs.RangeRequest, scope *memory.Scope, bm *memory.BufferManager) (vfs.RangeResult, error) {
	if err := d.validateURI(request.URI); err != nil {
		return vfs.RangeResult{}, err
	}
	if err := request.Validate(); err != nil {
		return vfs.RangeResult{}, err
	}
	if scope == nil || bm == nil {
		return vfs.RangeResult{}, errors.New("webdav: managed memory required")
	}
	before, err := d.Stat(ctx, request.URI)
	if err != nil {
		return vfs.RangeResult{}, err
	}
	if request.Expected != nil && !request.Expected.Matches(before.Identity) {
		return vfs.RangeResult{}, vfs.ErrObjectChanged
	}
	tag := request.Tag
	if tag == "" {
		tag = memory.TagNetworkBuffer
	}
	buffer, err := bm.Allocate(scope, tag, int(request.Length))
	if err != nil {
		return vfs.RangeResult{}, err
	}
	fail := func(err error) (vfs.RangeResult, error) {
		buffer.Release()
		return vfs.RangeResult{}, err
	}

	httpRequest, err := d.newRequest(ctx, http.MethodGet, request.URI, nil)
	if err != nil {
		return fail(err)
	}
	end := request.Offset + request.Length - 1
	httpRequest.Header.Set("Range", fmt.Sprintf("bytes=%d-%d", request.Offset, end))
	// If-Match uses strong comparison; submitting Apache's fresh weak ETag
	// would reject an unchanged resource. Weak metadata is still checked below.
	if before.Identity.ETag != "" && !strings.HasPrefix(before.Identity.ETag, "W/") {
		httpRequest.Header.Set("If-Match", before.Identity.ETag)
	}
	response, err := d.do(httpRequest)
	if err != nil {
		return fail(err)
	}
	defer response.Body.Close()
	if response.StatusCode == http.StatusPreconditionFailed {
		return fail(vfs.ErrObjectChanged)
	}
	if response.StatusCode == http.StatusOK {
		return fail(vfs.ErrRangeUnsupported)
	}
	if response.StatusCode != http.StatusPartialContent {
		return fail(statusError("GET range", response.StatusCode))
	}
	if err := validateContentRange(response.Header.Get("Content-Range"), request.Offset, end); err != nil {
		return fail(err)
	}
	if response.ContentLength >= 0 && response.ContentLength != request.Length {
		return fail(vfs.ErrShortRead)
	}
	if before.Identity.ETag != "" {
		etag := response.Header.Get("ETag")
		if etag != "" && etag != before.Identity.ETag {
			return fail(vfs.ErrObjectChanged)
		}
	}
	n, readErr := io.ReadFull(response.Body, buffer.Bytes())
	if readErr != nil || int64(n) != request.Length {
		return fail(vfs.ErrShortRead)
	}
	if response.ContentLength < 0 {
		var extra [1]byte
		if n, extraErr := response.Body.Read(extra[:]); n != 0 || (extraErr != nil && !errors.Is(extraErr, io.EOF)) {
			return fail(fmt.Errorf("%w: range body exceeds requested length", ErrProtocol))
		}
	}
	after, err := d.Stat(ctx, request.URI)
	if err != nil {
		return fail(err)
	}
	if !before.Identity.Matches(after.Identity) {
		return fail(vfs.ErrObjectChanged)
	}
	return vfs.RangeResult{Buffer: buffer, Identity: before.Identity}, nil
}
