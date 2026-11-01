package s3

import (
	"context"
	"encoding/xml"
	"errors"
	"fmt"
	"io"
	"net"
	"net/http"
	"net/url"
	"strconv"
	"strings"
	"time"

	"github.com/steamwo/fssql/internal/core/memory"
	"github.com/steamwo/fssql/internal/storage/remote"
	"github.com/steamwo/fssql/internal/storage/vfs"
)

const defaultMaxRetries = 3

type Config struct {
	Endpoint          string
	Region            string
	ForcePathStyle    bool
	AllowInsecureHTTP bool
	CredentialScope   string
	CredentialVersion string
	SecurityProfile   string
	MaxRetries        int
	RequestTimeout    time.Duration
}

func (c Config) validate() (*url.URL, error) {
	if c.Endpoint == "" {
		return nil, errors.New("s3: empty endpoint")
	}
	u, err := url.Parse(c.Endpoint)
	if err != nil {
		return nil, fmt.Errorf("s3: parse endpoint: %w", err)
	}
	if u.Scheme != "https" && !(c.AllowInsecureHTTP && u.Scheme == "http") {
		return nil, fmt.Errorf("s3: endpoint scheme %q is not allowed", u.Scheme)
	}
	if u.Host == "" {
		return nil, errors.New("s3: endpoint missing host")
	}
	if c.Region == "" {
		return nil, errors.New("s3: empty region")
	}
	if c.CredentialScope == "" {
		return nil, errors.New("s3: empty credential scope")
	}
	if c.SecurityProfile == "" {
		return nil, errors.New("s3: empty security profile")
	}
	return u, nil
}

type Driver struct {
	cfg      Config
	endpoint *url.URL
	creds    Credentials
	clients  *remote.ClientManager
	now      func() time.Time
	sleep    func(context.Context, time.Duration) error
}

func NewDriver(cfg Config, creds Credentials, clients *remote.ClientManager) (*Driver, error) {
	endpoint, err := cfg.validate()
	if err != nil {
		return nil, err
	}
	if err := creds.Validate(); err != nil {
		return nil, err
	}
	if cfg.MaxRetries <= 0 {
		cfg.MaxRetries = defaultMaxRetries
	}
	if cfg.RequestTimeout <= 0 {
		cfg.RequestTimeout = 30 * time.Second
	}
	if clients == nil {
		clients = remote.NewClientManager(32, func(remote.ClientKey) (*http.Client, error) {
			return &http.Client{Timeout: cfg.RequestTimeout}, nil
		})
	}
	return &Driver{
		cfg: cfg, endpoint: endpoint, creds: creds, clients: clients,
		now: time.Now,
		sleep: func(ctx context.Context, d time.Duration) error {
			t := time.NewTimer(d)
			defer t.Stop()
			select {
			case <-ctx.Done():
				return ctx.Err()
			case <-t.C:
				return nil
			}
		},
	}, nil
}

func (d *Driver) Scheme() string { return "s3" }

// StaticCapabilities 只声明已经由驱动真实实现的能力；Phase 4 的 ConditionalWrite
// 在 PutObject/CompleteMultipartUpload 原子完成点映射条件写，IncompleteWriteRecovery 只清理 job 独占 staging scope；仍不伪造目录 rename。
func (d *Driver) StaticCapabilities() vfs.CapabilitySet {
	return vfs.Capabilities(vfs.RandomRead, vfs.ParallelRandomRead, vfs.StreamingWrite, vfs.MultipartWrite, vfs.ConditionalRead, vfs.ConditionalWrite, vfs.StableETag, vfs.VersionedObject, vfs.ServerSideList, vfs.IncompleteWriteRecovery)
}

func (d *Driver) Probe(context.Context) (vfs.ProbeResult, error) {
	return vfs.ProbeResult{
		Supported: d.StaticCapabilities(), Endpoint: d.endpoint.String(),
		CredentialScope: d.cfg.CredentialScope, CredentialVersion: d.cfg.CredentialVersion,
		SecurityProfile: d.cfg.SecurityProfile,
	}, nil
}

func (d *Driver) Stat(ctx context.Context, uri vfs.URI) (vfs.ObjectInfo, error) {
	if uri.Scheme != "s3" {
		return vfs.ObjectInfo{}, fmt.Errorf("s3: unexpected scheme %q", uri.Scheme)
	}
	reqURL, err := d.objectURL(uri, "")
	if err != nil {
		return vfs.ObjectInfo{}, err
	}
	resp, err := d.do(ctx, http.MethodHead, reqURL, nil)
	if err != nil {
		return vfs.ObjectInfo{}, err
	}
	defer resp.Body.Close()
	if err := statusError(resp); err != nil {
		return vfs.ObjectInfo{}, err
	}
	identity, err := identityFromResponse(resp)
	if err != nil {
		return vfs.ObjectInfo{}, err
	}
	return vfs.ObjectInfo{URI: uri, Size: identity.Size, Identity: identity}, nil
}

func (d *Driver) List(ctx context.Context, base vfs.URI, opts vfs.ListOptions) (vfs.ListPage, error) {
	if base.Scheme != "s3" {
		return vfs.ListPage{}, fmt.Errorf("s3: unexpected scheme %q", base.Scheme)
	}
	maxKeys := opts.MaxKeys
	if maxKeys <= 0 || maxKeys > 1000 {
		maxKeys = 1000
	}
	u, err := d.bucketURL(base.Host)
	if err != nil {
		return vfs.ListPage{}, err
	}
	q := u.Query()
	q.Set("list-type", "2")
	q.Set("max-keys", strconv.Itoa(maxKeys))
	if opts.Prefix != "" {
		q.Set("prefix", opts.Prefix)
	}
	if opts.ContinuationToken != "" {
		q.Set("continuation-token", opts.ContinuationToken)
	}
	u.RawQuery = q.Encode()
	resp, err := d.do(ctx, http.MethodGet, u, nil)
	if err != nil {
		return vfs.ListPage{}, err
	}
	defer resp.Body.Close()
	if err := statusError(resp); err != nil {
		return vfs.ListPage{}, err
	}
	var result listBucketResult
	if err := xml.NewDecoder(io.LimitReader(resp.Body, 16<<20)).Decode(&result); err != nil {
		return vfs.ListPage{}, fmt.Errorf("s3: decode ListObjectsV2: %w", err)
	}
	page := vfs.ListPage{NextContinuationToken: result.NextContinuationToken, Truncated: result.IsTruncated}
	page.Entries = make([]vfs.ListEntry, 0, len(result.Contents))
	for _, obj := range result.Contents {
		uri, err := vfs.ParseURI("s3://" + base.Host + "/" + obj.Key)
		if err != nil {
			return vfs.ListPage{}, err
		}
		modified, _ := time.Parse(time.RFC3339, obj.LastModified)
		id := vfs.ObjectIdentity{ETag: trimETag(obj.ETag), Size: obj.Size, ModifiedAt: modified}
		page.Entries = append(page.Entries, vfs.ListEntry{URI: uri, Size: obj.Size, Identity: id})
	}
	return page, nil
}

func (d *Driver) ReadRange(ctx context.Context, r vfs.RangeRequest, scope *memory.Scope, bm *memory.BufferManager) (vfs.RangeResult, error) {
	if err := r.Validate(); err != nil {
		return vfs.RangeResult{}, err
	}
	if r.URI.Scheme != "s3" {
		return vfs.RangeResult{}, fmt.Errorf("s3: unexpected scheme %q", r.URI.Scheme)
	}
	if scope == nil || bm == nil {
		return vfs.RangeResult{}, errors.New("s3: managed memory scope and buffer manager are required")
	}
	if r.Length > int64(^uint(0)>>1) {
		return vfs.RangeResult{}, fmt.Errorf("s3: range too large: %d", r.Length)
	}
	tag := r.Tag
	if tag == "" {
		tag = memory.TagNetworkBuffer
	}
	buf, err := bm.Allocate(scope, tag, int(r.Length))
	if err != nil {
		return vfs.RangeResult{}, err
	}
	keep := false
	defer func() {
		if !keep {
			buf.Release()
		}
	}()
	versionID := ""
	if r.Expected != nil {
		versionID = r.Expected.VersionID
	}
	reqURL, err := d.objectURL(r.URI, versionID)
	if err != nil {
		return vfs.RangeResult{}, err
	}
	headers := make(http.Header)
	headers.Set("Range", fmt.Sprintf("bytes=%d-%d", r.Offset, r.Offset+r.Length-1))
	if r.Expected != nil && r.Expected.ETag != "" {
		headers.Set("If-Match", quoteETag(r.Expected.ETag))
	}
	resp, err := d.do(ctx, http.MethodGet, reqURL, headers)
	if err != nil {
		return vfs.RangeResult{}, err
	}
	defer resp.Body.Close()
	if resp.StatusCode == http.StatusPreconditionFailed {
		return vfs.RangeResult{}, vfs.ErrObjectChanged
	}
	if resp.StatusCode == http.StatusOK {
		return vfs.RangeResult{}, vfs.ErrRangeUnsupported
	}
	if resp.StatusCode != http.StatusPartialContent {
		if err := statusError(resp); err != nil {
			return vfs.RangeResult{}, err
		}
		return vfs.RangeResult{}, fmt.Errorf("s3: range status %d", resp.StatusCode)
	}
	identity, total, err := identityFromRangeResponse(resp, r.Offset, r.Length)
	if err != nil {
		return vfs.RangeResult{}, err
	}
	if total > 0 {
		identity.Size = total
	}
	if r.Expected != nil && !r.Expected.Matches(identity) {
		return vfs.RangeResult{}, vfs.ErrObjectChanged
	}
	n, err := io.ReadFull(resp.Body, buf.Bytes())
	if err != nil || int64(n) != r.Length {
		return vfs.RangeResult{}, fmt.Errorf("%w: got=%d want=%d", vfs.ErrShortRead, n, r.Length)
	}
	if resp.ContentLength >= 0 && resp.ContentLength != r.Length {
		return vfs.RangeResult{}, fmt.Errorf("%w: content-length=%d want=%d", vfs.ErrShortRead, resp.ContentLength, r.Length)
	}
	keep = true
	return vfs.RangeResult{Buffer: buf, Identity: identity}, nil
}

func (d *Driver) do(ctx context.Context, method string, u *url.URL, headers http.Header) (*http.Response, error) {
	key := remote.ClientKey{Endpoint: d.endpoint.String(), CredentialScope: d.cfg.CredentialScope, CredentialVersion: d.cfg.CredentialVersion, SecurityProfile: d.cfg.SecurityProfile}
	client, err := d.clients.Client(key)
	if err != nil {
		return nil, err
	}
	var lastErr error
	for attempt := 0; attempt <= d.cfg.MaxRetries; attempt++ {
		req, err := http.NewRequestWithContext(ctx, method, u.String(), nil)
		if err != nil {
			return nil, err
		}
		for k, vs := range headers {
			for _, v := range vs {
				req.Header.Add(k, v)
			}
		}
		if err := (SigV4Signer{Region: d.cfg.Region, Creds: d.creds}).Sign(req, d.now(), emptyPayloadSHA256); err != nil {
			return nil, err
		}
		resp, err := client.Do(req)
		if err == nil && !retryableStatus(resp.StatusCode) {
			return resp, nil
		}
		if err == nil {
			_, _ = io.Copy(io.Discard, io.LimitReader(resp.Body, 64<<10))
			_ = resp.Body.Close()
			lastErr = fmt.Errorf("s3: retryable status %d", resp.StatusCode)
		} else {
			lastErr = err
			if !retryableTransport(err) {
				return nil, err
			}
		}
		if attempt == d.cfg.MaxRetries {
			break
		}
		if err := d.sleep(ctx, time.Duration(1<<attempt)*10*time.Millisecond); err != nil {
			return nil, err
		}
	}
	return nil, lastErr
}

func (d *Driver) objectURL(uri vfs.URI, versionID string) (*url.URL, error) {
	var u *url.URL
	if d.cfg.ForcePathStyle {
		base := *d.endpoint
		base.Path = strings.TrimRight(base.Path, "/") + "/" + uri.Host
		if key := uri.ObjectKey(); key != "" {
			base.Path += "/" + key
		}
		u = &base
	} else {
		base := *d.endpoint
		base.Host = uri.Host + "." + base.Host
		base.Path = "/" + uri.ObjectKey()
		u = &base
	}
	if versionID != "" {
		q := u.Query()
		q.Set("versionId", versionID)
		u.RawQuery = q.Encode()
	}
	return u, nil
}

func (d *Driver) bucketURL(bucket string) (*url.URL, error) {
	base := *d.endpoint
	if d.cfg.ForcePathStyle {
		base.Path = strings.TrimRight(base.Path, "/") + "/" + bucket
	} else {
		base.Host = bucket + "." + base.Host
	}
	return &base, nil
}

func retryableStatus(code int) bool {
	return code == http.StatusTooManyRequests || code == http.StatusServiceUnavailable || code == http.StatusBadGateway || code == http.StatusGatewayTimeout || code == http.StatusInternalServerError
}

func retryableTransport(err error) bool {
	var ne net.Error
	return errors.As(err, &ne) || errors.Is(err, io.ErrUnexpectedEOF)
}

func statusError(resp *http.Response) error {
	if resp.StatusCode >= 200 && resp.StatusCode < 300 {
		return nil
	}
	if resp.StatusCode == http.StatusUnauthorized || resp.StatusCode == http.StatusForbidden {
		return fmt.Errorf("%w: http status %d", vfs.ErrCredential, resp.StatusCode)
	}
	if resp.StatusCode == http.StatusPreconditionFailed {
		return vfs.ErrObjectChanged
	}
	return fmt.Errorf("s3: http status %d", resp.StatusCode)
}

func identityFromResponse(resp *http.Response) (vfs.ObjectIdentity, error) {
	sz := resp.ContentLength
	if sz < 0 {
		if raw := resp.Header.Get("Content-Length"); raw != "" {
			n, err := strconv.ParseInt(raw, 10, 64)
			if err != nil {
				return vfs.ObjectIdentity{}, fmt.Errorf("s3: invalid content-length: %w", err)
			}
			sz = n
		}
	}
	modified, _ := http.ParseTime(resp.Header.Get("Last-Modified"))
	return vfs.ObjectIdentity{ETag: trimETag(resp.Header.Get("ETag")), VersionID: resp.Header.Get("x-amz-version-id"), Size: sz, ModifiedAt: modified}, nil
}

func identityFromRangeResponse(resp *http.Response, offset, length int64) (vfs.ObjectIdentity, int64, error) {
	start, end, total, err := parseContentRange(resp.Header.Get("Content-Range"))
	if err != nil {
		return vfs.ObjectIdentity{}, 0, err
	}
	if start != offset || end-start+1 != length {
		return vfs.ObjectIdentity{}, 0, fmt.Errorf("s3: unexpected content-range %d-%d for %d+%d", start, end, offset, length)
	}
	id, err := identityFromResponse(resp)
	return id, total, err
}

func parseContentRange(raw string) (start, end, total int64, err error) {
	if !strings.HasPrefix(raw, "bytes ") {
		return 0, 0, 0, fmt.Errorf("s3: missing/invalid content-range %q", raw)
	}
	parts := strings.Split(strings.TrimPrefix(raw, "bytes "), "/")
	if len(parts) != 2 {
		return 0, 0, 0, fmt.Errorf("s3: invalid content-range %q", raw)
	}
	se := strings.Split(parts[0], "-")
	if len(se) != 2 {
		return 0, 0, 0, fmt.Errorf("s3: invalid content-range %q", raw)
	}
	start, err = strconv.ParseInt(se[0], 10, 64)
	if err != nil {
		return
	}
	end, err = strconv.ParseInt(se[1], 10, 64)
	if err != nil {
		return
	}
	if parts[1] != "*" {
		total, err = strconv.ParseInt(parts[1], 10, 64)
	}
	return
}

func trimETag(s string) string { return strings.Trim(strings.TrimSpace(s), "\"") }

func quoteETag(s string) string {
	s = trimETag(s)
	if s == "" {
		return ""
	}
	return "\"" + s + "\""
}

type listBucketResult struct {
	XMLName               xml.Name `xml:"ListBucketResult"`
	IsTruncated           bool     `xml:"IsTruncated"`
	NextContinuationToken string   `xml:"NextContinuationToken"`
	Contents              []struct {
		Key          string `xml:"Key"`
		LastModified string `xml:"LastModified"`
		ETag         string `xml:"ETag"`
		Size         int64  `xml:"Size"`
	} `xml:"Contents"`
}
