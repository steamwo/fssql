package httpremote

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"fmt"
	"io"
	"io/fs"
	"net/http"
	"net/url"
	"strconv"
	"strings"
	"sync"

	"github.com/steamwo/fssql/internal/core/memory"
	"github.com/steamwo/fssql/internal/security"
	"github.com/steamwo/fssql/internal/storage/vfs"
)

var ErrProtocol = errors.New("http: invalid response")
var ErrEncodingUnsupported = errors.New("http: unsupported content encoding")

type Driver struct {
	scheme    string
	endpoints *endpoints
}

func (d *Driver) Scheme() string { return d.scheme }
func (*Driver) StaticCapabilities() vfs.CapabilitySet {
	return vfs.Capabilities(vfs.StreamingRead, vfs.RandomRead, vfs.ParallelRandomRead, vfs.ConditionalRead, vfs.StableETag)
}
func (*Driver) Probe(context.Context) (vfs.ProbeResult, error) { return vfs.ProbeResult{}, nil }
func (*Driver) List(context.Context, vfs.URI, vfs.ListOptions) (vfs.ListPage, error) {
	return vfs.ListPage{}, errors.New("http: directory listing is unsupported")
}

func (d *Driver) endpoint(u vfs.URI) (*endpoint, error) {
	parsed, err := url.Parse(u.RequestURL())
	if err != nil || !u.IsHTTP() {
		return nil, security.ErrEndpointDenied
	}
	key, err := origin(parsed)
	if err != nil {
		return nil, err
	}
	e := d.endpoints.items[key]
	if e == nil {
		return nil, security.ErrEndpointDenied
	}
	return e, nil
}

type responseBody struct {
	io.ReadCloser
	once   sync.Once
	finish func()
	err    error
}

func (b *responseBody) Close() error {
	b.once.Do(func() { b.err = b.ReadCloser.Close(); b.finish() })
	return b.err
}

func (d *Driver) request(ctx context.Context, u vfs.URI, rangeHeader, etag string) (*http.Response, *endpoint, error) {
	e, err := d.endpoint(u)
	if err != nil {
		return nil, nil, safeError(u, err)
	}
	if d.endpoints.closed.Load() {
		return nil, nil, safeError(u, context.Canceled)
	}
	requestCtx, cancel := context.WithCancel(ctx)
	if e.config.RequestTimeout > 0 {
		cancel()
		requestCtx, cancel = context.WithTimeout(ctx, e.config.RequestTimeout)
	}
	stop := context.AfterFunc(d.endpoints.ctx, cancel)
	select {
	case e.slots <- struct{}{}:
	case <-requestCtx.Done():
		stop()
		cancel()
		return nil, nil, safeError(u, requestCtx.Err())
	}
	finish := func() { <-e.slots; stop(); cancel() }
	// Validate every hop, including reused connections. Each actual dial repeats
	// validation and connects only to that validation's IP addresses.
	if _, err := e.config.Network.ResolveURL(requestCtx, e.config.Endpoint); err != nil {
		finish()
		return nil, nil, safeError(u, err)
	}
	req, err := http.NewRequestWithContext(requestCtx, http.MethodGet, u.RequestURL(), nil)
	if err != nil {
		finish()
		return nil, nil, safeError(u, err)
	}
	for key, value := range e.config.Headers {
		req.Header.Set(key, value)
	}
	if e.config.Username != "" {
		req.SetBasicAuth(e.config.Username, e.config.Password)
	} else if e.config.BearerToken != "" {
		req.Header.Set("Authorization", "Bearer "+e.config.BearerToken)
	}
	req.Header.Set("Accept-Encoding", "identity")
	if rangeHeader != "" {
		req.Header.Set("Range", rangeHeader)
	}
	if etag != "" {
		req.Header.Set("If-Match", etag)
	}
	resp, err := e.client.Do(req)
	if err != nil {
		finish()
		return nil, nil, safeError(u, err)
	}
	resp.Body = &responseBody{ReadCloser: resp.Body, finish: finish}
	return resp, e, nil
}

func isRedirect(status int) bool {
	return status == 301 || status == 302 || status == 303 || status == 307 || status == 308
}

func (d *Driver) follow(ctx context.Context, u vfs.URI, rangeHeader string) (*http.Response, vfs.URI, *endpoint, string, error) {
	seen := map[string]bool{}
	h := sha256.New()
	for hops := 0; ; hops++ {
		if seen[u.IdentityKey()] {
			return nil, u, nil, "", safeError(u, ErrProtocol)
		}
		seen[u.IdentityKey()] = true
		resp, e, err := d.request(ctx, u, rangeHeader, "")
		if err != nil {
			return nil, u, nil, "", err
		}
		_, _ = h.Write([]byte(e.scope))
		if !isRedirect(resp.StatusCode) {
			return resp, u, e, hex.EncodeToString(h.Sum(nil)), nil
		}
		location := resp.Header.Get("Location")
		_ = resp.Body.Close()
		if hops >= 5 || location == "" {
			return nil, u, nil, "", safeError(u, ErrProtocol)
		}
		base, _ := url.Parse(u.RequestURL())
		reference, err := url.Parse(location)
		if err != nil || reference.User != nil || strings.Contains(location, "#") {
			return nil, u, nil, "", safeError(u, ErrProtocol)
		}
		target, err := vfs.ParseURI(base.ResolveReference(reference).String())
		if err != nil {
			return nil, u, nil, "", safeError(u, ErrProtocol)
		}
		if u.Scheme == "https" && target.Scheme != "https" {
			return nil, u, nil, "", safeError(u, security.ErrRedirectDowngrade)
		}
		if _, err := d.endpoint(target); err != nil {
			return nil, u, nil, "", safeError(target, err)
		}
		u = target
	}
}

// OpenObject probes with GET, transfers a complete 200 body without reissuing
// it, or fixes the final URI and strong validator for subsequent exact ranges.
func (d *Driver) OpenObject(ctx context.Context, u vfs.URI) (vfs.ObjectSource, error) {
	var out vfs.ObjectSource
	initial, err := d.endpoint(u)
	if err != nil {
		return out, safeError(u, err)
	}
	resp, final, _, scope, err := d.follow(ctx, u, "bytes=0-0")
	if err != nil {
		return out, err
	}
	transferred := false
	defer func() {
		if !transferred && resp != nil {
			_ = resp.Body.Close()
		}
	}()
	encoding, err := contentEncoding(resp)
	if err != nil {
		return out, safeError(final, err)
	}
	out.ScopeKey = scope + ":" + final.IdentityKey()
	makeStream := func() (vfs.ObjectSource, error) {
		if resp.StatusCode != 200 || resp.Header.Get("Content-Range") != "" {
			return vfs.ObjectSource{}, safeError(final, statusError(resp.StatusCode))
		}
		out.Info = vfs.ObjectInfo{URI: final, Size: resp.ContentLength, Identity: vfs.ObjectIdentity{ETag: resp.Header.Get("ETag"), Size: resp.ContentLength}}
		out.Capabilities = vfs.Capabilities(vfs.StreamingRead)
		out.Stream = &safeBody{ReadCloser: resp.Body, uri: final}
		out.ContentEncoding = encoding
		transferred = true
		return out, nil
	}
	switch resp.StatusCode {
	case 200:
		if resp.Header.Get("Content-Range") != "" {
			return out, safeError(final, ErrProtocol)
		}
		if initial.config.FallbackPolicy == FallbackRequireRange {
			return out, safeError(final, vfs.ErrRangeUnsupported)
		}
		return makeStream()
	case 206:
		start, end, total, err := parseContentRange(singleHeader(resp, "Content-Range"))
		if err != nil || start != 0 || end != 0 || resp.ContentLength >= 0 && resp.ContentLength != 1 {
			return out, safeError(final, ErrProtocol)
		}
		var one [1]byte
		if err := readExact(resp.Body, one[:]); err != nil {
			return out, safeError(final, err)
		}
		etag := singleHeader(resp, "ETag")
		if total >= 0 && strongETag(etag) && encoding == "identity" {
			identity := vfs.ObjectIdentity{ETag: etag, Size: total}
			out.Info = vfs.ObjectInfo{URI: final, Size: total, Identity: identity}
			out.Capabilities = vfs.Capabilities(vfs.RandomRead, vfs.ParallelRandomRead, vfs.ConditionalRead, vfs.StableETag)
			out.Reader = &snapshotReader{driver: d, uri: final, identity: identity}
			return out, nil
		}
		if initial.config.FallbackPolicy == FallbackRequireRange {
			return out, safeError(final, vfs.ErrRangeUnsupported)
		}
		_ = resp.Body.Close()
		resp, final, _, scope, err = d.follow(ctx, final, "")
		if err != nil {
			return out, err
		}
		encoding, err = contentEncoding(resp)
		if err != nil {
			return out, safeError(final, err)
		}
		out.ScopeKey += ":" + scope + ":" + final.IdentityKey()
		return makeStream()
	case 416:
		start, end, total, err := parseContentRange(singleHeader(resp, "Content-Range"))
		if err != nil || start != -1 || end != -1 || total != 0 {
			return out, safeError(final, ErrProtocol)
		}
		out.Info = vfs.ObjectInfo{URI: final, Size: 0, Identity: vfs.ObjectIdentity{Size: 0}}
		out.Stream = io.NopCloser(strings.NewReader(""))
		return out, nil
	default:
		return out, safeError(final, statusError(resp.StatusCode))
	}
}

func (d *Driver) Stat(ctx context.Context, u vfs.URI) (vfs.ObjectInfo, error) {
	source, err := d.OpenObject(ctx, u)
	if source.Stream != nil {
		_ = source.Stream.Close()
	}
	return source.Info, err
}

func (d *Driver) ReadRange(ctx context.Context, req vfs.RangeRequest, s *memory.Scope, bm *memory.BufferManager) (vfs.RangeResult, error) {
	if req.Expected == nil || !strongETag(req.Expected.ETag) {
		return vfs.RangeResult{}, safeError(req.URI, vfs.ErrRangeUnsupported)
	}
	return (&snapshotReader{driver: d, uri: req.URI, identity: *req.Expected}).ReadRange(ctx, req, s, bm)
}

type snapshotReader struct {
	driver   *Driver
	uri      vfs.URI
	identity vfs.ObjectIdentity
}

func (r *snapshotReader) ReadRange(ctx context.Context, req vfs.RangeRequest, s *memory.Scope, bm *memory.BufferManager) (vfs.RangeResult, error) {
	if err := req.Validate(); err != nil {
		return vfs.RangeResult{}, err
	}
	if req.Expected != nil && !req.Expected.Matches(r.identity) {
		return vfs.RangeResult{}, vfs.ErrObjectChanged
	}
	if req.Length > r.identity.Size || req.Offset > r.identity.Size-req.Length || req.Length > int64(int(^uint(0)>>1)) {
		return vfs.RangeResult{}, vfs.ErrShortRead
	}
	end := req.Offset + req.Length - 1
	resp, _, err := r.driver.request(ctx, r.uri, fmt.Sprintf("bytes=%d-%d", req.Offset, end), r.identity.ETag)
	if err != nil {
		return vfs.RangeResult{}, err
	}
	defer resp.Body.Close()
	if resp.StatusCode == 412 || isRedirect(resp.StatusCode) || resp.StatusCode == 200 {
		return vfs.RangeResult{}, safeError(r.uri, vfs.ErrObjectChanged)
	}
	if resp.StatusCode != 206 {
		return vfs.RangeResult{}, safeError(r.uri, statusError(resp.StatusCode))
	}
	if err := identityEncoding(resp); err != nil {
		return vfs.RangeResult{}, safeError(r.uri, err)
	}
	start, actualEnd, total, err := parseContentRange(singleHeader(resp, "Content-Range"))
	if err != nil || start != req.Offset || actualEnd != end {
		return vfs.RangeResult{}, safeError(r.uri, ErrProtocol)
	}
	if total != r.identity.Size || singleHeader(resp, "ETag") != r.identity.ETag {
		return vfs.RangeResult{}, safeError(r.uri, vfs.ErrObjectChanged)
	}
	if resp.ContentLength >= 0 && resp.ContentLength != req.Length {
		return vfs.RangeResult{}, safeError(r.uri, ErrProtocol)
	}
	buf, err := bm.Allocate(s, req.Tag, int(req.Length))
	if err != nil {
		return vfs.RangeResult{}, err
	}
	if err := readExact(resp.Body, buf.Bytes()); err != nil {
		buf.Release()
		return vfs.RangeResult{}, safeError(r.uri, err)
	}
	return vfs.RangeResult{Buffer: buf, Identity: r.identity}, nil
}

type safeBody struct {
	io.ReadCloser
	uri vfs.URI
}

func (b *safeBody) Read(p []byte) (int, error) {
	n, err := b.ReadCloser.Read(p)
	if err != nil && err != io.EOF {
		err = safeError(b.uri, err)
	}
	return n, err
}
func (b *safeBody) Close() error {
	if err := b.ReadCloser.Close(); err != nil {
		return safeError(b.uri, err)
	}
	return nil
}

func statusError(status int) error {
	switch status {
	case 401, 403:
		return vfs.ErrCredential
	case 404:
		return fs.ErrNotExist
	case 412:
		return vfs.ErrObjectChanged
	}
	return fmt.Errorf("%w: unexpected status %d", ErrProtocol, status)
}
func singleHeader(resp *http.Response, name string) string {
	values := resp.Header.Values(name)
	if len(values) != 1 {
		return ""
	}
	return values[0]
}
func identityEncoding(resp *http.Response) error {
	kind, err := contentEncoding(resp)
	if err != nil {
		return err
	}
	if kind != "identity" {
		return ErrEncodingUnsupported
	}
	return nil
}

func contentEncoding(resp *http.Response) (string, error) {
	values := resp.Header.Values("Content-Encoding")
	if len(values) == 0 {
		return "identity", nil
	}
	if len(values) == 1 {
		kind := strings.ToLower(strings.TrimSpace(values[0]))
		if kind == "" || kind == "identity" {
			return "identity", nil
		}
		if kind == "gzip" {
			return "gzip", nil
		}
	}
	return "", ErrEncodingUnsupported
}
func strongETag(s string) bool {
	if len(s) < 2 || s[0] != '"' || s[len(s)-1] != '"' {
		return false
	}
	for i := 1; i < len(s)-1; i++ {
		if s[i] < 0x21 || s[i] == '"' || s[i] == 0x7f {
			return false
		}
	}
	return true
}
func readExact(reader io.Reader, data []byte) error {
	if _, err := io.ReadFull(reader, data); err != nil {
		return errors.Join(ErrProtocol, vfs.ErrShortRead, err)
	}
	var extra [1]byte
	n, err := reader.Read(extra[:])
	if n != 0 || err != io.EOF {
		return errors.Join(ErrProtocol, err)
	}
	return nil
}

// Unknown totals are valid for capability discovery, never for range snapshots.
func parseContentRange(value string) (int64, int64, int64, error) {
	bad := func() (int64, int64, int64, error) { return 0, 0, 0, ErrProtocol }
	if !strings.HasPrefix(value, "bytes ") {
		return bad()
	}
	interval, totalText, ok := strings.Cut(strings.TrimPrefix(value, "bytes "), "/")
	if !ok {
		return bad()
	}
	number := func(s string) (int64, error) {
		if s == "" {
			return 0, ErrProtocol
		}
		for _, c := range s {
			if c < '0' || c > '9' {
				return 0, ErrProtocol
			}
		}
		return strconv.ParseInt(s, 10, 64)
	}
	total := int64(-1)
	if totalText != "*" {
		var err error
		total, err = number(totalText)
		if err != nil {
			return bad()
		}
	}
	if interval == "*" {
		if total < 0 {
			return bad()
		}
		return -1, -1, total, nil
	}
	startText, endText, ok := strings.Cut(interval, "-")
	if !ok {
		return bad()
	}
	start, err := number(startText)
	if err != nil {
		return bad()
	}
	end, err := number(endText)
	if err != nil || end < start || total >= 0 && end >= total {
		return bad()
	}
	return start, end, total, nil
}
