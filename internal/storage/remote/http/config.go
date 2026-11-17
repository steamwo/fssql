package httpremote

import (
	"context"
	"crypto/sha256"
	"crypto/tls"
	"encoding/binary"
	"encoding/hex"
	"errors"
	"fmt"
	"net"
	"net/http"
	"net/url"
	"sort"
	"strconv"
	"strings"
	"sync/atomic"
	"time"

	"github.com/steamwo/fssql/internal/security"
	"github.com/steamwo/fssql/internal/storage/vfs"
)

type FallbackPolicy string

const (
	FallbackMaterialize  FallbackPolicy = "materialize"
	FallbackRequireRange FallbackPolicy = "require_range"
)

type Config struct {
	Endpoint                        string
	Username, Password, BearerToken string
	Headers                         map[string]string
	Network                         *security.NetworkPolicy
	TLSConfig                       *tls.Config
	MaxConnections                  int
	RequestTimeout                  time.Duration
	FallbackPolicy                  FallbackPolicy
}

type endpoint struct {
	key, scope string
	config     Config
	client     *http.Client
	transport  *http.Transport
	slots      chan struct{}
}

type endpoints struct {
	items  map[string]*endpoint
	ctx    context.Context
	cancel context.CancelFunc
	closed atomic.Bool
}

// NewDrivers freezes configuration without DNS, dialing, or capability probes.
// Both scheme drivers share the explicitly authorized redirect destinations.
func NewDrivers(configs []Config) ([]vfs.Driver, func() error, error) {
	ctx, cancel := context.WithCancel(context.Background())
	all := &endpoints{items: make(map[string]*endpoint), ctx: ctx, cancel: cancel}
	closeAll := func() error {
		if all.closed.CompareAndSwap(false, true) {
			cancel()
			for _, e := range all.items {
				e.transport.CloseIdleConnections()
			}
		}
		return nil
	}
	schemes := map[string]bool{}
	for _, config := range configs {
		e, scheme, err := newEndpoint(config)
		if err != nil {
			_ = closeAll()
			return nil, nil, err
		}
		if _, exists := all.items[e.key]; exists {
			e.transport.CloseIdleConnections()
			_ = closeAll()
			return nil, nil, errors.New("http: duplicate normalized endpoint")
		}
		all.items[e.key], schemes[scheme] = e, true
	}
	var drivers []vfs.Driver
	for _, scheme := range []string{"http", "https"} {
		if schemes[scheme] {
			drivers = append(drivers, &Driver{scheme: scheme, endpoints: all})
		}
	}
	return drivers, closeAll, nil
}

func origin(u *url.URL) (string, error) {
	scheme := strings.ToLower(u.Scheme)
	if (scheme != "http" && scheme != "https") || u.Hostname() == "" || u.User != nil || strings.HasSuffix(u.Host, ":") {
		return "", errors.New("http: invalid origin")
	}
	port := 80
	if scheme == "https" {
		port = 443
	}
	if u.Port() != "" {
		n, err := strconv.Atoi(u.Port())
		if err != nil || n <= 0 || n > 65535 {
			return "", errors.New("http: invalid endpoint port")
		}
		port = n
	}
	host := strings.TrimSuffix(strings.ToLower(u.Hostname()), ".")
	return scheme + "://" + net.JoinHostPort(host, strconv.Itoa(port)), nil
}

func newEndpoint(c Config) (*endpoint, string, error) {
	u, err := url.Parse(c.Endpoint)
	if err != nil {
		return nil, "", errors.New("http: invalid endpoint URL")
	}
	key, err := origin(u)
	if err != nil || u.Opaque != "" || u.Path != "" || u.RawQuery != "" || u.ForceQuery || strings.Contains(c.Endpoint, "#") {
		return nil, "", errors.New("http: endpoint must be http(s)://host[:port] without path, query, userinfo or fragment")
	}
	if c.FallbackPolicy == "" {
		c.FallbackPolicy = FallbackMaterialize
	}
	if c.FallbackPolicy != FallbackMaterialize && c.FallbackPolicy != FallbackRequireRange {
		return nil, "", errors.New("http: invalid fallback policy")
	}
	if c.RequestTimeout < 0 || c.MaxConnections < 0 {
		return nil, "", errors.New("http: negative timeout or connection limit")
	}
	if c.MaxConnections == 0 {
		c.MaxConnections = 8
	}
	if (c.Username == "" && c.Password != "") || (c.BearerToken != "" && (c.Username != "" || c.Password != "")) || strings.Contains(c.Username, ":") || strings.ContainsAny(c.Username+c.Password+c.BearerToken, "\r\n\x00") {
		return nil, "", errors.New("http: invalid or conflicting authentication")
	}
	headers := make(map[string]string, len(c.Headers))
	for name, value := range c.Headers {
		if !headerName(name) || !headerValue(value) {
			return nil, "", errors.New("http: invalid custom header")
		}
		name = http.CanonicalHeaderKey(name)
		if forbiddenHeader(name) {
			return nil, "", errors.New("http: driver-managed request header cannot be customized")
		}
		if _, ok := headers[name]; ok {
			return nil, "", errors.New("http: duplicate custom header")
		}
		headers[name] = value
	}
	c.Headers = headers
	if c.TLSConfig != nil {
		c.TLSConfig = c.TLSConfig.Clone()
	} else {
		c.TLSConfig = &tls.Config{MinVersion: tls.VersionTLS12}
	}
	if c.Network == nil {
		_, portText, _ := net.SplitHostPort(strings.TrimPrefix(key, strings.ToLower(u.Scheme)+"://"))
		port, _ := strconv.Atoi(portText)
		c.Network = security.NewNetworkPolicy(port)
	}
	e := &endpoint{key: key, scope: configScope(c, key), config: c, slots: make(chan struct{}, c.MaxConnections)}
	dialer := &net.Dialer{Timeout: 10 * time.Second, KeepAlive: 30 * time.Second}
	e.transport = &http.Transport{
		Proxy: nil, DisableCompression: true, ForceAttemptHTTP2: true,
		TLSClientConfig: c.TLSConfig, TLSHandshakeTimeout: 10 * time.Second, ResponseHeaderTimeout: 30 * time.Second,
		MaxResponseHeaderBytes: 64 << 10, MaxConnsPerHost: c.MaxConnections, MaxIdleConnsPerHost: c.MaxConnections, IdleConnTimeout: 90 * time.Second,
		DialContext: func(ctx context.Context, network, addr string) (net.Conn, error) {
			target, err := url.Parse(u.Scheme + "://" + addr)
			if err != nil {
				return nil, security.ErrEndpointDenied
			}
			actual, err := origin(target)
			if err != nil || actual != key {
				return nil, security.ErrEndpointDenied
			}
			validated, err := c.Network.ResolveURL(ctx, c.Endpoint)
			if err != nil {
				return nil, err
			}
			var last error
			for _, address := range validated.DialAddresses() {
				conn, err := dialer.DialContext(ctx, network, address)
				if err == nil {
					return conn, nil
				}
				last = err
			}
			if last == nil {
				last = security.ErrEndpointDenied
			}
			return nil, last
		},
	}
	e.client = &http.Client{Transport: e.transport, Timeout: c.RequestTimeout, CheckRedirect: func(*http.Request, []*http.Request) error { return http.ErrUseLastResponse }}
	return e, strings.ToLower(u.Scheme), nil
}

// Length-prefixed raw bytes preserve even non-UTF8 authentication/header values.
// Only the digest enters resource identity; private TLS keys are never hashed.
func configScope(c Config, origin string) string {
	h := sha256.New()
	part := func(value string) {
		var size [8]byte
		binary.LittleEndian.PutUint64(size[:], uint64(len(value)))
		_, _ = h.Write(size[:])
		_, _ = h.Write([]byte(value))
	}
	for _, value := range []string{origin, c.Username, c.Password, c.BearerToken, string(c.FallbackPolicy), c.TLSConfig.ServerName} {
		part(value)
	}
	keys := make([]string, 0, len(c.Headers))
	for key := range c.Headers {
		keys = append(keys, key)
	}
	sort.Strings(keys)
	part(strconv.Itoa(len(keys)))
	for _, key := range keys {
		part(key)
		part(c.Headers[key])
	}
	part(strconv.Itoa(len(c.TLSConfig.Certificates)))
	for _, certificate := range c.TLSConfig.Certificates {
		part(strconv.Itoa(len(certificate.Certificate)))
		for _, der := range certificate.Certificate {
			part(string(der))
		}
	}
	return hex.EncodeToString(h.Sum(nil))
}

func headerName(s string) bool {
	if s == "" {
		return false
	}
	for _, c := range s {
		if !(c >= '0' && c <= '9' || c >= 'a' && c <= 'z' || c >= 'A' && c <= 'Z' || strings.ContainsRune("!#$%&'*+-.^_`|~", c)) {
			return false
		}
	}
	return true
}
func headerValue(s string) bool {
	for _, c := range s {
		if c == 127 || c < 32 && c != '\t' {
			return false
		}
	}
	return true
}
func forbiddenHeader(name string) bool {
	switch name {
	case "Host", "Authorization", "Proxy-Authorization", "Range", "If-Match", "If-None-Match", "If-Modified-Since", "If-Unmodified-Since", "If-Range", "Accept-Encoding", "Connection", "Proxy-Connection", "Keep-Alive", "Transfer-Encoding", "Te", "Trailer", "Upgrade", "Content-Length":
		return true
	}
	return false
}

type requestError struct {
	uri   vfs.URI
	cause error
}

func (e *requestError) Error() string {
	reason := "request failed"
	switch {
	case errors.Is(e.cause, context.Canceled):
		reason = "request canceled"
	case errors.Is(e.cause, context.DeadlineExceeded):
		reason = "request timeout"
	case errors.Is(e.cause, vfs.ErrObjectChanged):
		reason = "object changed"
	case errors.Is(e.cause, vfs.ErrRangeUnsupported):
		reason = "stable Range unavailable"
	case errors.Is(e.cause, vfs.ErrCredential):
		reason = "authentication failed"
	case errors.Is(e.cause, ErrEncodingUnsupported):
		reason = "unsupported Content-Encoding"
	case errors.Is(e.cause, ErrProtocol):
		reason = "invalid response"
	case errors.Is(e.cause, security.ErrEndpointDenied):
		reason = "endpoint denied"
	}
	return fmt.Sprintf("http: %s for %s", reason, e.uri.String())
}
func (e *requestError) Unwrap() error { return e.cause }
func safeError(u vfs.URI, err error) error {
	var urlErr *url.Error
	if errors.As(err, &urlErr) {
		err = urlErr.Err
	}
	return &requestError{uri: vfs.URI{Scheme: u.Scheme, Host: u.Host, Path: u.Path, RawPath: u.RawPath}, cause: err}
}
