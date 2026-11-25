package vfs

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"fmt"
	"net/url"
	"path"
	"strings"
	"time"

	"github.com/steamwo/fssql/internal/core/memory"
)

var (
	ErrUnsupportedScheme = errors.New("vfs: unsupported scheme")
	ErrRangeUnsupported  = errors.New("vfs: exact range read unsupported")
	ErrObjectChanged     = errors.New("vfs: object identity changed")
	ErrShortRead         = errors.New("vfs: short range read")
	ErrCredential        = errors.New("vfs: credential failure")
)

type URI struct {
	Scheme     string
	Host       string
	Path       string
	Raw        string
	RawPath    string
	RawQuery   string
	ForceQuery bool
	// ViewID routes reads within one query; ViewKey identifies its representation.
	ViewID  string
	ViewKey string
}

func ParseURI(raw string) (URI, error) {
	u, err := url.Parse(raw)
	if err != nil {
		// url.Parse errors may contain the complete signed URL.
		return URI{}, errors.New("vfs: invalid uri")
	}
	if u.Scheme == "" {
		return URI{}, errors.New("vfs: uri has no scheme")
	}
	scheme := strings.ToLower(u.Scheme)
	if scheme == "http" || scheme == "https" {
		if u.Host == "" || u.Opaque != "" || u.User != nil || strings.Contains(raw, "#") {
			return URI{}, errors.New("vfs: HTTP uri requires a host and forbids userinfo and fragment")
		}
		c := URI{Scheme: scheme, Host: u.Host, Path: u.Path, RawPath: u.RawPath, RawQuery: u.RawQuery, ForceQuery: u.ForceQuery}
		c.Raw = c.String()
		return c, nil
	}
	host := u.Host
	if scheme == "s3" {
		host = strings.ToLower(host)
	}
	clean := path.Clean("/" + strings.TrimPrefix(u.EscapedPath(), "/"))
	if clean == "/." {
		clean = "/"
	}
	clean, err = url.PathUnescape(clean)
	if err != nil {
		return URI{}, fmt.Errorf("vfs: unescape path: %w", err)
	}
	c := URI{Scheme: scheme, Host: host, Path: clean}
	c.Raw = c.String()
	return c, nil
}

func (u URI) String() string {
	if u.IsHTTP() {
		display := u.url()
		parts := strings.Split(u.RawQuery, "&")
		for i, part := range parts {
			if part == "" {
				continue
			}
			key, _, ok := strings.Cut(part, "=")
			if ok {
				parts[i] = key + "=REDACTED"
			} else {
				parts[i] = "REDACTED"
			}
		}
		display.RawQuery = strings.Join(parts, "&")
		return display.String()
	}
	return u.url().String()
}

func (u URI) url() *url.URL {
	p := u.Path
	if p == "" && !u.IsHTTP() {
		p = "/"
	}
	return &url.URL{Scheme: strings.ToLower(u.Scheme), Host: u.Host, Path: p, RawPath: u.RawPath, RawQuery: u.RawQuery, ForceQuery: u.ForceQuery}
}

// RequestURL is the lossless transport address. Never use it for diagnostics.
func (u URI) RequestURL() string { return u.url().String() }
func (u URI) GoString() string   { return u.String() }
func (u URI) IsHTTP() bool {
	return strings.EqualFold(u.Scheme, "http") || strings.EqualFold(u.Scheme, "https")
}
func (u URI) IdentityKey() string {
	sum := sha256.Sum256([]byte(u.RequestURL()))
	return hex.EncodeToString(sum[:])
}

// ReadKey keeps different logical views out of the same reader window.
func (u URI) ReadKey() string {
	if u.ViewID != "" {
		return u.ViewID
	}
	return u.IdentityKey()
}
func (u URI) HasGlob() bool {
	if u.IsHTTP() {
		return strings.ContainsAny(u.url().EscapedPath(), "*[")
	}
	return strings.ContainsAny(u.ObjectKey(), "*?[")
}

func (u URI) ObjectKey() string { return strings.TrimPrefix(u.Path, "/") }

type ObjectIdentity struct {
	ETag       string
	VersionID  string
	SnapshotID string
	Size       int64
	ModifiedAt time.Time
}

func (i ObjectIdentity) Stable() bool { return i.VersionID != "" || i.ETag != "" }

func (i ObjectIdentity) Matches(other ObjectIdentity) bool {
	if i.SnapshotID != "" || other.SnapshotID != "" {
		return i.SnapshotID != "" && i.SnapshotID == other.SnapshotID && i.Size == other.Size
	}
	if i.VersionID != "" || other.VersionID != "" {
		return i.VersionID != "" && i.VersionID == other.VersionID
	}
	if i.ETag != "" || other.ETag != "" {
		return i.ETag != "" && i.ETag == other.ETag && (i.Size == 0 || other.Size == 0 || i.Size == other.Size)
	}
	return i.Size == other.Size && i.ModifiedAt.Equal(other.ModifiedAt)
}

type ObjectInfo struct {
	URI      URI
	Size     int64
	Identity ObjectIdentity
}

type ListEntry struct {
	URI      URI
	Size     int64
	Identity ObjectIdentity
}

type ListOptions struct {
	Prefix            string
	ContinuationToken string
	MaxKeys           int
}

type ListPage struct {
	Entries               []ListEntry
	NextContinuationToken string
	Truncated             bool
}

// ListWalker optionally visits a prefix in a single listing. A visit error
// stops enumeration immediately. Entry order is unspecified.
type ListWalker interface {
	WalkList(ctx context.Context, uri URI, prefix string, visit func(ListEntry) error) error
}

type RangeRequest struct {
	URI      URI
	Offset   int64
	Length   int64
	Expected *ObjectIdentity
	Tag      memory.Tag
}

func (r RangeRequest) Validate() error {
	if r.Offset < 0 {
		return fmt.Errorf("vfs: negative range offset %d", r.Offset)
	}
	if r.Length <= 0 {
		return fmt.Errorf("vfs: non-positive range length %d", r.Length)
	}
	return nil
}

type RangeResult struct {
	Buffer   *memory.Buffer
	Identity ObjectIdentity
}

type Driver interface {
	Scheme() string
	StaticCapabilities() CapabilitySet
	Probe(context.Context) (ProbeResult, error)
	Stat(context.Context, URI) (ObjectInfo, error)
	List(context.Context, URI, ListOptions) (ListPage, error)
	ReadRange(context.Context, RangeRequest, *memory.Scope, *memory.BufferManager) (RangeResult, error)
}

type Registry struct {
	drivers map[string]Driver
	views   ObjectViewResolver
}

func NewRegistry(ds ...Driver) (*Registry, error) {
	r := &Registry{drivers: make(map[string]Driver, len(ds))}
	for _, d := range ds {
		if d == nil {
			return nil, errors.New("vfs: nil driver")
		}
		s := strings.ToLower(d.Scheme())
		if s == "" {
			return nil, errors.New("vfs: empty driver scheme")
		}
		if _, ok := r.drivers[s]; ok {
			return nil, fmt.Errorf("vfs: duplicate driver %s", s)
		}
		r.drivers[s] = d
	}
	return r, nil
}

func (r *Registry) DriverFor(u URI) (Driver, error) {
	d, ok := r.drivers[strings.ToLower(u.Scheme)]
	if !ok {
		return nil, fmt.Errorf("%w: %s", ErrUnsupportedScheme, u.Scheme)
	}
	return d, nil
}

func (r *Registry) EffectiveCapabilities(ctx context.Context, u URI) (CapabilitySet, error) {
	if u.ViewID != "" && r.views != nil {
		obj, err := r.views.Lookup(u)
		return obj.Capabilities, err
	}
	d, err := r.DriverFor(u)
	if err != nil {
		return 0, err
	}
	p, err := d.Probe(ctx)
	if err != nil {
		return 0, err
	}
	return ResolveEffectiveCapabilities(d.StaticCapabilities(), p)
}
