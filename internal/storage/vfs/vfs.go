package vfs

import (
    "context"
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
    Scheme string
    Host   string
    Path   string
    Raw    string
}

func ParseURI(raw string) (URI, error) {
    u, err := url.Parse(raw)
    if err != nil { return URI{}, fmt.Errorf("vfs: parse uri: %w", err) }
    if u.Scheme == "" { return URI{}, fmt.Errorf("vfs: uri has no scheme: %q", raw) }
    scheme := strings.ToLower(u.Scheme)
    host := u.Host
    if scheme == "s3" { host = strings.ToLower(host) }
    clean := path.Clean("/" + strings.TrimPrefix(u.EscapedPath(), "/"))
    if clean == "/." { clean = "/" }
    clean, err = url.PathUnescape(clean)
    if err != nil { return URI{}, fmt.Errorf("vfs: unescape path: %w", err) }
    c := URI{Scheme: scheme, Host: host, Path: clean}
    c.Raw = c.String()
    return c, nil
}

func (u URI) String() string {
    p := u.Path
    if p == "" { p = "/" }
    return (&url.URL{Scheme: strings.ToLower(u.Scheme), Host: u.Host, Path: p}).String()
}

func (u URI) ObjectKey() string { return strings.TrimPrefix(u.Path, "/") }

type ObjectIdentity struct {
    ETag       string
    VersionID  string
    Size       int64
    ModifiedAt time.Time
}

func (i ObjectIdentity) Stable() bool { return i.VersionID != "" || i.ETag != "" }

func (i ObjectIdentity) Matches(other ObjectIdentity) bool {
    if i.VersionID != "" || other.VersionID != "" { return i.VersionID != "" && i.VersionID == other.VersionID }
    if i.ETag != "" || other.ETag != "" { return i.ETag != "" && i.ETag == other.ETag && (i.Size == 0 || other.Size == 0 || i.Size == other.Size) }
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

type RangeRequest struct {
    URI      URI
    Offset   int64
    Length   int64
    Expected *ObjectIdentity
    Tag      memory.Tag
}

func (r RangeRequest) Validate() error {
    if r.Offset < 0 { return fmt.Errorf("vfs: negative range offset %d", r.Offset) }
    if r.Length <= 0 { return fmt.Errorf("vfs: non-positive range length %d", r.Length) }
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
    List(context.Context, URI, string) ([]ListEntry, error)
    ReadRange(context.Context, RangeRequest, *memory.Scope, *memory.BufferManager) (RangeResult, error)
}

type Registry struct { drivers map[string]Driver }
func NewRegistry(ds ...Driver) (*Registry,error) {
    r:=&Registry{drivers:make(map[string]Driver,len(ds))}
    for _,d:=range ds { if d==nil{return nil,errors.New("vfs: nil driver")}; s:=strings.ToLower(d.Scheme()); if s==""{return nil,errors.New("vfs: empty driver scheme")}; if _,ok:=r.drivers[s];ok{return nil,fmt.Errorf("vfs: duplicate driver %s",s)}; r.drivers[s]=d }
    return r,nil
}
func (r *Registry) DriverFor(u URI)(Driver,error){d,ok:=r.drivers[strings.ToLower(u.Scheme)];if !ok{return nil,fmt.Errorf("%w: %s",ErrUnsupportedScheme,u.Scheme)};return d,nil}
func (r *Registry) EffectiveCapabilities(ctx context.Context,u URI)(CapabilitySet,error){d,err:=r.DriverFor(u);if err!=nil{return 0,err};p,err:=d.Probe(ctx);if err!=nil{return 0,err};return ResolveEffectiveCapabilities(d.StaticCapabilities(),p)}
