package vfs

import (
	"context"
	"fmt"
	"io"
	"strings"

	"github.com/steamwo/fssql/internal/core/memory"
)

// ObjectViewSpec describes logical bytes independently of their input protocol.
type ObjectViewSpec struct {
	FileCompression string
	Materialize     bool
}

func ParseFileCompression(value string) (string, error) {
	value = strings.ToLower(strings.TrimSpace(value))
	switch value {
	case "", "auto", "none", "gzip", "zstd":
		return value, nil
	default:
		return "", fmt.Errorf("vfs: unsupported file compression %q", value)
	}
}

type LogicalObjectReader interface {
	ReadRange(context.Context, RangeRequest, *memory.Scope, *memory.BufferManager) (RangeResult, error)
}

type LogicalObject struct {
	URI            URI
	Size           int64
	Identity       ObjectIdentity
	SourceIdentity ObjectIdentity
	Capabilities   CapabilitySet
	Reader         LogicalObjectReader
}

type ObjectViewResolver interface {
	Resolve(context.Context, URI, ObjectViewSpec) (LogicalObject, error)
	Lookup(URI) (LogicalObject, error)
}

// SequentialReadOpener opens physical bytes with cancellation and identity checks.
// Advertising StreamingRead alone does not implement this interface.
type SequentialReadOpener interface {
	OpenRead(context.Context, URI, *ObjectIdentity) (io.ReadCloser, error)
}

// ObjectSourceOpener selects a physical representation once per query view.
// A non-nil Stream transfers ownership to the resolver, including the body of
// an initial Range request that returned a complete object.
type ObjectSourceOpener interface {
	OpenObject(context.Context, URI) (ObjectSource, error)
}

type ObjectSource struct {
	Info            ObjectInfo
	Capabilities    CapabilitySet
	Reader          LogicalObjectReader
	Stream          io.ReadCloser
	ContentEncoding string // HTTP representation coding, before file compression.
	ScopeKey        string // Digest of frozen endpoint configuration and representation.
}

func (r *Registry) HasObjectViews() bool { return r.views != nil }

func (r *Registry) ResolveObject(ctx context.Context, u URI, spec ObjectViewSpec) (LogicalObject, error) {
	if r.views != nil {
		return r.views.Resolve(ctx, u, spec)
	}
	if spec.Materialize || (spec.FileCompression != "" && spec.FileCompression != "none") {
		return LogicalObject{}, fmt.Errorf("vfs: object view requires a query resolver")
	}
	d, err := r.DriverFor(u)
	if err != nil {
		return LogicalObject{}, err
	}
	info, err := d.Stat(ctx, u)
	return LogicalObject{URI: info.URI, Size: info.Size, Identity: info.Identity, Reader: d}, err
}

// WithObjectViews leaves the physical registry intact and routes query reads by ViewID.
func (r *Registry) WithObjectViews(views ObjectViewResolver) *Registry {
	out := &Registry{drivers: make(map[string]Driver, len(r.drivers)), views: views}
	for scheme, d := range r.drivers {
		out.drivers[scheme] = &viewDriver{Driver: d, views: views}
	}
	return out
}

type viewDriver struct {
	Driver
	views ObjectViewResolver
}

func (d *viewDriver) Stat(ctx context.Context, u URI) (ObjectInfo, error) {
	if u.ViewID == "" {
		return d.Driver.Stat(ctx, u)
	}
	obj, err := d.views.Lookup(u)
	return ObjectInfo{URI: obj.URI, Size: obj.Size, Identity: obj.Identity}, err
}

func (d *viewDriver) ReadRange(ctx context.Context, req RangeRequest, s *memory.Scope, bm *memory.BufferManager) (RangeResult, error) {
	if req.URI.ViewID == "" {
		return d.Driver.ReadRange(ctx, req, s, bm)
	}
	obj, err := d.views.Lookup(req.URI)
	if err != nil {
		return RangeResult{}, err
	}
	return obj.Reader.ReadRange(ctx, req, s, bm)
}

// A scan may close its own session, but shared logical readers belong to the query.
func (d *viewDriver) OpenRangeReader(ctx context.Context, u URI, expected *ObjectIdentity) (RangeReader, error) {
	if u.ViewID == "" {
		if opener, ok := d.Driver.(RangeReaderOpener); ok {
			return opener.OpenRangeReader(ctx, u, expected)
		}
		return &borrowedRangeReader{reader: d.Driver}, nil
	}
	obj, err := d.views.Lookup(u)
	if err != nil {
		return nil, err
	}
	if expected != nil && !expected.Matches(obj.Identity) {
		return nil, ErrObjectChanged
	}
	if opener, ok := obj.Reader.(RangeReaderOpener); ok {
		return opener.OpenRangeReader(ctx, u, expected)
	}
	return &borrowedRangeReader{reader: obj.Reader}, nil
}

type borrowedRangeReader struct{ reader LogicalObjectReader }

func (r *borrowedRangeReader) ReadRange(ctx context.Context, req RangeRequest, s *memory.Scope, bm *memory.BufferManager) (RangeResult, error) {
	return r.reader.ReadRange(ctx, req, s, bm)
}
func (*borrowedRangeReader) Close() error { return nil }
