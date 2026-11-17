// Package objectview owns query-local logical bytes and their temporary storage.
package objectview

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"fmt"
	"io"
	"sync"
	"sync/atomic"

	"github.com/steamwo/fssql/internal/core/memory"
	"github.com/steamwo/fssql/internal/query"
	storagetemp "github.com/steamwo/fssql/internal/storage/temp"
	"github.com/steamwo/fssql/internal/storage/vfs"
)

const bufferBytes = 64 << 10

var nextResolver atomic.Uint64

type flight struct {
	done   chan struct{}
	object vfs.LogicalObject
	err    error
}

type Resolver struct {
	q        *query.Context
	ctx      context.Context
	cancel   context.CancelFunc
	physical *vfs.Registry
	buffers  *memory.BufferManager
	id       uint64
	mu       sync.Mutex
	closed   bool
	flights  map[string]*flight
	objects  map[string]vfs.LogicalObject
	// Physical identities remain separate from materialized content identities.
	sources map[string]vfs.ObjectIdentity
	closers []io.Closer
	active  sync.WaitGroup
}

func New(q *query.Context, physical *vfs.Registry, buffers *memory.BufferManager) (*Resolver, error) {
	if q == nil || q.Memory == nil || physical == nil || buffers == nil || q.Closed() {
		return nil, errors.New("objectview: requires a live query, registry and managed buffers")
	}
	ctx, cancel := context.WithCancel(q.Context())
	r := &Resolver{q: q, ctx: ctx, cancel: cancel, physical: physical, buffers: buffers, id: nextResolver.Add(1), flights: make(map[string]*flight), objects: make(map[string]vfs.LogicalObject), sources: make(map[string]vfs.ObjectIdentity)}
	if err := q.AddCleanup(r.Close); err != nil {
		_ = r.Close()
		return nil, err
	}
	return r, nil
}

func (r *Resolver) Registry() *vfs.Registry { return r.physical.WithObjectViews(r) }

func (r *Resolver) Lookup(u vfs.URI) (vfs.LogicalObject, error) {
	r.mu.Lock()
	defer r.mu.Unlock()
	if r.closed {
		return vfs.LogicalObject{}, errors.New("objectview: query is closed")
	}
	obj, ok := r.objects[u.ViewID]
	if !ok || obj.URI.IdentityKey() != u.IdentityKey() || obj.URI.ViewKey != u.ViewKey {
		return vfs.LogicalObject{}, errors.New("objectview: unknown logical view")
	}
	return obj, nil
}

func (r *Resolver) Resolve(ctx context.Context, u vfs.URI, spec vfs.ObjectViewSpec) (vfs.LogicalObject, error) {
	if err := ctx.Err(); err != nil {
		return vfs.LogicalObject{}, err
	}
	if err := r.q.Context().Err(); err != nil {
		return vfs.LogicalObject{}, err
	}
	if u.ViewID != "" {
		return r.Lookup(u)
	}
	if spec.FileCompression == "" {
		spec.FileCompression = "none"
	}
	mode, err := vfs.ParseFileCompression(spec.FileCompression)
	if err != nil {
		return vfs.LogicalObject{}, err
	}
	spec.FileCompression = mode
	key := fmt.Sprintf("%s\x00%s\x00%t", u.IdentityKey(), spec.FileCompression, spec.Materialize)
	r.mu.Lock()
	if r.closed {
		r.mu.Unlock()
		return vfs.LogicalObject{}, errors.New("objectview: query is closed")
	}
	if f := r.flights[key]; f != nil {
		r.mu.Unlock()
		select {
		case <-ctx.Done():
			return vfs.LogicalObject{}, ctx.Err()
		case <-r.q.Context().Done():
			return vfs.LogicalObject{}, r.q.Context().Err()
		case <-f.done:
			return f.object, f.err
		}
	}
	f := &flight{done: make(chan struct{})}
	r.flights[key] = f
	r.active.Add(1)
	r.mu.Unlock()
	defer r.active.Done()
	buildCtx, cancel := context.WithCancel(ctx)
	stop := context.AfterFunc(r.ctx, cancel)
	defer stop()
	defer cancel()
	obj, source, closer, err := r.build(buildCtx, u, spec, key)
	r.mu.Lock()
	if closer != nil {
		r.closers = append(r.closers, closer)
	}
	if err == nil {
		obj.URI.ViewID = fmt.Sprintf("%d:%d", r.id, len(r.objects)+1)
		r.objects[obj.URI.ViewID] = obj
		r.sources[obj.URI.ViewID] = source
	}
	f.object, f.err = obj, err
	close(f.done)
	r.mu.Unlock()
	return obj, err
}

func (r *Resolver) build(ctx context.Context, u vfs.URI, spec vfs.ObjectViewSpec, key string) (vfs.LogicalObject, vfs.ObjectIdentity, io.Closer, error) {
	var obj vfs.LogicalObject
	d, err := r.physical.DriverFor(u)
	if err != nil {
		return obj, vfs.ObjectIdentity{}, nil, err
	}
	var source vfs.ObjectSource
	verifySource := false
	if opener, ok := d.(vfs.ObjectSourceOpener); ok {
		source, err = opener.OpenObject(ctx, u)
		if err != nil {
			return obj, vfs.ObjectIdentity{}, nil, err
		}
	} else {
		info, err := d.Stat(ctx, u)
		if err != nil {
			return obj, vfs.ObjectIdentity{}, nil, err
		}
		probe, err := d.Probe(ctx)
		if err != nil {
			return obj, info.Identity, nil, err
		}
		caps, err := vfs.ResolveEffectiveCapabilities(d.StaticCapabilities(), probe)
		if err != nil {
			return obj, info.Identity, nil, err
		}
		source = vfs.ObjectSource{Info: info, Capabilities: caps, Reader: &physicalReader{uri: u, identity: info.Identity, reader: d}, ScopeKey: fmt.Sprintf("%q\x00%q\x00%q\x00%q", probe.Endpoint, probe.CredentialScope, probe.CredentialVersion, probe.SecurityProfile)}
		verifySource = true
	}
	sum := sha256.Sum256([]byte(key + "\x00" + source.ScopeKey + "\x00" + source.ContentEncoding))
	u.ViewKey = hex.EncodeToString(sum[:])
	identity := source.Info.Identity
	obj = vfs.LogicalObject{URI: u, Size: source.Info.Size, Identity: identity, SourceIdentity: identity, Capabilities: source.Capabilities, Reader: source.Reader}
	random := source.Reader != nil && source.Info.Size >= 0 && source.Capabilities.Has(vfs.RandomRead)
	if source.Stream == nil && (source.ContentEncoding == "" || source.ContentEncoding == "identity") && random && !spec.Materialize && spec.FileCompression == "none" {
		return obj, identity, nil, nil
	}
	input := source.Stream
	if input == nil {
		if random {
			input = &rangeStream{ctx: ctx, driver: &sourceDriver{Driver: d, reader: source.Reader}, uri: u, identity: identity, size: source.Info.Size, scope: r.q.Memory, buffers: r.buffers}
		} else if opener, ok := d.(vfs.SequentialReadOpener); ok {
			input, err = opener.OpenRead(ctx, u, &identity)
			if err != nil {
				return vfs.LogicalObject{}, identity, nil, err
			}
		}
	}
	if input == nil {
		return vfs.LogicalObject{}, identity, nil, vfs.ErrRangeUnsupported
	}
	p, err := r.prepareInput(ctx, input, source.Info.Size, source.ContentEncoding, spec.FileCompression)
	if err != nil {
		return vfs.LogicalObject{}, identity, nil, err
	}
	if !p.decoded && random && source.Stream == nil && !spec.Materialize {
		if err := p.Close(); err != nil {
			return vfs.LogicalObject{}, identity, nil, err
		}
		return obj, identity, nil, nil
	}
	size := source.Info.Size
	if p.decoded {
		size = -1
	}
	file, logicalIdentity, err := r.materializeInput(ctx, p, size)
	if err != nil {
		return vfs.LogicalObject{}, identity, nil, err
	}
	if verifySource {
		current, checkErr := d.Stat(ctx, u)
		if checkErr == nil && !identity.Matches(current.Identity) {
			checkErr = vfs.ErrObjectChanged
		}
		if checkErr != nil {
			return vfs.LogicalObject{}, identity, nil, errors.Join(checkErr, file.Remove())
		}
	}
	obj.Size, obj.Identity = logicalIdentity.Size, logicalIdentity
	obj.Capabilities = vfs.Capabilities(vfs.RandomRead, vfs.ParallelRandomRead)
	obj.Reader = &tempReader{file: file, identity: logicalIdentity}
	return obj, identity, removeFile{file}, nil
}

func (r *Resolver) materializeInput(ctx context.Context, input io.ReadCloser, size int64) (_ *storagetemp.File, _ vfs.ObjectIdentity, resultErr error) {
	canceller, canCancel := input.(interface{ Cancel() })
	input = &onceReadCloser{ReadCloser: input}
	stop := context.AfterFunc(ctx, func() {
		if canCancel {
			canceller.Cancel()
		} else {
			_ = input.Close()
		}
	})
	defer stop()
	defer func() { resultErr = errors.Join(resultErr, input.Close()) }()
	if r.q.Temp == nil {
		return nil, vfs.ObjectIdentity{}, errors.New("objectview: materialization requires query temporary storage")
	}
	file, err := r.q.Temp.CreateFile()
	if err != nil {
		return nil, vfs.ObjectIdentity{}, err
	}
	defer func() {
		if resultErr != nil {
			resultErr = errors.Join(resultErr, file.Remove())
		}
	}()
	buf, err := r.buffers.Allocate(r.q.Memory, memory.TagReadAhead, bufferBytes)
	if err != nil {
		return nil, vfs.ObjectIdentity{}, err
	}
	defer buf.Release()
	h := sha256.New()
	for {
		if err := ctx.Err(); err != nil {
			return nil, vfs.ObjectIdentity{}, err
		}
		n, readErr := input.Read(buf.Bytes())
		if n < 0 || n > buf.Len() {
			return nil, vfs.ObjectIdentity{}, errors.New("objectview: invalid sequential read count")
		}
		if n > 0 {
			if _, err := file.WriteContext(ctx, buf.Bytes()[:n]); err != nil {
				return nil, vfs.ObjectIdentity{}, err
			}
			_, _ = h.Write(buf.Bytes()[:n])
		}
		if readErr == io.EOF {
			break
		}
		if readErr != nil {
			return nil, vfs.ObjectIdentity{}, readErr
		}
		if n == 0 {
			return nil, vfs.ObjectIdentity{}, io.ErrNoProgress
		}
	}
	if err := input.Close(); err != nil {
		return nil, vfs.ObjectIdentity{}, err
	}
	if err := ctx.Err(); err != nil {
		return nil, vfs.ObjectIdentity{}, err
	}
	if size >= 0 && file.Size() != size {
		return nil, vfs.ObjectIdentity{}, vfs.ErrShortRead
	}
	return file, vfs.ObjectIdentity{Size: file.Size(), SnapshotID: "sha256:" + hex.EncodeToString(h.Sum(nil))}, nil
}

type sourceDriver struct {
	vfs.Driver
	reader vfs.LogicalObjectReader
}

func (d *sourceDriver) ReadRange(ctx context.Context, req vfs.RangeRequest, s *memory.Scope, bm *memory.BufferManager) (vfs.RangeResult, error) {
	return d.reader.ReadRange(ctx, req, s, bm)
}

// Close waits for initialization, then releases readers before query Temp/Memory.
func (r *Resolver) Close() error {
	r.cancel()
	r.mu.Lock()
	r.closed = true
	r.mu.Unlock()
	r.active.Wait()
	r.mu.Lock()
	defer r.mu.Unlock()
	var errs []error
	var pending []io.Closer
	for _, c := range r.closers {
		if err := c.Close(); err != nil {
			errs = append(errs, err)
			pending = append(pending, c)
		}
	}
	r.closers = pending
	r.objects = nil
	r.sources = nil
	r.flights = nil
	return errors.Join(errs...)
}

type onceReadCloser struct {
	io.ReadCloser
	once sync.Once
	err  error
}
type removeFile struct{ file *storagetemp.File }

func (c removeFile) Close() error { return c.file.Remove() }
func (r *onceReadCloser) Close() error {
	r.once.Do(func() { r.err = r.ReadCloser.Close() })
	return r.err
}

type physicalReader struct {
	uri      vfs.URI
	identity vfs.ObjectIdentity
	reader   vfs.LogicalObjectReader
}

// Physical scan sessions stay lazy; enumerating a FileSet must not open one
// handle per file. Each consumer owns its session, never the shared view.
func (r *physicalReader) OpenRangeReader(ctx context.Context, _ vfs.URI, expected *vfs.ObjectIdentity) (vfs.RangeReader, error) {
	if expected != nil && !expected.Matches(r.identity) {
		return nil, vfs.ErrObjectChanged
	}
	if opener, ok := r.reader.(vfs.RangeReaderOpener); ok {
		u := r.uri
		u.ViewID, u.ViewKey = "", ""
		session, err := opener.OpenRangeReader(ctx, u, &r.identity)
		if err != nil {
			return nil, err
		}
		return &physicalSession{physicalReader: physicalReader{uri: u, identity: r.identity, reader: session}, closer: session}, nil
	}
	return &physicalSession{physicalReader: *r}, nil
}

type physicalSession struct {
	physicalReader
	closer io.Closer
}

func (r *physicalSession) Close() error {
	if r.closer != nil {
		return r.closer.Close()
	}
	return nil
}

func (r *physicalReader) ReadRange(ctx context.Context, req vfs.RangeRequest, s *memory.Scope, bm *memory.BufferManager) (vfs.RangeResult, error) {
	if req.Expected != nil && !req.Expected.Matches(r.identity) {
		return vfs.RangeResult{}, vfs.ErrObjectChanged
	}
	req.URI, req.Expected = r.uri, &r.identity
	req.URI.ViewID, req.URI.ViewKey = "", ""
	return r.reader.ReadRange(ctx, req, s, bm)
}

type tempReader struct {
	file     *storagetemp.File
	identity vfs.ObjectIdentity
}

func (r *tempReader) ReadRange(ctx context.Context, req vfs.RangeRequest, s *memory.Scope, bm *memory.BufferManager) (vfs.RangeResult, error) {
	if err := ctx.Err(); err != nil {
		return vfs.RangeResult{}, err
	}
	if err := req.Validate(); err != nil {
		return vfs.RangeResult{}, err
	}
	if req.Expected != nil && !req.Expected.Matches(r.identity) {
		return vfs.RangeResult{}, vfs.ErrObjectChanged
	}
	if req.Length > r.identity.Size || req.Offset > r.identity.Size-req.Length {
		return vfs.RangeResult{}, vfs.ErrShortRead
	}
	if req.Length > int64(int(^uint(0)>>1)) {
		return vfs.RangeResult{}, errors.New("objectview: range exceeds addressable memory")
	}
	buf, err := bm.Allocate(s, req.Tag, int(req.Length))
	if err != nil {
		return vfs.RangeResult{}, err
	}
	n, err := r.file.ReadAt(buf.Bytes(), req.Offset)
	if err == nil {
		err = ctx.Err()
	}
	if err != nil || int64(n) != req.Length {
		buf.Release()
		if err != nil {
			return vfs.RangeResult{}, err
		}
		return vfs.RangeResult{}, vfs.ErrShortRead
	}
	return vfs.RangeResult{Buffer: buf, Identity: r.identity}, nil
}

type rangeStream struct {
	ctx          context.Context
	driver       vfs.Driver
	uri          vfs.URI
	identity     vfs.ObjectIdentity
	offset, size int64
	scope        *memory.Scope
	buffers      *memory.BufferManager
}

func (r *rangeStream) Read(p []byte) (int, error) {
	if len(p) == 0 {
		return 0, nil
	}
	if err := r.ctx.Err(); err != nil {
		return 0, err
	}
	if r.offset == r.size {
		return 0, io.EOF
	}
	length := min(int64(len(p)), int64(bufferBytes), r.size-r.offset)
	result, err := r.driver.ReadRange(r.ctx, vfs.RangeRequest{URI: r.uri, Offset: r.offset, Length: length, Expected: &r.identity, Tag: memory.TagReadAhead}, r.scope, r.buffers)
	if err != nil {
		return 0, err
	}
	defer result.Buffer.Release()
	if int64(result.Buffer.Len()) != length {
		return 0, vfs.ErrShortRead
	}
	n := copy(p, result.Buffer.Bytes())
	r.offset += int64(n)
	return n, nil
}
func (*rangeStream) Close() error { return nil }
