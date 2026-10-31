package parquet

import (
	"context"
	"errors"
	"fmt"
	"sort"
	"strings"
	"testing"

	"github.com/steamwo/fssql/internal/core/memory"
	"github.com/steamwo/fssql/internal/storage/fileset"
	"github.com/steamwo/fssql/internal/storage/vfs"
)

type bindObject struct {
	uri      vfs.URI
	data     []byte
	identity vfs.ObjectIdentity
}

type bindDriver struct {
	objects map[string]bindObject
	ranges  []vfs.RangeRequest
}

func newBindDriver(t *testing.T, objects map[string][]byte) *bindDriver {
	t.Helper()
	d := &bindDriver{objects: make(map[string]bindObject, len(objects))}
	for raw, data := range objects {
		u, err := vfs.ParseURI(raw)
		if err != nil {
			t.Fatal(err)
		}
		etag := "etag-" + strings.TrimSuffix(strings.TrimPrefix(u.Path, "/"), ".parquet")
		d.objects[u.String()] = bindObject{uri: u, data: append([]byte(nil), data...), identity: vfs.ObjectIdentity{ETag: etag, Size: int64(len(data))}}
	}
	return d
}

func (d *bindDriver) Scheme() string { return "mem" }
func (d *bindDriver) StaticCapabilities() vfs.CapabilitySet {
	return vfs.Capabilities(vfs.RandomRead, vfs.ConditionalRead, vfs.StableETag, vfs.ServerSideList)
}
func (d *bindDriver) Probe(context.Context) (vfs.ProbeResult, error) {
	return vfs.ProbeResult{Supported: d.StaticCapabilities(), Endpoint: "mem://x", CredentialScope: "x", SecurityProfile: "test"}, nil
}
func (d *bindDriver) Stat(_ context.Context, u vfs.URI) (vfs.ObjectInfo, error) {
	obj, ok := d.objects[u.String()]
	if !ok {
		return vfs.ObjectInfo{}, errors.New("not found")
	}
	return vfs.ObjectInfo{URI: obj.uri, Size: int64(len(obj.data)), Identity: obj.identity}, nil
}
func (d *bindDriver) List(_ context.Context, base vfs.URI, opts vfs.ListOptions) (vfs.ListPage, error) {
	keys := make([]string, 0, len(d.objects))
	for key := range d.objects {
		keys = append(keys, key)
	}
	sort.Strings(keys)
	page := vfs.ListPage{}
	for _, key := range keys {
		obj := d.objects[key]
		if obj.uri.Host != base.Host || !strings.HasPrefix(obj.uri.ObjectKey(), opts.Prefix) {
			continue
		}
		page.Entries = append(page.Entries, vfs.ListEntry{URI: obj.uri, Size: int64(len(obj.data)), Identity: obj.identity})
	}
	return page, nil
}
func (d *bindDriver) ReadRange(_ context.Context, r vfs.RangeRequest, scope *memory.Scope, bm *memory.BufferManager) (vfs.RangeResult, error) {
	obj, ok := d.objects[r.URI.String()]
	if !ok {
		return vfs.RangeResult{}, errors.New("not found")
	}
	if r.Expected != nil && !r.Expected.Matches(obj.identity) {
		return vfs.RangeResult{}, vfs.ErrObjectChanged
	}
	if r.Offset < 0 || r.Length <= 0 || r.Offset+r.Length > int64(len(obj.data)) {
		return vfs.RangeResult{}, fmt.Errorf("bad range")
	}
	d.ranges = append(d.ranges, r)
	buf, err := bm.Allocate(scope, r.Tag, int(r.Length))
	if err != nil {
		return vfs.RangeResult{}, err
	}
	copy(buf.Bytes(), obj.data[r.Offset:r.Offset+r.Length])
	return vfs.RangeResult{Buffer: buf, Identity: obj.identity}, nil
}

func metadataWithField(name string) []byte {
	w := &compactWriter{}
	w.i32Field(1, 1)
	w.listHeader(2, ctStruct, 2)
	w.b = appendStruct(w.b, func(s *compactWriter) { s.strField(4, "schema"); s.i32Field(5, 1) })
	w.b = appendStruct(w.b, func(s *compactWriter) { s.i32Field(1, int32(TypeInt64)); s.i32Field(3, int32(Required)); s.strField(4, name) })
	w.i64Field(3, 0)
	w.listHeader(4, ctStruct, 0)
	w.stop()
	return w.b
}

func newTestBinder(t *testing.T, d *bindDriver) (*SourceBinder, *MetadataCache, *memory.Scope) {
	t.Helper()
	registry, err := vfs.NewRegistry(d)
	if err != nil {
		t.Fatal(err)
	}
	cache, err := NewMetadataCache(16)
	if err != nil {
		t.Fatal(err)
	}
	mm := memory.NewManager(1 << 20)
	scope := mm.NewScope()
	binder, err := NewSourceBinder(registry, cache, scope, memory.NewBufferManager(nil))
	if err != nil {
		t.Fatal(err)
	}
	return binder, cache, scope
}

func TestSourceBinderBoundedUsesSingleSchemaFileAndCache(t *testing.T) {
	a := parquetFile(metadataWithField("id"))
	b := parquetFile(metadataWithField("other"))
	d := newBindDriver(t, map[string][]byte{
		"mem://x/a.parquet": a,
		"mem://x/b.parquet": b,
	})
	binder, cache, scope := newTestBinder(t, d)

	binding, err := binder.Bind(context.Background(), "mem://x/*.parquet", SourceBindOptions{FileSet: fileset.PlanOptions{MaxFiles: 10}})
	if err != nil {
		t.Fatal(err)
	}
	if binding.MatchedFiles() != 2 || binding.SchemaFiles() != 1 || binding.Fingerprint() == "" {
		t.Fatalf("binding matched=%d schemaFiles=%d fingerprint=%q", binding.MatchedFiles(), binding.SchemaFiles(), binding.Fingerprint())
	}
	fields := binding.Schema().Fields
	if len(fields) != 1 || fields[0].Name != "id" {
		t.Fatalf("fields=%+v", fields)
	}
	if len(d.ranges) != 2 {
		t.Fatalf("bounded bind ranges=%d, want exactly one footer discovery", len(d.ranges))
	}
	for _, r := range d.ranges {
		if r.URI.ObjectKey() != "a.parquet" {
			t.Fatalf("bounded bind read non-leading schema file: %+v", r)
		}
	}

	before := len(d.ranges)
	binding2, err := binder.Bind(context.Background(), "mem://x/*.parquet", SourceBindOptions{FileSet: fileset.PlanOptions{MaxFiles: 10}})
	if err != nil {
		t.Fatal(err)
	}
	if len(d.ranges) != before {
		t.Fatalf("cache hit performed extra footer reads: before=%d after=%d", before, len(d.ranges))
	}
	if !sameFields(binding.Schema().Fields, binding2.Schema().Fields) {
		t.Fatalf("cached binding schema changed: first=%+v second=%+v", binding.Schema().Fields, binding2.Schema().Fields)
	}
	if cache.Metrics().Hits == 0 {
		t.Fatalf("cache metrics=%+v", cache.Metrics())
	}
	if scope.Used() != 0 {
		t.Fatalf("managed memory leaked: %d", scope.Used())
	}
}

func TestSourceBinderFullValidationFindsMismatch(t *testing.T) {
	d := newBindDriver(t, map[string][]byte{
		"mem://x/a.parquet": parquetFile(metadataWithField("id")),
		"mem://x/b.parquet": parquetFile(metadataWithField("other")),
	})
	binder, _, scope := newTestBinder(t, d)
	_, err := binder.Bind(context.Background(), "mem://x/*.parquet", SourceBindOptions{ValidationMode: SchemaValidationFull})
	if err == nil || !strings.Contains(err.Error(), "schema mismatch") {
		t.Fatalf("error=%v, want schema mismatch", err)
	}
	if len(d.ranges) != 4 {
		t.Fatalf("full validation ranges=%d, want two footer discoveries", len(d.ranges))
	}
	if scope.Used() != 0 {
		t.Fatalf("managed memory leaked: %d", scope.Used())
	}
}

func TestSourceBinderUnionByNameReadsAllSchemas(t *testing.T) {
	d := newBindDriver(t, map[string][]byte{
		"mem://x/a.parquet": parquetFile(metadataWithField("id")),
		"mem://x/b.parquet": parquetFile(metadataWithField("name")),
	})
	binder, _, scope := newTestBinder(t, d)
	binding, err := binder.Bind(context.Background(), "mem://x/*.parquet", SourceBindOptions{UnionByName: true})
	if err != nil {
		t.Fatal(err)
	}
	fields := binding.Schema().Fields
	if binding.SchemaFiles() != 2 || len(fields) != 2 || fields[0].Name != "id" || fields[1].Name != "name" {
		t.Fatalf("binding schemaFiles=%d fields=%+v", binding.SchemaFiles(), fields)
	}
	if !binding.Schema().UnionByName {
		t.Fatal("resolved schema should record union_by_name")
	}
	if len(d.ranges) != 4 {
		t.Fatalf("union_by_name ranges=%d, want all footer metadata", len(d.ranges))
	}
	if scope.Used() != 0 {
		t.Fatalf("managed memory leaked: %d", scope.Used())
	}
}
