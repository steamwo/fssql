package json

import (
	"context"
	"fmt"
	"github.com/steamwo/fssql/internal/core/memory"
	"github.com/steamwo/fssql/internal/core/types"
	"github.com/steamwo/fssql/internal/storage/vfs"
	"sort"
	"strings"
	"sync"
	"testing"
)

func nestedSchema() []types.StructField {
	profile := types.NewStruct(types.StructField{Name: "name", Type: types.LogicalType{ID: types.Varchar}}, types.StructField{Name: "score", Type: types.LogicalType{ID: types.Double}})
	return []types.StructField{{Name: "id", Type: types.LogicalType{ID: types.BigInt}}, {Name: "profile", Type: profile}, {Name: "tags", Type: types.NewList(types.LogicalType{ID: types.Varchar})}}
}
func TestTypedNestedProjection(t *testing.T) {
	records := [][]byte{[]byte(`{"id":1,"profile":{"name":"alpha","score":1.5},"tags":["x","y"],"skip":{"a":1}}`), []byte(`{"id":2,"profile":{"name":"b\u0065ta","score":2.5},"tags":[]}`)}
	proj, _ := NewProjectedPathSet("id", "profile.name", "tags")
	m := memory.NewManager(1 << 20)
	s := m.NewScope()
	bm := memory.NewBufferManager(nil)
	c, err := DecodeRecords(records, nestedSchema(), proj, s, bm)
	if err != nil {
		t.Fatal(err)
	}
	if v, ok := c.Columns[0].Int64(1); !ok || v != 2 {
		t.Fatalf("id %d %v", v, ok)
	}
	pc := c.Columns[1].Children()
	if v, ok := pc[0].String(1); !ok || v != "beta" {
		t.Fatalf("name %q %v", v, ok)
	}
	if _, ok := pc[1].Float64(0); ok {
		t.Fatal("score should be unprojected/null")
	}
	e, ok := c.Columns[2].ListEntry(0)
	if !ok || e.Length != 2 {
		t.Fatalf("entry %+v %v", e, ok)
	}
	if v, ok := c.Columns[2].Children()[0].String(int(e.Offset)); !ok || v != "x" {
		t.Fatalf("tag %q %v", v, ok)
	}
	c.Release()
	if s.Used() != 0 {
		t.Fatalf("leak %d", s.Used())
	}
}
func TestInferNested(t *testing.T) {
	sc, err := InferRecords([][]byte{[]byte(`{"id":1,"v":1,"p":{"name":"a"},"xs":[1,2]}`), []byte(`{"id":2,"v":2.5,"p":{"name":"b","active":true},"xs":[3]}`)}, 10)
	if err != nil {
		t.Fatal(err)
	}
	if len(sc.Fields) != 4 || sc.Fields[1].Type.ID != types.Double || sc.Fields[2].Type.ID != types.Struct || sc.Fields[3].Type.ID != types.List {
		t.Fatalf("schema %#v", sc.Fields)
	}
}
func TestMalformedJSON(t *testing.T) {
	m := memory.NewManager(1 << 20)
	s := m.NewScope()
	bm := memory.NewBufferManager(nil)
	_, err := DecodeRecords([][]byte{[]byte(`{"id":1,}`)}, nestedSchema(), ProjectedPathSet{}, s, bm)
	if err == nil {
		t.Fatal("expected error")
	}
	if s.Used() != 0 {
		t.Fatalf("leak %d", s.Used())
	}
}
func TestAllocationFailureNoLeak(t *testing.T) {
	m := memory.NewManager(32)
	s := m.NewScope()
	bm := memory.NewBufferManager(nil)
	_, err := DecodeRecords([][]byte{[]byte(`{"id":1,"profile":{"name":"alpha"},"tags":["x"]}`)}, nestedSchema(), ProjectedPathSet{}, s, bm)
	if err == nil {
		t.Fatal("expected alloc failure")
	}
	if s.Used() != 0 {
		t.Fatalf("leak %d", s.Used())
	}
}

type memDriver struct {
	data []byte
	caps vfs.CapabilitySet
	uri  vfs.URI
}

func newMem(t *testing.T, data []byte, caps vfs.CapabilitySet) *memDriver {
	u, e := vfs.ParseURI("memjson://dataset/data.ndjson")
	if e != nil {
		t.Fatal(e)
	}
	return &memDriver{data: data, caps: caps, uri: u}
}
func (d *memDriver) Scheme() string                        { return "memjson" }
func (d *memDriver) StaticCapabilities() vfs.CapabilitySet { return d.caps }
func (d *memDriver) Probe(context.Context) (vfs.ProbeResult, error) {
	return vfs.ProbeResult{Supported: d.caps, Endpoint: "memjson://dataset", CredentialScope: "x", SecurityProfile: "x"}, nil
}
func (d *memDriver) Stat(context.Context, vfs.URI) (vfs.ObjectInfo, error) {
	id := vfs.ObjectIdentity{ETag: "x", Size: int64(len(d.data))}
	return vfs.ObjectInfo{URI: d.uri, Size: int64(len(d.data)), Identity: id}, nil
}
func (d *memDriver) List(context.Context, vfs.URI, vfs.ListOptions) (vfs.ListPage, error) {
	return vfs.ListPage{}, nil
}
func (d *memDriver) ReadRange(ctx context.Context, r vfs.RangeRequest, s *memory.Scope, bm *memory.BufferManager) (vfs.RangeResult, error) {
	if r.Offset+r.Length > int64(len(d.data)) {
		return vfs.RangeResult{}, fmt.Errorf("oob")
	}
	b, e := bm.Allocate(s, r.Tag, int(r.Length))
	if e != nil {
		return vfs.RangeResult{}, e
	}
	copy(b.Bytes(), d.data[r.Offset:r.Offset+r.Length])
	return vfs.RangeResult{Buffer: b, Identity: vfs.ObjectIdentity{ETag: "x", Size: int64(len(d.data))}}, nil
}
func fixture(n int) []byte {
	var b strings.Builder
	for i := 0; i < n; i++ {
		fmt.Fprintf(&b, "{\"id\":%d,\"name\":\"row-%d\",\"tags\":[\"a\",\"b\"]}\n", i, i)
	}
	return []byte(b.String())
}
func collect(t *testing.T, s *NDJSONSource, w int) []int64 {
	var mu sync.Mutex
	var ids []int64
	var first error
	var wg sync.WaitGroup
	for i := 0; i < w; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for {
				c, ok, e := s.Next(context.Background())
				if e != nil {
					mu.Lock()
					if first == nil {
						first = e
					}
					mu.Unlock()
					return
				}
				if !ok {
					return
				}
				var local []int64
				for j := 0; j < c.Cardinality; j++ {
					v, valid := c.Columns[0].Int64(j)
					if !valid {
						t.Error("null id")
					}
					local = append(local, v)
				}
				c.Release()
				mu.Lock()
				ids = append(ids, local...)
				mu.Unlock()
			}
		}()
	}
	wg.Wait()
	if first != nil {
		t.Fatal(first)
	}
	sort.Slice(ids, func(i, j int) bool { return ids[i] < ids[j] })
	return ids
}
func TestParallelNDJSONEqualsSequential(t *testing.T) {
	data := fixture(5000)
	d := newMem(t, data, vfs.Capabilities(vfs.RandomRead, vfs.ParallelRandomRead))
	r, _ := vfs.NewRegistry(d)
	m := memory.NewManager(64 << 20)
	s := m.NewScope()
	bm := memory.NewBufferManager(nil)
	binder, _ := NewNDJSONBinder(r, s, bm)
	binding, err := binder.Bind(context.Background(), d.uri.String(), NDJSONBindOptions{})
	if err != nil {
		t.Fatal(err)
	}
	seq, _ := NewNDJSONSource(context.Background(), r, binding, NDJSONScanOptions{EnableParallel: false, TargetRangeBytes: 256}, s, bm)
	a := collect(t, seq, 1)
	par, _ := NewNDJSONSource(context.Background(), r, binding, NDJSONScanOptions{EnableParallel: true, TargetRangeBytes: 256}, s, bm)
	b := collect(t, par, 4)
	if len(a) != 5000 || len(b) != 5000 {
		t.Fatalf("lens %d %d", len(a), len(b))
	}
	for i := 0; i < 5000; i++ {
		if a[i] != int64(i) || b[i] != int64(i) {
			t.Fatalf("bad %d %d %d", i, a[i], b[i])
		}
	}
	if s.Used() != 0 {
		t.Fatalf("leak %d", s.Used())
	}
}

func BenchmarkTypedDecode1024(b *testing.B) {
	records := make([][]byte, 1024)
	for i := range records {
		records[i] = []byte(fmt.Sprintf(`{"id":%d,"profile":{"name":"row-%d","score":1.25},"tags":["a","b"]}`, i, i))
	}
	schema := nestedSchema()
	proj, _ := NewProjectedPathSet("id", "profile.name", "profile.score", "tags")
	m := memory.NewManager(512 << 20)
	s := m.NewScope()
	bm := memory.NewBufferManager(nil)
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		c, err := DecodeRecords(records, schema, proj, s, bm)
		if err != nil {
			b.Fatal(err)
		}
		c.Release()
	}
}
