package objectview_test

import (
	"context"
	"crypto/sha256"
	"encoding/binary"
	"encoding/hex"
	"errors"
	"io"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/steamwo/fssql/internal/catalog"
	"github.com/steamwo/fssql/internal/core/memory"
	"github.com/steamwo/fssql/internal/core/vector"
	"github.com/steamwo/fssql/internal/format/csv"
	jsonformat "github.com/steamwo/fssql/internal/format/json"
	"github.com/steamwo/fssql/internal/format/parquet"
	"github.com/steamwo/fssql/internal/function"
	"github.com/steamwo/fssql/internal/query"
	"github.com/steamwo/fssql/internal/session"
	"github.com/steamwo/fssql/internal/storage/fileset"
	"github.com/steamwo/fssql/internal/storage/objectview"
	storagetemp "github.com/steamwo/fssql/internal/storage/temp"
	"github.com/steamwo/fssql/internal/storage/vfs"
)

type streamDriver struct {
	data    string
	opens   atomic.Int32
	stats   atomic.Int32
	closes  atomic.Int32
	started chan struct{}
	release chan struct{}
	readErr error
	unknown bool
}

func (*streamDriver) Scheme() string { return "https" }
func (*streamDriver) StaticCapabilities() vfs.CapabilitySet {
	return vfs.Capabilities(vfs.StreamingRead)
}
func (*streamDriver) Probe(context.Context) (vfs.ProbeResult, error) { return vfs.ProbeResult{}, nil }
func (d *streamDriver) Stat(_ context.Context, u vfs.URI) (vfs.ObjectInfo, error) {
	d.stats.Add(1)
	size := int64(len(d.data))
	if d.unknown {
		size = -1
	}
	return vfs.ObjectInfo{URI: u, Size: size, Identity: vfs.ObjectIdentity{ETag: "source-version", Size: size}}, nil
}
func (*streamDriver) List(context.Context, vfs.URI, vfs.ListOptions) (vfs.ListPage, error) {
	return vfs.ListPage{}, errors.New("unexpected List")
}
func (*streamDriver) ReadRange(context.Context, vfs.RangeRequest, *memory.Scope, *memory.BufferManager) (vfs.RangeResult, error) {
	return vfs.RangeResult{}, errors.New("physical RandomRead must not be used")
}
func (d *streamDriver) OpenRead(ctx context.Context, _ vfs.URI, _ *vfs.ObjectIdentity) (io.ReadCloser, error) {
	if d.opens.Add(1) == 1 && d.started != nil {
		close(d.started)
	}
	if d.release != nil {
		select {
		case <-ctx.Done():
			return nil, ctx.Err()
		case <-d.release:
		}
	}
	return &streamBody{Reader: strings.NewReader(d.data), d: d}, nil
}

type streamBody struct {
	*strings.Reader
	d *streamDriver
}

func (b *streamBody) Read(p []byte) (int, error) {
	n, err := b.Reader.Read(p)
	if err == io.EOF && b.d.readErr != nil {
		return n, b.d.readErr
	}
	return n, err
}
func (b *streamBody) Close() error { b.d.closes.Add(1); return nil }

func setup(t *testing.T, d vfs.Driver, memoryLimit, tempLimit int64) (*query.Context, *objectview.Resolver, *memory.Manager, *memory.BufferManager) {
	t.Helper()
	m := memory.NewManager(memoryLimit)
	bm := memory.NewBufferManager(nil)
	quotas := storagetemp.NewTemporaryMemoryManager(storagetemp.Limits{Global: tempLimit, DefaultResourceGroup: tempLimit, DefaultQuery: tempLimit})
	tmp, err := storagetemp.NewTemporaryStorageManager(t.TempDir(), quotas)
	if err != nil {
		t.Fatal(err)
	}
	ts, err := tmp.NewQueryScope("test")
	if err != nil {
		t.Fatal(err)
	}
	q := query.NewWithTemp(context.Background(), m, ts)
	reg, err := vfs.NewRegistry(d)
	if err != nil {
		t.Fatal(err)
	}
	r, err := objectview.New(q, reg, bm)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		if err := q.Close(); err != nil {
			t.Error(err)
		}
		if m.Used() != 0 || tmp.Used() != 0 || ts.OwnedFiles() != 0 {
			t.Errorf("leaked memory=%d temp=%d files=%d", m.Used(), tmp.Used(), ts.OwnedFiles())
		}
	})
	return q, r, m, bm
}

func uri(t *testing.T, raw string) vfs.URI {
	t.Helper()
	u, err := vfs.ParseURI(raw)
	if err != nil {
		t.Fatal(err)
	}
	return u
}

func TestSharedMaterializationRoutesRangeReads(t *testing.T) {
	d := &streamDriver{data: "id\n1\n2\n", started: make(chan struct{}), release: make(chan struct{})}
	q, r, _, bm := setup(t, d, 8<<20, 1<<20)
	u := uri(t, "https://example.test/a%2Fb//c?sig=secret")
	objects := make([]vfs.LogicalObject, 12)
	errs := make([]error, len(objects))
	var wg sync.WaitGroup
	for i := range objects {
		wg.Add(1)
		go func(i int) { defer wg.Done(); objects[i], errs[i] = r.Resolve(q.Context(), u, vfs.ObjectViewSpec{}) }(i)
	}
	<-d.started
	ctx, cancel := context.WithCancel(q.Context())
	cancel()
	if _, err := r.Resolve(ctx, u, vfs.ObjectViewSpec{}); !errors.Is(err, context.Canceled) {
		t.Fatal(err)
	}
	close(d.release)
	wg.Wait()
	for i, obj := range objects {
		if errs[i] != nil {
			t.Fatal(errs[i])
		}
		if obj.URI.ViewID != objects[0].URI.ViewID {
			t.Fatal("duplicate logical views")
		}
	}
	obj := objects[0]
	sum := sha256.Sum256([]byte(d.data))
	if obj.Identity.SnapshotID != "sha256:"+hex.EncodeToString(sum[:]) || obj.Identity.ETag != "" || obj.Identity.VersionID != "" || obj.Identity.Stable() {
		t.Fatalf("invalid identity: %+v", obj.Identity)
	}
	if obj.URI.RequestURL() != u.RequestURL() || d.opens.Load() != 1 || d.closes.Load() != 1 || d.stats.Load() != 2 {
		t.Fatal("materialization was not shared or URI changed")
	}
	reg := r.Registry()
	driver, _ := reg.DriverFor(obj.URI)
	result, err := driver.ReadRange(q.Context(), vfs.RangeRequest{URI: obj.URI, Offset: 0, Length: obj.Size, Expected: &obj.Identity}, q.Memory, bm)
	if err != nil {
		t.Fatal(err)
	}
	if string(result.Buffer.Bytes()) != d.data {
		t.Fatal("wrong logical bytes")
	}
	result.Buffer.Release()
	bad := obj.URI
	bad.RawQuery = "sig=other"
	if _, err := r.Lookup(bad); err == nil {
		t.Fatal("forged URI accepted")
	}
	other, err := r.Resolve(q.Context(), u, vfs.ObjectViewSpec{Materialize: true})
	if err != nil || other.URI.ViewID == obj.URI.ViewID || other.URI.ViewKey == obj.URI.ViewKey {
		t.Fatalf("representations shared: %v", err)
	}
	distinct, err := r.Resolve(q.Context(), uri(t, "https://example.test/a%2Fb//c?sig=second"), vfs.ObjectViewSpec{})
	if err != nil || distinct.URI.ViewID == obj.URI.ViewID {
		t.Fatalf("signed resources shared: %v", err)
	}
	if err := q.Close(); err != nil {
		t.Fatal(err)
	}
	if _, err := r.Lookup(obj.URI); err == nil {
		t.Fatal("closed view still accessible")
	}
}

func TestMaterializationFailuresReleaseResources(t *testing.T) {
	for _, tc := range []struct {
		name          string
		mem, temp     int64
		readErr, want error
	}{
		{"temp quota", 1 << 20, 2, nil, storagetemp.ErrQuotaExceeded},
		{"memory quota", 2, 1 << 20, nil, memory.ErrLimitExceeded},
		{"truncated stream", 1 << 20, 1 << 20, io.ErrUnexpectedEOF, io.ErrUnexpectedEOF},
	} {
		t.Run(tc.name, func(t *testing.T) {
			d := &streamDriver{data: "payload", readErr: tc.readErr}
			q, r, m, _ := setup(t, d, tc.mem, tc.temp)
			obj, err := r.Resolve(q.Context(), uri(t, "https://example.test/x"), vfs.ObjectViewSpec{})
			if !errors.Is(err, tc.want) || obj.Identity.SnapshotID != "" {
				t.Fatalf("obj=%+v err=%v", obj, err)
			}
			if q.Temp.Used() != 0 || q.Temp.OwnedFiles() != 0 || m.Used() != 0 || d.closes.Load() != 1 {
				t.Fatal("failed initialization leaked resources")
			}
		})
	}
}

func TestQueryCloseCancelsInitialization(t *testing.T) {
	d := &streamDriver{data: "payload", started: make(chan struct{}), release: make(chan struct{})}
	q, r, _, _ := setup(t, d, 1<<20, 1<<20)
	done := make(chan error, 1)
	go func() {
		_, err := r.Resolve(context.Background(), uri(t, "https://example.test/x"), vfs.ObjectViewSpec{})
		done <- err
	}()
	<-d.started
	if err := q.Close(); err != nil {
		t.Fatal(err)
	}
	select {
	case err := <-done:
		if !errors.Is(err, context.Canceled) {
			t.Fatal(err)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("initialization did not stop")
	}
}

func TestUnknownAndEmptyPhysicalInput(t *testing.T) {
	for _, data := range []string{"", "unknown-length"} {
		d := &streamDriver{data: data, unknown: true}
		q, r, _, _ := setup(t, d, 1<<20, 1<<20)
		obj, err := r.Resolve(q.Context(), uri(t, "https://example.test/x"), vfs.ObjectViewSpec{})
		if err != nil || obj.Size != int64(len(data)) || obj.Identity.Size != obj.Size || obj.Identity.SnapshotID == "" {
			t.Fatalf("obj=%+v err=%v", obj, err)
		}
	}
}

type randomDriver struct {
	vfs.Driver
	data  string
	reads atomic.Int32
}

func (*randomDriver) StaticCapabilities() vfs.CapabilitySet { return vfs.Capabilities(vfs.RandomRead) }
func (d *randomDriver) ReadRange(ctx context.Context, req vfs.RangeRequest, s *memory.Scope, bm *memory.BufferManager) (vfs.RangeResult, error) {
	d.reads.Add(1)
	if err := ctx.Err(); err != nil {
		return vfs.RangeResult{}, err
	}
	b, err := bm.Allocate(s, req.Tag, int(req.Length))
	if err != nil {
		return vfs.RangeResult{}, err
	}
	copy(b.Bytes(), d.data[req.Offset:req.Offset+req.Length])
	return vfs.RangeResult{Buffer: b, Identity: *req.Expected}, nil
}

func TestRandomReadSequentialAdapterAndStreamingCapabilityBoundary(t *testing.T) {
	base := &streamDriver{data: strings.Repeat("x", 150000)}
	d := &randomDriver{Driver: base, data: base.data}
	q, r, _, bm := setup(t, d, 1<<20, 1<<20)
	u := uri(t, "https://example.test/random")
	obj, err := r.Resolve(q.Context(), u, vfs.ObjectViewSpec{Materialize: true})
	if err != nil || obj.Size != int64(len(d.data)) || d.reads.Load() != 3 || base.opens.Load() != 0 {
		t.Fatalf("range adapter: %v reads=%d", err, d.reads.Load())
	}
	driver, _ := r.Registry().DriverFor(obj.URI)
	reader, err := driver.(vfs.RangeReaderOpener).OpenRangeReader(q.Context(), obj.URI, &obj.Identity)
	if err != nil {
		t.Fatal(err)
	}
	if err := reader.Close(); err != nil {
		t.Fatal(err)
	}
	// Closing a borrowed scanner must leave the query-owned materialization alive.
	result, err := driver.ReadRange(q.Context(), vfs.RangeRequest{URI: obj.URI, Length: 1}, q.Memory, bm)
	if err != nil {
		t.Fatal(err)
	}
	result.Buffer.Release()
	// Hide OpenRead while retaining the advertised StreamingRead bit.
	capabilityOnly := struct{ vfs.Driver }{base}
	q2, r2, _, _ := setup(t, capabilityOnly, 1<<20, 1<<20)
	if _, err := r2.Resolve(q2.Context(), u, vfs.ObjectViewSpec{}); !errors.Is(err, vfs.ErrRangeUnsupported) {
		t.Fatalf("capability alone enabled streaming: %v", err)
	}
}

func TestParquetBindAndCountUseMaterializedRanges(t *testing.T) {
	children, physical, required := int32(1), parquet.TypeInt64, parquet.Required
	meta, err := parquet.EncodeFileMetaData(parquet.FileMetaData{Version: 1, Schema: []parquet.SchemaElement{{Name: "schema", NumChildren: &children}, {Name: "id", Type: &physical, Repetition: &required}}, NumRows: 2, RowGroups: []parquet.RowGroup{{NumRows: 2}}})
	if err != nil {
		t.Fatal(err)
	}
	data := append([]byte("PAR1"), meta...)
	var tail [8]byte
	binary.LittleEndian.PutUint32(tail[:4], uint32(len(meta)))
	copy(tail[4:], "PAR1")
	data = append(data, tail[:]...)
	d := &streamDriver{data: string(data)}
	q, r, _, bm := setup(t, d, 8<<20, 1<<20)
	shared, _ := parquet.NewMetadataCache(8)
	cache := parquet.NewQueryMetadataCache(shared, nil)
	binder, err := parquet.NewSourceBinder(r.Registry(), cache, q.Memory, bm)
	if err != nil {
		t.Fatal(err)
	}
	pattern := "https://example.test/data?sig=secret"
	binding, err := binder.Bind(q.Context(), pattern, parquet.SourceBindOptions{})
	if err != nil {
		t.Fatal(err)
	}
	s, err := parquet.NewMetadataCountSource(q.Context(), parquet.RuntimeSourceConfig{Binder: binder, Pattern: pattern, ExpectedFingerprint: binding.Fingerprint()})
	if err != nil {
		t.Fatal(err)
	}
	chunk, ok, err := s.Next(q.Context())
	if err != nil || !ok {
		t.Fatalf("count failed: %v", err)
	}
	n, valid := chunk.Columns[0].Int64(0)
	chunk.Release()
	if !valid || n != 2 || d.opens.Load() != 1 || shared.Metrics().Entries != 0 || cache.Metrics().Hits == 0 {
		t.Fatal("Parquet view/cache routing failed")
	}
}

type countSink struct{ rows int }

func (s *countSink) Consume(_ context.Context, chunk *vector.DataChunk) error {
	s.rows += chunk.Cardinality
	return nil
}
func (*countSink) Finalize(context.Context) error { return nil }

func TestProductionQuerySourcesFreezeContentSnapshot(t *testing.T) {
	var snapshots []string
	for _, data := range []string{"1\n2\n", "3\n4\n"} {
		d := &streamDriver{data: data}
		q, _, _, bm := setup(t, d, 16<<20, 1<<20)
		reg, _ := vfs.NewRegistry(d)
		cache, _ := parquet.NewMetadataCache(8)
		functions := function.NewQueryCatalog()
		sources, err := session.NewStableQuerySourceFactory(reg, cache, bm, functions)
		if err != nil {
			t.Fatal(err)
		}
		cat, err := catalog.NewMemoryCatalog(16)
		if err != nil {
			t.Fatal(err)
		}
		engine, err := session.NewStableSQLEngine(session.StableSQLEngineConfig{Catalog: cat, Functions: functions, Sources: sources, Buffers: bm, Workers: 1})
		if err != nil {
			t.Fatal(err)
		}
		prepared, err := engine.Prepare(q.Context(), q, "SELECT * FROM read_csv('https://example.test/data?sig=secret') LIMIT 1")
		if err != nil {
			t.Fatal(err)
		}
		snapshot, ok := q.SourceSnapshot()
		if !ok {
			t.Fatal("source snapshot missing")
		}
		snapshots = append(snapshots, snapshot.Fingerprint)
		sink := &countSink{}
		if err := prepared.Run(q, sink); err != nil {
			t.Fatal(err)
		}
		if sink.rows != 1 || d.opens.Load() != 1 {
			t.Fatal("production bind/runtime did not share a view")
		}
		if err := q.Close(); err != nil {
			t.Fatal(err)
		}
	}
	if snapshots[0] == snapshots[1] {
		t.Fatal("same-size changed content omitted from SourceSnapshot")
	}
}

func TestBindFailureAndEarlySourceCloseReleaseQueryFiles(t *testing.T) {
	for _, data := range []string{"invalid json", strings.Repeat("{\"id\":1}\n", 5000)} {
		d := &streamDriver{data: data}
		q, r, _, bm := setup(t, d, 16<<20, 1<<20)
		b, err := jsonformat.NewJSONBinder(r.Registry(), q.Memory, bm)
		if err != nil {
			t.Fatal(err)
		}
		pattern := "https://example.test/data?sig=secret"
		binding, err := b.Bind(q.Context(), pattern, jsonformat.JSONBindOptions{})
		if data == "invalid json" {
			if err == nil {
				t.Fatal("invalid input bound")
			}
		} else {
			if err != nil {
				t.Fatal(err)
			}
			s, err := jsonformat.NewJSONRuntimeSource(q.Context(), jsonformat.JSONRuntimeSourceConfig{Binder: b, Pattern: pattern, ExpectedFingerprint: binding.Fingerprint()})
			if err != nil {
				t.Fatal(err)
			}
			chunk, ok, err := s.Next(q.Context())
			if err != nil || !ok {
				t.Fatalf("read: %v", err)
			}
			chunk.Release()
			if err := s.Close(); err != nil {
				t.Fatal(err)
			}
		}
		if err := q.Close(); err != nil {
			t.Fatal(err)
		}
		if q.Temp.OwnedFiles() != 0 || q.Temp.Used() != 0 {
			t.Fatal("query retained files")
		}
	}
}

type source interface {
	Next(context.Context) (*vector.DataChunk, bool, error)
}

func TestTextBindAndRuntimeShareMaterializedView(t *testing.T) {
	for _, format := range []string{"csv", "json", "ndjson"} {
		t.Run(format, func(t *testing.T) {
			data := "{\"id\":1}\n{\"id\":2}\n"
			if format == "csv" {
				data = "1\n2\n"
			}
			if format == "json" {
				data = "[{\"id\":1},{\"id\":2}]"
			}
			d := &streamDriver{data: data}
			q, r, _, bm := setup(t, d, 16<<20, 1<<20)
			pattern := "https://example.test/data?sig=secret&literal=?*["
			var s source
			var err error
			switch format {
			case "csv":
				b, e := csv.NewSourceBinder(r.Registry(), q.Memory, bm)
				if e != nil {
					t.Fatal(e)
				}
				binding, e := b.Bind(q.Context(), pattern, csv.SourceBindOptions{})
				if e != nil {
					t.Fatal(e)
				}
				s, err = csv.NewRuntimeSource(q.Context(), csv.RuntimeSourceConfig{Binder: b, Pattern: pattern, ExpectedFingerprint: binding.Fingerprint()})
			case "json":
				b, e := jsonformat.NewJSONBinder(r.Registry(), q.Memory, bm)
				if e != nil {
					t.Fatal(e)
				}
				binding, e := b.Bind(q.Context(), pattern, jsonformat.JSONBindOptions{})
				if e != nil {
					t.Fatal(e)
				}
				s, err = jsonformat.NewJSONRuntimeSource(q.Context(), jsonformat.JSONRuntimeSourceConfig{Binder: b, Pattern: pattern, ExpectedFingerprint: binding.Fingerprint()})
			case "ndjson":
				b, e := jsonformat.NewNDJSONBinder(r.Registry(), q.Memory, bm)
				if e != nil {
					t.Fatal(e)
				}
				binding, e := b.Bind(q.Context(), pattern, jsonformat.NDJSONBindOptions{})
				if e != nil {
					t.Fatal(e)
				}
				s, err = jsonformat.NewNDJSONRuntimeSource(q.Context(), jsonformat.NDJSONRuntimeSourceConfig{Binder: b, Pattern: pattern, ExpectedFingerprint: binding.Fingerprint()})
			}
			if err != nil {
				t.Fatal(err)
			}
			rows := 0
			for {
				chunk, ok, err := s.Next(q.Context())
				if err != nil {
					t.Fatal(err)
				}
				if !ok {
					break
				}
				rows += chunk.Cardinality
				chunk.Release()
			}
			if c, ok := s.(io.Closer); ok {
				if err := c.Close(); err != nil {
					t.Fatal(err)
				}
			}
			if rows != 2 || d.opens.Load() != 1 || d.stats.Load() != 2 {
				t.Fatalf("rows=%d opens=%d stats=%d", rows, d.opens.Load(), d.stats.Load())
			}
			set, err := fileset.NewPlanner(r.Registry()).Plan(q.Context(), pattern, fileset.PlanOptions{})
			if err != nil || set.Files()[0].URI.ViewID == "" {
				t.Fatalf("view lost in FileSnapshot: %v", err)
			}
		})
	}
}
