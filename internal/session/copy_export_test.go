package session

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"testing"

	"github.com/steamwo/fssql/internal/core/memory"
	exportpkg "github.com/steamwo/fssql/internal/export"
	"github.com/steamwo/fssql/internal/query"
	"github.com/steamwo/fssql/internal/storage/vfs"
)

type copyTestManifestStore struct {
	mu    sync.Mutex
	last  *exportpkg.Manifest
	saves int
}

func (s *copyTestManifestStore) Save(_ context.Context, manifest *exportpkg.Manifest) error {
	data, err := manifest.MarshalBinary()
	if err != nil {
		return err
	}
	clone, err := exportpkg.UnmarshalManifest(data)
	if err != nil {
		return err
	}
	s.mu.Lock()
	s.last = clone
	s.saves++
	s.mu.Unlock()
	return nil
}

func (s *copyTestManifestStore) Load(context.Context) (*exportpkg.Manifest, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.last == nil {
		return nil, errors.New("no manifest")
	}
	data, err := s.last.MarshalBinary()
	if err != nil {
		return nil, err
	}
	return exportpkg.UnmarshalManifest(data)
}

func (s *copyTestManifestStore) Delete(context.Context) error { return nil }

type copyTestAtomicDriver struct {
	mu        sync.Mutex
	caps      vfs.CapabilitySet
	final     []byte
	etag      int
	opens     int
	aborts    int
	failClose bool
}

func (d *copyTestAtomicDriver) Scheme() string                        { return "copytest" }
func (d *copyTestAtomicDriver) StaticCapabilities() vfs.CapabilitySet { return d.caps }
func (d *copyTestAtomicDriver) Probe(context.Context) (vfs.ProbeResult, error) {
	return vfs.ProbeResult{Supported: d.caps, Endpoint: "copytest://host"}, nil
}
func (d *copyTestAtomicDriver) Stat(_ context.Context, uri vfs.URI) (vfs.ObjectInfo, error) {
	d.mu.Lock()
	defer d.mu.Unlock()
	return vfs.ObjectInfo{URI: uri, Size: int64(len(d.final)), Identity: vfs.ObjectIdentity{ETag: fmt.Sprintf("etag-%d", d.etag), Size: int64(len(d.final))}}, nil
}
func (d *copyTestAtomicDriver) List(context.Context, vfs.URI, vfs.ListOptions) (vfs.ListPage, error) {
	return vfs.ListPage{}, nil
}
func (d *copyTestAtomicDriver) ReadRange(context.Context, vfs.RangeRequest, *memory.Scope, *memory.BufferManager) (vfs.RangeResult, error) {
	return vfs.RangeResult{}, vfs.ErrRangeUnsupported
}
func (d *copyTestAtomicDriver) OpenAtomicPublishWrite(_ context.Context, uri vfs.URI, _ vfs.WriteOptions, scope *memory.Scope, bm *memory.BufferManager) (vfs.WriteHandle, error) {
	if scope == nil || bm == nil {
		return nil, errors.New("managed memory required")
	}
	d.mu.Lock()
	d.opens++
	d.mu.Unlock()
	return &copyTestAtomicHandle{driver: d, uri: uri}, nil
}

type copyTestAtomicHandle struct {
	driver   *copyTestAtomicDriver
	uri      vfs.URI
	pending  []byte
	terminal bool
}

func (h *copyTestAtomicHandle) Write(ctx context.Context, data []byte) (int, error) {
	if h.terminal {
		return 0, vfs.ErrWriteClosed
	}
	if err := ctx.Err(); err != nil {
		return 0, err
	}
	h.pending = append(h.pending, data...)
	return len(data), nil
}
func (h *copyTestAtomicHandle) Close(ctx context.Context) (vfs.ObjectIdentity, error) {
	if h.terminal {
		return vfs.ObjectIdentity{}, vfs.ErrWriteClosed
	}
	if err := ctx.Err(); err != nil {
		return vfs.ObjectIdentity{}, err
	}
	h.driver.mu.Lock()
	defer h.driver.mu.Unlock()
	if h.driver.failClose {
		return vfs.ObjectIdentity{}, errors.New("injected publish failure")
	}
	h.driver.final = append(h.driver.final[:0], h.pending...)
	h.driver.etag++
	h.terminal = true
	return vfs.ObjectIdentity{ETag: fmt.Sprintf("etag-%d", h.driver.etag), Size: int64(len(h.driver.final))}, nil
}
func (h *copyTestAtomicHandle) Abort(context.Context) error {
	if h.terminal {
		return nil
	}
	h.terminal = true
	h.pending = nil
	h.driver.mu.Lock()
	h.driver.aborts++
	h.driver.mu.Unlock()
	return nil
}

func newCopyEngineHarness(t *testing.T, driver *copyTestAtomicDriver) (*StableStatementEngine, *memory.Manager, *copyTestManifestStore) {
	t.Helper()
	manager := memory.NewManager(32 << 20)
	buffers := memory.NewBufferManager(nil)
	selects := newSQLTestEngine(t, buffers)
	control, err := NewSQLControlAdapter(nil, []string{"copytest"})
	if err != nil {
		t.Fatal(err)
	}
	registry, err := vfs.NewRegistry(driver)
	if err != nil {
		t.Fatal(err)
	}
	store := &copyTestManifestStore{}
	executor, err := NewExportCopyExecutor(ExportCopyExecutorConfig{
		Registry: registry,
		Buffers:  buffers,
		ManifestStore: func(*query.Context, string) (exportpkg.ManifestStore, error) {
			return store, nil
		},
	})
	if err != nil {
		t.Fatal(err)
	}
	engine, err := NewStableStatementEngineWithCopy(selects, control, executor)
	if err != nil {
		t.Fatal(err)
	}
	return engine, manager, store
}

func TestStableCopyUsesAtomicExportAndCommitsManifest(t *testing.T) {
	driver := &copyTestAtomicDriver{caps: vfs.Capabilities(vfs.StreamingWrite, vfs.AtomicPublishWrite), final: []byte("old-complete")}
	engine, manager, store := newCopyEngineHarness(t, driver)
	q := query.New(context.Background(), manager)
	prepared, err := engine.Prepare(context.Background(), q, "COPY (SELECT 1 AS value) TO 'copytest://host/result.csv' (FORMAT csv, HEADER true)")
	if err != nil {
		t.Fatal(err)
	}
	if _, ok := q.SourceSnapshot(); !ok {
		t.Fatal("COPY Prepare must freeze even an empty source set")
	}
	driver.mu.Lock()
	before := string(driver.final)
	driver.mu.Unlock()
	if before != "old-complete" {
		t.Fatalf("Prepare mutated final=%q", before)
	}
	if err := prepared.Run(q, statementNoopSink{}); err != nil {
		t.Fatal(err)
	}
	driver.mu.Lock()
	final, opens, aborts := string(driver.final), driver.opens, driver.aborts
	driver.mu.Unlock()
	if final != "value\n1\n" || opens != 1 || aborts != 0 {
		t.Fatalf("final=%q opens=%d aborts=%d", final, opens, aborts)
	}
	store.mu.Lock()
	last := store.last
	store.mu.Unlock()
	if last == nil || last.ExportState != exportpkg.ExportStateCommitted || last.FileStates[0].PublishedIdentity == nil {
		t.Fatalf("manifest=%+v", last)
	}
	if err := q.Close(); err != nil {
		t.Fatal(err)
	}
	if manager.Used() != 0 {
		t.Fatalf("managed memory leak=%d", manager.Used())
	}
}

func TestStableCopyZeroRowsStillPublishesValidFile(t *testing.T) {
	driver := &copyTestAtomicDriver{caps: vfs.Capabilities(vfs.StreamingWrite, vfs.AtomicPublishWrite), final: []byte("old")}
	engine, manager, _ := newCopyEngineHarness(t, driver)
	q := query.New(context.Background(), manager)
	prepared, err := engine.Prepare(context.Background(), q, "COPY (SELECT 1 AS value WHERE false) TO 'copytest://host/empty.csv' (FORMAT csv, HEADER true)")
	if err != nil {
		t.Fatal(err)
	}
	if err := prepared.Run(q, statementNoopSink{}); err != nil {
		t.Fatal(err)
	}
	driver.mu.Lock()
	final := string(driver.final)
	driver.mu.Unlock()
	if final != "value\n" {
		t.Fatalf("empty COPY final=%q", final)
	}
	if err := q.Close(); err != nil {
		t.Fatal(err)
	}
}

func TestStableCopyPublishFailureLeavesPreviousFinalAndCleansOwnership(t *testing.T) {
	driver := &copyTestAtomicDriver{caps: vfs.Capabilities(vfs.StreamingWrite, vfs.AtomicPublishWrite), final: []byte("old-complete"), failClose: true}
	engine, manager, store := newCopyEngineHarness(t, driver)
	q := query.New(context.Background(), manager)
	prepared, err := engine.Prepare(context.Background(), q, "COPY (SELECT 7 AS value) TO 'copytest://host/result.csv' (FORMAT csv)")
	if err != nil {
		t.Fatal(err)
	}
	if err := prepared.Run(q, statementNoopSink{}); err == nil {
		t.Fatal("expected publish failure")
	}
	driver.mu.Lock()
	final, aborts := string(driver.final), driver.aborts
	driver.mu.Unlock()
	if final != "old-complete" || aborts != 1 {
		t.Fatalf("failed publish final=%q aborts=%d", final, aborts)
	}
	store.mu.Lock()
	last := store.last
	store.mu.Unlock()
	if last == nil || last.ExportState != exportpkg.ExportStateFailed {
		t.Fatalf("failure manifest=%+v", last)
	}
	if err := q.Close(); err != nil {
		t.Fatal(err)
	}
	if manager.Used() != 0 {
		t.Fatalf("managed memory leak=%d", manager.Used())
	}
}

func TestStableCopyRejectsMissingAtomicCapabilityBeforeOpen(t *testing.T) {
	driver := &copyTestAtomicDriver{caps: vfs.Capabilities(vfs.StreamingWrite), final: []byte("old")}
	engine, manager, store := newCopyEngineHarness(t, driver)
	q := query.New(context.Background(), manager)
	prepared, err := engine.Prepare(context.Background(), q, "COPY (SELECT 1 AS value) TO 'copytest://host/result.csv'")
	if err != nil {
		t.Fatal(err)
	}
	if err := prepared.Run(q, statementNoopSink{}); err == nil {
		t.Fatal("expected atomic capability rejection")
	}
	driver.mu.Lock()
	opens, final := driver.opens, string(driver.final)
	driver.mu.Unlock()
	store.mu.Lock()
	saves := store.saves
	store.mu.Unlock()
	if opens != 0 || saves != 0 || final != "old" {
		t.Fatalf("opens=%d saves=%d final=%q", opens, saves, final)
	}
	if err := q.Close(); err != nil {
		t.Fatal(err)
	}
}

func TestStableCopyCanceledBeforeRunDoesNotOpenOrReplace(t *testing.T) {
	driver := &copyTestAtomicDriver{caps: vfs.Capabilities(vfs.StreamingWrite, vfs.AtomicPublishWrite), final: []byte("old")}
	engine, manager, _ := newCopyEngineHarness(t, driver)
	q := query.New(context.Background(), manager)
	prepared, err := engine.Prepare(context.Background(), q, "COPY (SELECT 1 AS value) TO 'copytest://host/result.csv'")
	if err != nil {
		t.Fatal(err)
	}
	q.Cancel()
	if err := prepared.Run(q, statementNoopSink{}); !errors.Is(err, context.Canceled) {
		t.Fatalf("cancel error=%v", err)
	}
	driver.mu.Lock()
	opens, final := driver.opens, string(driver.final)
	driver.mu.Unlock()
	if opens != 0 || final != "old" {
		t.Fatalf("opens=%d final=%q", opens, final)
	}
	if err := q.Close(); err != nil {
		t.Fatal(err)
	}
}

func TestStableStatementEngineWithoutCopyAdapterFailsClosed(t *testing.T) {
	engine, _, _, manager, _ := newStatementEngineHarness(t)
	q := query.New(context.Background(), manager)
	if _, err := engine.Prepare(context.Background(), q, "COPY (SELECT 1) TO 'copytest://host/result.csv'"); err == nil {
		t.Fatal("expected missing COPY adapter error")
	}
	if err := q.Close(); err != nil {
		t.Fatal(err)
	}
}
