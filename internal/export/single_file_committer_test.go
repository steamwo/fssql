package export

import (
	"context"
	"errors"
	"testing"

	"github.com/steamwo/fssql/internal/core/memory"
	"github.com/steamwo/fssql/internal/storage/vfs"
)

type singleFileTestStore struct {
	saved []*Manifest
	err   error
}

func (s *singleFileTestStore) Save(_ context.Context, m *Manifest) error {
	if s.err != nil {
		return s.err
	}
	s.saved = append(s.saved, cloneManifest(m))
	return nil
}
func (s *singleFileTestStore) Load(context.Context) (*Manifest, error) {
	if len(s.saved) == 0 {
		return nil, errors.New("no manifest")
	}
	return cloneManifest(s.saved[len(s.saved)-1]), nil
}
func (s *singleFileTestStore) Delete(context.Context) error { return nil }

type singleFileTestDriver struct {
	caps    vfs.CapabilitySet
	info    vfs.ObjectInfo
	statErr error
}

func (d *singleFileTestDriver) Scheme() string                        { return "test" }
func (d *singleFileTestDriver) StaticCapabilities() vfs.CapabilitySet { return d.caps }
func (d *singleFileTestDriver) Probe(context.Context) (vfs.ProbeResult, error) {
	return vfs.ProbeResult{Supported: d.caps, Endpoint: "test://host"}, nil
}
func (d *singleFileTestDriver) Stat(context.Context, vfs.URI) (vfs.ObjectInfo, error) {
	return d.info, d.statErr
}
func (d *singleFileTestDriver) List(context.Context, vfs.URI, vfs.ListOptions) (vfs.ListPage, error) {
	return vfs.ListPage{}, nil
}
func (d *singleFileTestDriver) ReadRange(context.Context, vfs.RangeRequest, *memory.Scope, *memory.BufferManager) (vfs.RangeResult, error) {
	return vfs.RangeResult{}, vfs.ErrRangeUnsupported
}

func preparedSingleFileManifest(t *testing.T, identity PublishedObjectIdentity) *Manifest {
	t.Helper()
	m, err := NewManifest("job", "query-fp", "source-fp", "test://host/output", "csv", "none", "atomic", []string{"result.csv"})
	if err != nil {
		t.Fatal(err)
	}
	if err := m.TransitionExport(ExportStateWriting); err != nil {
		t.Fatal(err)
	}
	if err := m.TransitionFile("result.csv", FileStateWriting, 0, ""); err != nil {
		t.Fatal(err)
	}
	if err := m.TransitionFilePublished("result.csv", identity.Size, "checksum", identity); err != nil {
		t.Fatal(err)
	}
	return m
}

func TestSingleFileCommitterAdvancesExistingManifestStateMachine(t *testing.T) {
	identity := PublishedObjectIdentity{ETag: "etag-new", VersionID: "v2", Size: 7}
	m := preparedSingleFileManifest(t, identity)
	store := &singleFileTestStore{}
	driver := &singleFileTestDriver{
		caps: vfs.Capabilities(vfs.AtomicPublishWrite),
		info: vfs.ObjectInfo{Size: 7, Identity: vfs.ObjectIdentity{ETag: "etag-new", VersionID: "v2", Size: 7}},
	}
	registry, _ := vfs.NewRegistry(driver)
	committer := &SingleFileCommitter{Manifest: m, Store: store, Registry: registry}
	if err := committer.Commit(context.Background()); err != nil {
		t.Fatal(err)
	}
	if m.ExportState != ExportStateCommitted {
		t.Fatalf("state=%s", m.ExportState)
	}
	if len(store.saved) != 2 || store.saved[0].ExportState != ExportStateCommitting || store.saved[1].ExportState != ExportStateCommitted {
		t.Fatalf("durable states=%v", manifestStates(store.saved))
	}
}

func TestSingleFileCommitterResumesFromCommittingByIdentity(t *testing.T) {
	identity := PublishedObjectIdentity{ETag: "etag-new", Size: 4}
	m := preparedSingleFileManifest(t, identity)
	if err := m.TransitionExport(ExportStateCommitting); err != nil {
		t.Fatal(err)
	}
	driver := &singleFileTestDriver{caps: vfs.Capabilities(vfs.AtomicPublishWrite), info: vfs.ObjectInfo{Size: 4, Identity: vfs.ObjectIdentity{ETag: "etag-new", Size: 4}}}
	registry, _ := vfs.NewRegistry(driver)
	if err := (&SingleFileCommitter{Manifest: m, Registry: registry}).Commit(context.Background()); err != nil {
		t.Fatal(err)
	}
	if m.ExportState != ExportStateCommitted {
		t.Fatalf("state=%s", m.ExportState)
	}
}

func TestSingleFileCommitterIdentityMismatchFailsExplicitly(t *testing.T) {
	m := preparedSingleFileManifest(t, PublishedObjectIdentity{ETag: "expected", Size: 3})
	driver := &singleFileTestDriver{caps: vfs.Capabilities(vfs.AtomicPublishWrite), info: vfs.ObjectInfo{Size: 3, Identity: vfs.ObjectIdentity{ETag: "other", Size: 3}}}
	registry, _ := vfs.NewRegistry(driver)
	err := (&SingleFileCommitter{Manifest: m, Registry: registry}).Commit(context.Background())
	if !errors.Is(err, ErrSingleFileCommit) {
		t.Fatalf("error=%v", err)
	}
	if m.ExportState != ExportStateFailed {
		t.Fatalf("identity mismatch must fail manifest, state=%s", m.ExportState)
	}
}

func TestSingleFileCommitterTransientStatFailureRemainsCommitting(t *testing.T) {
	m := preparedSingleFileManifest(t, PublishedObjectIdentity{ETag: "expected", Size: 3})
	driver := &singleFileTestDriver{caps: vfs.Capabilities(vfs.AtomicPublishWrite), statErr: errors.New("temporary network failure")}
	registry, _ := vfs.NewRegistry(driver)
	err := (&SingleFileCommitter{Manifest: m, Registry: registry}).Commit(context.Background())
	if err == nil {
		t.Fatal("expected stat failure")
	}
	if m.ExportState != ExportStateCommitting {
		t.Fatalf("transient failure should remain restartable COMMITTING, state=%s", m.ExportState)
	}
}

func TestSingleFileCommitterRequiresDurableIdentityAndAtomicCapability(t *testing.T) {
	m, err := NewManifest("job", "query-fp", "source-fp", "test://host/output", "csv", "none", "atomic", []string{"result.csv"})
	if err != nil {
		t.Fatal(err)
	}
	_ = m.TransitionExport(ExportStateWriting)
	_ = m.TransitionFile("result.csv", FileStateWriting, 0, "")
	_ = m.TransitionFile("result.csv", FileStateComplete, 1, "checksum")
	driver := &singleFileTestDriver{caps: vfs.Capabilities(vfs.AtomicPublishWrite)}
	registry, _ := vfs.NewRegistry(driver)
	if err := (&SingleFileCommitter{Manifest: m, Registry: registry}).Commit(context.Background()); !errors.Is(err, ErrSingleFileCommit) {
		t.Fatalf("missing identity error=%v", err)
	}
	if m.ExportState != ExportStateWriting {
		t.Fatalf("missing identity changed state=%s", m.ExportState)
	}

	m = preparedSingleFileManifest(t, PublishedObjectIdentity{ETag: "e", Size: 1})
	driver.caps = vfs.Capabilities(vfs.StreamingWrite)
	if err := (&SingleFileCommitter{Manifest: m, Registry: registry}).Commit(context.Background()); !errors.Is(err, ErrSingleFileCommit) {
		t.Fatalf("missing atomic capability error=%v", err)
	}
	if m.ExportState != ExportStateWriting {
		t.Fatalf("capability failure changed state=%s", m.ExportState)
	}
}

func TestPublishedIdentityManifestRoundTrip(t *testing.T) {
	m := preparedSingleFileManifest(t, PublishedObjectIdentity{ETag: "etag", VersionID: "v3", Size: 11})
	data, err := m.MarshalBinary()
	if err != nil {
		t.Fatal(err)
	}
	got, err := UnmarshalManifest(data)
	if err != nil {
		t.Fatal(err)
	}
	identity := got.FileStates[0].PublishedIdentity
	if identity == nil || identity.ETag != "etag" || identity.VersionID != "v3" || identity.Size != 11 {
		t.Fatalf("identity=%+v", identity)
	}
}

func manifestStates(items []*Manifest) []ExportState {
	states := make([]ExportState, len(items))
	for i := range items {
		states[i] = items[i].ExportState
	}
	return states
}
