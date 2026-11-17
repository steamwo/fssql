package export

import (
	"context"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/steamwo/fssql/internal/core/memory"
	"github.com/steamwo/fssql/internal/storage/local"
	"github.com/steamwo/fssql/internal/storage/vfs"
)

type publishFaultDriver struct {
	*local.Driver
	hook func(string) error
}

func (d *publishFaultDriver) event(s string) error {
	if d.hook != nil {
		return d.hook(s)
	}
	return nil
}
func (d *publishFaultDriver) BeginPublish(ctx context.Context, i vfs.PublishIntent, r vfs.RecordOwnership, s *memory.Scope, b *memory.BufferManager) (vfs.PublishHandle, error) {
	h, err := d.Driver.BeginPublish(ctx, i, func(ctx context.Context, p []byte) error {
		if err := d.event("ownership-before"); err != nil {
			return err
		}
		err := r(ctx, p)
		if err != nil {
			return err
		}
		return d.event("ownership-after")
	}, s, b)
	if h != nil {
		h = &publishFaultHandle{PublishHandle: h, driver: d}
	}
	return h, err
}
func (d *publishFaultDriver) ResumePublish(ctx context.Context, i vfs.PublishIntent, o []byte, s *vfs.SealedObject, m *memory.Scope, b *memory.BufferManager) (vfs.PublishHandle, error) {
	if err := d.event("resume-before"); err != nil {
		return nil, err
	}
	h, err := d.Driver.ResumePublish(ctx, i, o, s, m, b)
	if err != nil {
		return nil, err
	}
	return &publishFaultHandle{PublishHandle: h, driver: d}, nil
}

type publishFaultHandle struct {
	vfs.PublishHandle
	driver *publishFaultDriver
}

func (h *publishFaultHandle) Write(ctx context.Context, p []byte) (int, error) {
	n, err := h.PublishHandle.Write(ctx, p)
	if err == nil {
		err = h.driver.event("write-after")
	}
	return n, err
}
func (h *publishFaultHandle) Seal(ctx context.Context, e vfs.WriteEvidence) (vfs.SealedObject, error) {
	if err := h.driver.event("seal-before"); err != nil {
		return vfs.SealedObject{}, err
	}
	s, err := h.PublishHandle.Seal(ctx, e)
	if err == nil {
		err = h.driver.event("seal-after")
	}
	return s, err
}
func (h *publishFaultHandle) Publish(ctx context.Context, s vfs.SealedObject) (vfs.ObjectIdentity, error) {
	if err := h.driver.event("publish-before"); err != nil {
		return vfs.ObjectIdentity{}, err
	}
	i, err := h.PublishHandle.Publish(ctx, s)
	if err == nil {
		err = h.driver.event("publish-after")
	}
	return i, err
}
func (h *publishFaultHandle) Verify(ctx context.Context, s vfs.SealedObject, final bool) (vfs.ObjectIdentity, error) {
	if final {
		if err := h.driver.event("verify-before"); err != nil {
			return vfs.ObjectIdentity{}, err
		}
	}
	i, err := h.PublishHandle.Verify(ctx, s, final)
	if err == nil && final {
		err = h.driver.event("verify-after")
	}
	return i, err
}
func (h *publishFaultHandle) Abort(ctx context.Context, s *vfs.SealedObject) error {
	if err := h.driver.event("cleanup-before"); err != nil {
		return err
	}
	if err := h.PublishHandle.Abort(ctx, s); err != nil {
		return err
	}
	return h.driver.event("cleanup-after")
}

type publishFaultStore struct {
	ManifestStore
	hook func(string) error
}

func (s *publishFaultStore) Save(ctx context.Context, m *Manifest) error {
	name := string(m.ExportState)
	if m.ExportState == ExportStateWriting && len(m.Publish.Ownership) > 0 {
		name = "OWNED"
	}
	if m.ExportState == ExportStateCommitted && !m.Publish.CleanupPending {
		name = "CLEANED"
	}
	if s.hook != nil {
		if err := s.hook("save-" + name + "-before"); err != nil {
			return err
		}
	}
	if err := s.ManifestStore.Save(ctx, m); err != nil {
		return err
	}
	if s.hook != nil {
		return s.hook("save-" + name + "-after")
	}
	return nil
}

type publishFixture struct {
	c         *PublishCoordinator
	driver    *publishFaultDriver
	store     *publishFaultStore
	manifest  *Manifest
	dest, dir string
	scope     *memory.Scope
}

func newPublishFixture(t *testing.T) *publishFixture {
	t.Helper()
	dir := t.TempDir()
	job, err := NewJobID()
	if err != nil {
		t.Fatal(err)
	}
	return newPublishFixtureAt(t, dir, string(job))
}

func newPublishFixtureAt(t *testing.T, dir, job string) *publishFixture {
	t.Helper()
	d, err := local.NewDriver(dir)
	if err != nil {
		t.Fatal(err)
	}
	driver := &publishFaultDriver{Driver: d}
	registry, err := vfs.NewRegistry(driver)
	if err != nil {
		t.Fatal(err)
	}
	scope := memory.NewManager(8 << 20).NewScope()
	bm := memory.NewBufferManager(nil)
	u, err := local.URIFromPath(filepath.Join(dir, "result.bin"))
	if err != nil {
		t.Fatal(err)
	}
	i, err := driver.PlanPublish(context.Background(), u, string(job), vfs.PublishStrict)
	if err != nil {
		t.Fatal(err)
	}
	m, err := NewPublishManifest(string(job), "query", "source", "csv", "identity", i)
	if err != nil {
		t.Fatal(err)
	}
	manifestDir := filepath.Join(dir, "manifests")
	store, err := NewLocalManifestStore(filepath.Join(manifestDir, string(job)+".json"), 0, scope, bm)
	if err != nil {
		t.Fatal(err)
	}
	fault := &publishFaultStore{ManifestStore: store}
	c := &PublishCoordinator{ManifestDir: manifestDir, Registry: registry, Scope: scope, Buffers: bm, Store: func(string) (ManifestStore, error) { return fault, nil }}
	f := &publishFixture{c: c, driver: driver, store: fault, manifest: m, dest: filepath.Join(dir, "result.bin"), dir: dir, scope: scope}
	t.Cleanup(func() {
		if scope.Used() != 0 {
			t.Errorf("managed memory leak: %d", scope.Used())
		}
		_ = scope.Close()
	})
	return f
}
func publishBytes(ctx context.Context, s ByteSink) error {
	_, err := s.Write(ctx, []byte("physical-output\n"))
	return err
}

func TestStagedPublishCrashRecoveryMatrix(t *testing.T) {
	for _, point := range []string{"save-PLANNED-before", "save-PLANNED-after", "save-WRITING-after", "ownership-before", "ownership-after", "write-after", "seal-before", "seal-after", "save-PREPARED-before", "save-PREPARED-after", "save-COMMITTING-before", "save-COMMITTING-after", "publish-before", "publish-after", "verify-after", "save-COMMITTED-before", "save-COMMITTED-after", "cleanup-before", "cleanup-after", "save-CLEANED-after"} {
		t.Run(point, func(t *testing.T) {
			f := newPublishFixture(t)
			crashed := false
			hook := func(event string) error {
				if event == point {
					panic("crash")
				}
				return nil
			}
			f.store.hook = hook
			f.driver.hook = hook
			func() {
				defer func() {
					if r := recover(); r != nil {
						if r != "crash" {
							panic(r)
						}
						crashed = true
					}
				}()
				_, err := f.c.Run(context.Background(), f.manifest, publishBytes)
				if err != nil {
					t.Fatal(err)
				}
			}()
			if !crashed {
				t.Fatal("crash point not reached")
			}
			f.store.hook = nil
			f.driver.hook = nil
			m, loadErr := f.store.Load(context.Background())
			_, err := f.c.Recover(context.Background(), f.manifest.JobID)
			if errors.Is(loadErr, os.ErrNotExist) || m.ExportState == ExportStatePlanned || m.ExportState == ExportStateWriting {
				if err == nil {
					t.Fatal("unsealed job recovered")
				}
				if _, e := os.Stat(f.dest); !errors.Is(e, os.ErrNotExist) {
					t.Fatalf("final created before PREPARED: %v", e)
				}
			} else {
				if err != nil {
					t.Fatal(err)
				}
				data, e := os.ReadFile(f.dest)
				if e != nil || string(data) != "physical-output\n" {
					t.Fatalf("final %q: %v", data, e)
				}
				if _, e := f.c.Recover(context.Background(), f.manifest.JobID); e != nil {
					t.Fatal(e)
				}
			}
		})
	}
}

func TestStagedPublishFailureClassification(t *testing.T) {
	for _, point := range []string{"ownership-before", "write-after", "seal-before", "save-PREPARED-before", "save-PREPARED-after", "save-COMMITTING-before", "save-COMMITTING-after", "publish-before", "publish-after", "verify-after", "save-COMMITTED-before", "save-COMMITTED-after", "cleanup-before", "save-CLEANED-before"} {
		t.Run(point, func(t *testing.T) {
			f := newPublishFixture(t)
			injected := errors.New("injected failure")
			hook := func(event string) error {
				if event == point {
					return injected
				}
				return nil
			}
			f.store.hook = hook
			f.driver.hook = hook
			_, err := f.c.Run(context.Background(), f.manifest, publishBytes)
			cleanup := point == "cleanup-before" || point == "save-CLEANED-before"
			if cleanup {
				if err != nil {
					t.Fatal(err)
				}
			} else {
				if !errors.Is(err, injected) {
					t.Fatalf("lost cause: %v", err)
				}
				var job *ExportJobError
				var uncertain *CommitUncertainError
				if !errors.As(err, &job) && !errors.As(err, &uncertain) {
					t.Fatalf("no JobID: %v", err)
				}
			}
			m, e := f.store.Load(context.Background())
			if e != nil {
				t.Fatal(e)
			}
			wantUncertain := strings.HasPrefix(point, "publish-") || point == "verify-after" || strings.HasPrefix(point, "save-COMMITTED-")
			if errors.Is(err, ErrCommitUncertain) != wantUncertain {
				t.Fatalf("uncertainty=%v: %v", wantUncertain, err)
			}
			if strings.HasPrefix(point, "save-COMMITTING-") {
				if _, e := os.Stat(f.dest); !errors.Is(e, os.ErrNotExist) {
					t.Fatal("published despite failed intent save")
				}
				matches, _ := filepath.Glob(filepath.Join(f.dir, "*.staging"))
				if len(matches) != 1 {
					t.Fatal("sealed staging deleted")
				}
			}
			f.store.hook = nil
			f.driver.hook = nil
			if m.ExportState == ExportStatePrepared || m.ExportState == ExportStateCommitting || m.ExportState == ExportStateCommitted {
				if _, e := f.c.Recover(context.Background(), m.JobID); e != nil {
					t.Fatal(e)
				}
			}
		})
	}
}

func TestStagedPublishConflictAndStateGates(t *testing.T) {
	for _, same := range []bool{false, true} {
		t.Run(fmt.Sprint(same), func(t *testing.T) {
			f := newPublishFixture(t)
			contents := "third-party"
			if same {
				contents = "physical-output\n"
			}
			if err := os.WriteFile(f.dest, []byte(contents), 0600); err != nil {
				t.Fatal(err)
			}
			_, err := f.c.Run(context.Background(), f.manifest, publishBytes)
			if !errors.Is(err, ErrCommitConflict) {
				t.Fatal(err)
			}
			if _, err := f.c.Recover(context.Background(), f.manifest.JobID); !errors.Is(err, ErrCommitConflict) {
				t.Fatal(err)
			}
			data, _ := os.ReadFile(f.dest)
			if string(data) != contents {
				t.Fatal("conflict target modified")
			}
		})
	}
	t.Run("committed never recreates", func(t *testing.T) {
		f := newPublishFixture(t)
		if _, err := f.c.Run(context.Background(), f.manifest, publishBytes); err != nil {
			t.Fatal(err)
		}
		if err := os.Remove(f.dest); err != nil {
			t.Fatal(err)
		}
		if _, err := f.c.Recover(context.Background(), f.manifest.JobID); !errors.Is(err, ErrExportNotRecoverable) {
			t.Fatal(err)
		}
		m, _ := f.store.Load(context.Background())
		if m.ExportState != ExportStateCommitted {
			t.Fatal(m.ExportState)
		}
	})
	t.Run("committed modified", func(t *testing.T) {
		f := newPublishFixture(t)
		if _, err := f.c.Run(context.Background(), f.manifest, publishBytes); err != nil {
			t.Fatal(err)
		}
		_ = os.WriteFile(f.dest, []byte("changed"), 0600)
		_, err := f.c.Recover(context.Background(), f.manifest.JobID)
		if !errors.Is(err, ErrCommitConflict) || errors.Is(err, ErrCommitUncertain) {
			t.Fatal(err)
		}
		m, _ := f.store.Load(context.Background())
		if m.ExportState != ExportStateCommitted {
			t.Fatal(m.ExportState)
		}
	})
}
