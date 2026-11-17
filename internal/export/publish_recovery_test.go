package export

import (
	"context"
	"encoding/json"
	"errors"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/steamwo/fssql/internal/core/memory"
	"github.com/steamwo/fssql/internal/storage/local"
	"github.com/steamwo/fssql/internal/storage/vfs"
)

func stopPublishAt(t *testing.T, f *publishFixture, point string) {
	t.Helper()
	stopped := false
	hook := func(event string) error {
		if event == point {
			panic("stop")
		}
		return nil
	}
	f.store.hook = hook
	f.driver.hook = hook
	func() {
		defer func() {
			if r := recover(); r != nil {
				if r != "stop" {
					panic(r)
				}
				stopped = true
			}
		}()
		_, err := f.c.Run(context.Background(), f.manifest, publishBytes)
		if err != nil {
			t.Fatal(err)
		}
	}()
	f.store.hook = nil
	f.driver.hook = nil
	if !stopped {
		t.Fatal("stop point not reached")
	}
}

func TestStagedRecoveryValidatesSealedEvidence(t *testing.T) {
	for _, mutation := range []string{"stage content", "stage missing", "token job", "token target", "scope", "mode", "mode changed", "manifest job", "revision", "version", "file traversal", "checksum"} {
		t.Run(mutation, func(t *testing.T) {
			f := newPublishFixture(t)
			stopPublishAt(t, f, "save-PREPARED-after")
			m, err := f.store.Load(context.Background())
			if err != nil {
				t.Fatal(err)
			}
			stages, _ := filepath.Glob(filepath.Join(f.dir, "*.staging"))
			if len(stages) != 1 {
				t.Fatal(stages)
			}
			switch mutation {
			case "stage content":
				_ = os.WriteFile(stages[0], []byte("modified-output\n"), 0600)
			case "stage missing":
				_ = os.Remove(stages[0])
			case "token job":
				m.Publish.Intent.Token = []byte(`{"job_id":"other"}`)
			case "token target":
				m.Publish.Sealed.RecoveryToken = []byte(`{"destination":"file:///outside"}`)
			case "scope":
				m.Publish.Intent.ScopeKey = "wrong configuration"
			case "mode":
				m.Publish.Intent.Mode = "overwrite"
			case "mode changed":
				m.Publish.Intent.Mode = vfs.PublishCompatible
			case "manifest job":
				m.JobID = strings.Repeat("f", 32)
			case "revision":
				m.Revision = 0
			case "version":
				m.ManifestVersion = 1
			case "file traversal":
				m.FileStates[0].Path = "../outside"
			case "checksum":
				m.Publish.Sealed.Checksum = strings.Repeat("0", 64)
				m.FileStates[0].Checksum = m.Publish.Sealed.Checksum
			}
			if mutation != "stage content" && mutation != "stage missing" {
				data, _ := json.Marshal(m)
				if err := os.WriteFile(filepath.Join(f.c.ManifestDir, f.manifest.JobID+".json"), data, 0600); err != nil {
					t.Fatal(err)
				}
			}
			_, err = f.c.Recover(context.Background(), f.manifest.JobID)
			if err == nil {
				t.Fatal("invalid evidence accepted")
			}
			if errors.Is(err, ErrCommitUncertain) {
				t.Fatalf("PREPARED failure marked uncertain: %v", err)
			}
			if _, err := os.Stat(f.dest); !errors.Is(err, os.ErrNotExist) {
				t.Fatal("published invalid payload")
			}
		})
	}
}

func TestStagedRecoveryUncertaintyAndCommittedCleanup(t *testing.T) {
	t.Run("resume timeout preserves COMMITTING", func(t *testing.T) {
		f := newPublishFixture(t)
		stopPublishAt(t, f, "save-COMMITTING-after")
		f.driver.hook = func(event string) error {
			if event == "resume-before" {
				return context.DeadlineExceeded
			}
			return nil
		}
		_, err := f.c.Recover(context.Background(), f.manifest.JobID)
		if !errors.Is(err, ErrCommitUncertain) || !errors.Is(err, context.DeadlineExceeded) {
			t.Fatal(err)
		}
		m, _ := f.store.Load(context.Background())
		if m.ExportState != ExportStateCommitting {
			t.Fatal(m.ExportState)
		}
	})
	t.Run("COMMITTING final conflict", func(t *testing.T) {
		f := newPublishFixture(t)
		stopPublishAt(t, f, "publish-after")
		_ = os.WriteFile(f.dest, []byte("third party"), 0600)
		if _, err := f.c.Recover(context.Background(), f.manifest.JobID); !errors.Is(err, ErrCommitConflict) {
			t.Fatal(err)
		}
		m, _ := f.store.Load(context.Background())
		if m.ExportState != ExportStateFailed {
			t.Fatal(m.ExportState)
		}
		_ = os.Remove(f.dest)
		if _, err := f.c.Recover(context.Background(), m.JobID); !errors.Is(err, ErrCommitConflict) {
			t.Fatal(err)
		}
	})
	t.Run("verify timeout preserves state", func(t *testing.T) {
		f := newPublishFixture(t)
		stopPublishAt(t, f, "publish-after")
		f.driver.hook = func(event string) error {
			if event == "verify-before" {
				return context.DeadlineExceeded
			}
			return nil
		}
		_, err := f.c.Recover(context.Background(), f.manifest.JobID)
		if !errors.Is(err, ErrCommitUncertain) || !errors.Is(err, context.DeadlineExceeded) {
			t.Fatal(err)
		}
		m, _ := f.store.Load(context.Background())
		if m.ExportState != ExportStateCommitting {
			t.Fatal(m.ExportState)
		}
		f.driver.hook = nil
		if _, err := f.c.Recover(context.Background(), m.JobID); err != nil {
			t.Fatal(err)
		}
	})
	t.Run("committed verify timeout", func(t *testing.T) {
		f := newPublishFixture(t)
		if _, err := f.c.Run(context.Background(), f.manifest, publishBytes); err != nil {
			t.Fatal(err)
		}
		f.driver.hook = func(event string) error {
			if event == "verify-before" {
				return context.DeadlineExceeded
			}
			return nil
		}
		_, err := f.c.Recover(context.Background(), f.manifest.JobID)
		if !errors.Is(err, context.DeadlineExceeded) || errors.Is(err, ErrCommitUncertain) {
			t.Fatal(err)
		}
	})
	t.Run("unknown ownership retained", func(t *testing.T) {
		f := newPublishFixture(t)
		stopPublishAt(t, f, "ownership-before")
		_, err := f.c.Recover(context.Background(), f.manifest.JobID)
		if !errors.Is(err, ErrExportNotRecoverable) {
			t.Fatal(err)
		}
		m, _ := f.store.Load(context.Background())
		if !m.Publish.CleanupPending {
			t.Fatal("unknown ownership falsely cleaned")
		}
		stages, _ := filepath.Glob(filepath.Join(f.dir, "*.staging"))
		if len(stages) != 1 {
			t.Fatal("unknown ownership deleted")
		}
	})
}

func TestStagedManifestRejectsStaleRevisionAndRollback(t *testing.T) {
	f := newPublishFixture(t)
	stopPublishAt(t, f, "save-PREPARED-after")
	m, _ := f.store.Load(context.Background())
	stale := cloneManifest(m)
	if err := f.store.Save(context.Background(), stale); !errors.Is(err, ErrStaleRevision) {
		t.Fatal(err)
	}
	stale.Revision++
	stale.ExportState = ExportStateWriting
	if err := f.store.Save(context.Background(), stale); !errors.Is(err, ErrInvalidTransition) {
		t.Fatal(err)
	}
	if _, err := f.c.Recover(context.Background(), m.JobID); err != nil {
		t.Fatal(err)
	}
	stale = cloneManifest(m)
	stale.Revision += 3
	if err := f.store.Save(context.Background(), stale); err == nil {
		t.Fatal("terminal rollback accepted")
	}
}

func TestStagedNormalPublishAndRecoveryShareLock(t *testing.T) {
	f := newPublishFixture(t)
	writing := make(chan struct{})
	proceed := make(chan struct{})
	done := make(chan error, 1)
	go func() {
		_, err := f.c.Run(context.Background(), f.manifest, func(ctx context.Context, s ByteSink) error { close(writing); <-proceed; return publishBytes(ctx, s) })
		done <- err
	}()
	<-writing
	ctx, cancel := context.WithTimeout(context.Background(), 80*time.Millisecond)
	defer cancel()
	if _, err := f.c.Recover(ctx, f.manifest.JobID); !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("recovery raced active writer: %v", err)
	}
	close(proceed)
	if err := <-done; err != nil {
		t.Fatal(err)
	}
	var wg sync.WaitGroup
	results := make(chan error, 8)
	for i := 0; i < 8; i++ {
		wg.Add(1)
		go func() { defer wg.Done(); _, err := f.c.Recover(context.Background(), f.manifest.JobID); results <- err }()
	}
	wg.Wait()
	close(results)
	for err := range results {
		if err != nil {
			t.Fatal(err)
		}
	}
}

func TestExportJobLockProcessHelper(t *testing.T) {
	mode := os.Getenv("FSSQL_PUBLISH_TEST_MODE")
	if mode == "" {
		return
	}
	dir, job := os.Getenv("FSSQL_PUBLISH_TEST_DIR"), os.Getenv("FSSQL_PUBLISH_TEST_JOB")
	if mode == "publish" {
		f := newPublishFixtureAt(t, os.Getenv("FSSQL_PUBLISH_TEST_ROOT"), job)
		point := os.Getenv("FSSQL_PUBLISH_TEST_POINT")
		hook := func(event string) error {
			if event == point {
				if err := os.WriteFile(filepath.Join(dir, "ready"), []byte(event), 0600); err != nil {
					return err
				}
				time.Sleep(5 * time.Minute)
			}
			return nil
		}
		f.store.hook, f.driver.hook = hook, hook
		if _, err := f.c.Run(context.Background(), f.manifest, publishBytes); err != nil {
			t.Fatal(err)
		}
		t.Fatal("publish checkpoint not reached")
	}
	if mode == "lock" {
		unlock, err := LockJob(context.Background(), dir, job)
		if err != nil {
			t.Fatal(err)
		}
		defer unlock()
		if err := os.WriteFile(filepath.Join(dir, "ready"), []byte("ready"), 0600); err != nil {
			t.Fatal(err)
		}
		time.Sleep(5 * time.Minute)
		return
	}
	root := os.Getenv("FSSQL_PUBLISH_TEST_ROOT")
	d, err := local.NewDriver(root)
	if err != nil {
		t.Fatal(err)
	}
	registry, _ := vfs.NewRegistry(d)
	scope := memory.NewManager(8 << 20).NewScope()
	bm := memory.NewBufferManager(nil)
	c := PublishCoordinator{ManifestDir: dir, Registry: registry, Scope: scope, Buffers: bm, Store: func(job string) (ManifestStore, error) {
		return NewLocalManifestStore(filepath.Join(dir, job+".json"), 0, scope, bm)
	}}
	if _, err := c.Recover(context.Background(), job); err != nil {
		t.Fatal(err)
	}
	if scope.Used() != 0 {
		t.Fatal("memory leak")
	}
}

func TestStagedRecoveryAfterProcessKill(t *testing.T) {
	for _, point := range []string{"save-PREPARED-after", "save-COMMITTING-after", "publish-after", "save-COMMITTED-after"} {
		t.Run(point, func(t *testing.T) {
			f := newPublishFixture(t)
			cmd := publishChild("publish", f.c.ManifestDir, f.manifest.JobID, f.dir)
			cmd.Env = append(cmd.Env, "FSSQL_PUBLISH_TEST_POINT="+point)
			if err := cmd.Start(); err != nil {
				t.Fatal(err)
			}
			defer func() { _ = cmd.Process.Kill(); _ = cmd.Wait() }()
			deadline := time.After(10 * time.Second)
			for {
				if _, err := os.Stat(filepath.Join(f.c.ManifestDir, "ready")); err == nil {
					break
				}
				select {
				case <-deadline:
					t.Fatal("child did not reach publish checkpoint")
				case <-time.After(10 * time.Millisecond):
				}
			}
			if err := cmd.Process.Kill(); err != nil {
				t.Fatal(err)
			}
			_ = cmd.Wait()
			for i := 0; i < 2; i++ {
				if _, err := f.c.Recover(context.Background(), f.manifest.JobID); err != nil {
					t.Fatal(err)
				}
			}
			data, err := os.ReadFile(f.dest)
			if err != nil || string(data) != "physical-output\n" {
				t.Fatalf("output=%q error=%v", data, err)
			}
		})
	}
}

func publishChild(mode, dir, job, root string) *exec.Cmd {
	cmd := exec.Command(os.Args[0], "-test.run=^TestExportJobLockProcessHelper$", "-test.timeout=30s")
	cmd.Env = append(os.Environ(), "FSSQL_PUBLISH_TEST_MODE="+mode, "FSSQL_PUBLISH_TEST_DIR="+dir, "FSSQL_PUBLISH_TEST_JOB="+job, "FSSQL_PUBLISH_TEST_ROOT="+root)
	return cmd
}

func TestExportJobLockAcrossProcessesAndExit(t *testing.T) {
	dir := t.TempDir()
	job := strings.Repeat("a", 32)
	cmd := publishChild("lock", dir, job, "")
	if err := cmd.Start(); err != nil {
		t.Fatal(err)
	}
	defer func() { _ = cmd.Process.Kill(); _ = cmd.Wait() }()
	deadline := time.After(10 * time.Second)
	for {
		if _, err := os.Stat(filepath.Join(dir, "ready")); err == nil {
			break
		}
		select {
		case <-deadline:
			t.Fatal("child did not acquire lock")
		case <-time.After(10 * time.Millisecond):
		}
	}
	ctx, cancel := context.WithTimeout(context.Background(), 80*time.Millisecond)
	defer cancel()
	if _, err := LockJob(ctx, dir, job); !errors.Is(err, context.DeadlineExceeded) {
		t.Fatal(err)
	}
	if err := cmd.Process.Kill(); err != nil {
		t.Fatal(err)
	}
	_ = cmd.Wait()
	unlock, err := LockJob(context.Background(), dir, job)
	if err != nil {
		t.Fatal(err)
	}
	_ = unlock()
	if _, err := os.Stat(filepath.Join(dir, job+".lock")); err != nil {
		t.Fatal("lock inode deleted")
	}
}

func TestStagedRecoveryAcrossProcesses(t *testing.T) {
	f := newPublishFixture(t)
	stopPublishAt(t, f, "save-PREPARED-after")
	first := publishChild("recover", f.c.ManifestDir, f.manifest.JobID, f.dir)
	second := publishChild("recover", f.c.ManifestDir, f.manifest.JobID, f.dir)
	if err := first.Start(); err != nil {
		t.Fatal(err)
	}
	if err := second.Start(); err != nil {
		_ = first.Process.Kill()
		_ = first.Wait()
		t.Fatal(err)
	}
	if err := first.Wait(); err != nil {
		t.Fatal(err)
	}
	if err := second.Wait(); err != nil {
		t.Fatal(err)
	}
	m, err := f.store.Load(context.Background())
	if err != nil || m.ExportState != ExportStateCommitted || m.Publish.CleanupPending {
		t.Fatalf("manifest=%+v err=%v", m, err)
	}
}

func FuzzJobID(f *testing.F) {
	for _, seed := range []string{strings.Repeat("a", 32), strings.Repeat("b", 64), "../x", "%2e%2e", "A", strings.Repeat("A", 32), ""} {
		f.Add(seed)
	}
	f.Fuzz(func(t *testing.T, value string) {
		job, err := parseJobID(value)
		valid := len(value) == 32 || len(value) == 64
		for _, c := range value {
			valid = valid && (c >= '0' && c <= '9' || c >= 'a' && c <= 'f')
		}
		if (err == nil) != valid || err == nil && string(job) != value {
			t.Fatalf("invalid parse: %q", value)
		}
	})
}

func FuzzManifestVersions(f *testing.F) {
	m, _ := NewManifest(strings.Repeat("a", 64), "query", "source", "file:///output", "csv", "identity", "snapshot", []string{"part.csv"})
	data, _ := m.MarshalBinary()
	f.Add(data)
	intent := vfs.PublishIntent{JobID: strings.Repeat("b", 32), Destination: "file:///output/part.csv", Protocol: "file", Mode: vfs.PublishStrict, ScopeKey: "scope", Token: []byte("token")}
	v2, err := NewPublishManifest(intent.JobID, "query", "source", "csv", "identity", intent)
	if err != nil {
		f.Fatal(err)
	}
	data, err = v2.MarshalBinary()
	if err != nil {
		f.Fatal(err)
	}
	f.Add(data)
	f.Add([]byte(`{"manifest_version":2}`))
	f.Add([]byte(`null`))
	f.Fuzz(func(t *testing.T, data []byte) {
		if len(data) > 64<<10 {
			return
		}
		m, err := UnmarshalManifest(data)
		if err != nil {
			return
		}
		encoded, err := m.MarshalBinary()
		if err != nil {
			t.Fatal(err)
		}
		if _, err := UnmarshalManifest(encoded); err != nil {
			t.Fatal(err)
		}
	})
}

func TestStagedPublishCancellationAndMemoryLimit(t *testing.T) {
	t.Run("cancel while writing", func(t *testing.T) {
		f := newPublishFixture(t)
		ctx, cancel := context.WithCancel(context.Background())
		defer cancel()
		_, err := f.c.Run(ctx, f.manifest, func(ctx context.Context, s ByteSink) error {
			if err := publishBytes(ctx, s); err != nil {
				return err
			}
			cancel()
			return ctx.Err()
		})
		if !errors.Is(err, context.Canceled) || errors.Is(err, ErrCommitUncertain) {
			t.Fatal(err)
		}
		if _, err := os.Stat(f.dest); !errors.Is(err, os.ErrNotExist) {
			t.Fatal("canceled write published")
		}
		stages, _ := filepath.Glob(filepath.Join(f.dir, "*.staging"))
		if len(stages) != 0 {
			t.Fatal("canceled staging leaked")
		}
	})
	t.Run("metadata memory", func(t *testing.T) {
		f := newPublishFixture(t)
		limited := memory.NewManager(1024).NewScope()
		defer limited.Close()
		f.c.Scope = limited
		_, err := f.c.Run(context.Background(), f.manifest, publishBytes)
		if !errors.Is(err, memory.ErrLimitExceeded) {
			t.Fatal(err)
		}
		if limited.Used() != 0 {
			t.Fatal("metadata reservation leaked")
		}
		if _, err := os.Stat(f.dest); !errors.Is(err, os.ErrNotExist) {
			t.Fatal("memory failure published")
		}
	})
}

func TestStagedLocalRejectsFinalAsStaging(t *testing.T) {
	f := newPublishFixture(t)
	u, err := local.URIFromPath(filepath.Join(f.dir, ".fssql-"+f.manifest.JobID+".staging"))
	if err != nil {
		t.Fatal(err)
	}
	if _, err := f.driver.PlanPublish(context.Background(), u, f.manifest.JobID, vfs.PublishStrict); err == nil {
		t.Fatal("staging can name final output")
	}
}
