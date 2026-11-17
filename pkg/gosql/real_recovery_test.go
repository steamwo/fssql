package gosql

import (
	"bytes"
	"context"
	"crypto/rand"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"io"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/steamwo/fssql/internal/core/memory"
	exportpkg "github.com/steamwo/fssql/internal/export"
	"github.com/steamwo/fssql/internal/storage/vfs"
)

// These checkpoints wrap real drivers and the durable manifest store. No
// production hooks or fixture transports participate in this opt-in suite.
type realCheckpointStore struct {
	exportpkg.ManifestStore
	hook func(string) error
}

func (s *realCheckpointStore) Save(ctx context.Context, m *exportpkg.Manifest) error {
	if err := s.hook("save-" + string(m.ExportState) + "-before"); err != nil {
		return err
	}
	if err := s.ManifestStore.Save(ctx, m); err != nil {
		return err
	}
	if err := s.hook("save-" + string(m.ExportState) + "-after"); err != nil {
		return err
	}
	if m.ExportState == exportpkg.ExportStateWriting && m.Publish != nil {
		var own struct {
			Owned bool              `json:"owned"`
			Parts []json.RawMessage `json:"parts"`
		}
		_ = json.Unmarshal(m.Publish.Ownership, &own)
		if own.Owned {
			return s.hook("ownership-after")
		}
		if len(own.Parts) > 0 {
			return s.hook("part-after")
		}
	}
	return nil
}

type realCheckpointDriver struct {
	vfs.Driver
	vfs.StagedPublishDriver
	hook func(string) error
}
type realCheckpointHandle struct {
	vfs.PublishHandle
	hook func(string) error
}

func (d realCheckpointDriver) BeginPublish(ctx context.Context, i vfs.PublishIntent, r vfs.RecordOwnership, s *memory.Scope, b *memory.BufferManager) (vfs.PublishHandle, error) {
	h, err := d.StagedPublishDriver.BeginPublish(ctx, i, r, s, b)
	if h == nil {
		return nil, err
	}
	return &realCheckpointHandle{h, d.hook}, err
}
func (h *realCheckpointHandle) Publish(ctx context.Context, s vfs.SealedObject) (vfs.ObjectIdentity, error) {
	if err := h.hook("publish-before"); err != nil {
		return vfs.ObjectIdentity{}, err
	}
	i, err := h.PublishHandle.Publish(ctx, s)
	if err != nil {
		return i, err
	}
	return i, h.hook("publish-after")
}

func realJobID(t *testing.T) string {
	t.Helper()
	var b [16]byte
	if _, err := rand.Read(b[:]); err != nil {
		t.Fatal(err)
	}
	return hex.EncodeToString(b[:])
}

func realPayload(kind string) []byte {
	if os.Getenv("FSSQL_REAL_EMPTY") == "1" {
		return nil
	}
	n := 200013
	if kind == "s3" {
		n = 12<<20 + 13
	}
	b := make([]byte, n)
	for i := range b {
		b[i] = byte((i*31 + i/251) % 253)
	}
	return b
}

func runRealJob(t *testing.T, cfg Config, root string, mode CopyPublishMode, job string, hook func(string) error) error {
	t.Helper()
	db, err := Open(cfg)
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	u, err := vfs.ParseURI(root + "/recovery-" + job + ".bin")
	if err != nil {
		t.Fatal(err)
	}
	d, err := db.exportRegistry.DriverFor(u)
	if err != nil {
		t.Fatal(err)
	}
	p, ok := d.(vfs.StagedPublishDriver)
	if !ok {
		t.Fatal("missing real staged driver")
	}
	i, err := p.PlanPublish(context.Background(), u, job, vfs.PublishMode(mode))
	if err != nil {
		t.Fatal(err)
	}
	m, err := exportpkg.NewPublishManifest(job, "synthetic bytes", "acceptance", "csv", "identity", i)
	if err != nil {
		t.Fatal(err)
	}
	r, err := vfs.NewRegistry(realCheckpointDriver{d, p, hook})
	if err != nil {
		t.Fatal(err)
	}
	scope := db.memory.NewScope()
	defer scope.Close()
	c := exportpkg.PublishCoordinator{ManifestDir: cfg.ManifestDir, Registry: r, Scope: scope, Buffers: db.buffers, Store: func(id string) (exportpkg.ManifestStore, error) {
		s, err := exportpkg.NewLocalManifestStore(filepath.Join(cfg.ManifestDir, id+".json"), 0, scope, db.buffers)
		return &realCheckpointStore{ManifestStore: s, hook: hook}, err
	}}
	ctx, cancel := context.WithTimeout(context.Background(), 90*time.Second)
	defer cancel()
	data := realPayload(u.Scheme)
	_, err = c.Run(ctx, m, func(ctx context.Context, sink exportpkg.ByteSink) error {
		for offset := 0; offset < len(data); {
			end := offset + 64<<10
			if end > len(data) {
				end = len(data)
			}
			n, err := sink.Write(ctx, data[offset:end])
			if err != nil {
				return err
			}
			offset += n
			if err := hook("payload-after"); err != nil {
				return err
			}
		}
		return nil
	})
	if scope.Used() != 0 {
		t.Errorf("managed memory leak: %d", scope.Used())
	}
	return err
}

func checkRealRecovery(t *testing.T, cfg Config, job string, recoverable bool) {
	t.Helper()
	db, err := Open(cfg)
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	sum := sha256.Sum256(realPayload(os.Getenv("FSSQL_REAL_PROTOCOL")))
	for n := 0; n < 2; n++ {
		r, err := db.RecoverExport(context.Background(), job)
		if recoverable {
			if err != nil || r.Checksum != hex.EncodeToString(sum[:]) || r.Bytes != int64(len(realPayload(os.Getenv("FSSQL_REAL_PROTOCOL")))) {
				t.Fatalf("recovery=%+v error=%v", r, err)
			}
		} else if !errors.Is(err, ErrExportNotRecoverable) {
			t.Fatalf("WRITING recovery: %v", err)
		}
	}
	if db.MemoryUsed() != 0 {
		t.Fatal("recovery memory leak")
	}
	data, err := os.ReadFile(filepath.Join(cfg.ManifestDir, job+".json"))
	if err != nil {
		t.Fatal(err)
	}
	var m exportpkg.Manifest
	if json.Unmarshal(data, &m) != nil || m.Publish.CleanupPending {
		t.Fatalf("cleanup not completed: %s", data)
	}
	if bytes.Contains(data, []byte("opaquelocktoken:")) || bytes.Contains(data, []byte(os.Getenv("FSSQL_REAL_PASSWORD"))) && os.Getenv("FSSQL_REAL_PASSWORD") != "" {
		t.Fatal("manifest exposes a token or credential")
	}
}

func TestRealRemoteFaultRecovery(t *testing.T) {
	_, _, _ = realRemoteConfig(t)
	for _, point := range []string{"save-PREPARED-after", "save-COMMITTING-before", "save-COMMITTING-after", "publish-before", "publish-after", "save-COMMITTED-before", "save-COMMITTED-after"} {
		t.Run(point, func(t *testing.T) {
			cfg, root, mode := realRemoteConfig(t)
			job := realJobID(t)
			fired := false
			err := runRealJob(t, cfg, root, mode, job, func(event string) error {
				if !fired && event == point {
					fired = true
					return io.ErrUnexpectedEOF
				}
				return nil
			})
			if !fired || err == nil {
				t.Fatalf("checkpoint=%t error=%v", fired, err)
			}
			if (strings.HasPrefix(point, "publish-") || strings.HasPrefix(point, "save-COMMITTED")) && !errors.Is(err, ErrCommitUncertain) {
				t.Fatalf("lost commit result: %v", err)
			}
			checkRealRecovery(t, cfg, job, true)
		})
	}
}

func TestRealRemoteKillChild(t *testing.T) {
	if os.Getenv("FSSQL_REAL_CHILD") != "1" {
		t.Skip("child process only")
	}
	cfg, root, mode := realRemoteConfig(t)
	cfg.ManifestDir = os.Getenv("FSSQL_REAL_CHILD_MANIFESTS")
	err := runRealJob(t, cfg, root, mode, os.Getenv("FSSQL_REAL_CHILD_JOB"), func(event string) error {
		if event == os.Getenv("FSSQL_REAL_CHILD_POINT") {
			if err := os.WriteFile(filepath.Join(cfg.ManifestDir, "ready"), []byte(event), 0600); err != nil {
				return err
			}
			select {} // the parent must terminate the actual writer process
		}
		return nil
	})
	t.Fatalf("child did not stop at checkpoint: %v", err)
}

func TestRealRemoteKillRecovery(t *testing.T) {
	_, _, _ = realRemoteConfig(t)
	points := []string{"ownership-after", "save-PREPARED-after", "save-COMMITTING-after", "publish-after", "save-COMMITTED-after"}
	if os.Getenv("FSSQL_REAL_PROTOCOL") == "s3" {
		points[0] = "part-after"
	} else {
		points = append([]string{"payload-after"}, points...)
	}
	if os.Getenv("FSSQL_REAL_EMPTY") == "1" {
		points = points[1:]
	}
	for _, point := range points {
		t.Run(point, func(t *testing.T) {
			cfg, _, _ := realRemoteConfig(t)
			job := realJobID(t)
			cmd := exec.Command(os.Args[0], "-test.run=^TestRealRemoteKillChild$", "-test.timeout=2m")
			cmd.Env = append(os.Environ(), "FSSQL_REAL_CHILD=1", "FSSQL_REAL_CHILD_JOB="+job, "FSSQL_REAL_CHILD_POINT="+point, "FSSQL_REAL_CHILD_MANIFESTS="+cfg.ManifestDir)
			var output bytes.Buffer
			cmd.Stdout, cmd.Stderr = &output, &output
			if err := cmd.Start(); err != nil {
				t.Fatal(err)
			}
			done := make(chan error, 1)
			go func() { done <- cmd.Wait() }()
			defer func() { _ = cmd.Process.Kill() }()
			deadline := time.After(90 * time.Second)
			for {
				if _, err := os.Stat(filepath.Join(cfg.ManifestDir, "ready")); err == nil {
					break
				}
				select {
				case err := <-done:
					t.Fatalf("child exited: %v\n%s", err, output.String())
				case <-deadline:
					_ = cmd.Process.Kill()
					<-done
					t.Fatalf("checkpoint timeout\n%s", output.String())
				case <-time.After(20 * time.Millisecond):
				}
			}
			if err := cmd.Process.Kill(); err != nil {
				t.Fatal(err)
			}
			<-done
			if point == "save-PREPARED-after" && os.Getenv("FSSQL_REAL_RESTART_ARGV") != "" {
				var argv []string
				if json.Unmarshal([]byte(os.Getenv("FSSQL_REAL_RESTART_ARGV")), &argv) != nil || len(argv) == 0 {
					t.Fatal("invalid restart argv")
				}
				if b, err := exec.Command(argv[0], argv[1:]...).CombinedOutput(); err != nil {
					t.Fatalf("restart: %v %s", err, b)
				}
			}
			if point == "save-COMMITTING-after" {
				var wg sync.WaitGroup
				for n := 0; n < 4; n++ {
					wg.Add(1)
					go func() {
						defer wg.Done()
						db, err := Open(cfg)
						if err != nil {
							t.Error(err)
							return
						}
						defer db.Close()
						if _, err := db.RecoverExport(context.Background(), job); err != nil {
							t.Error(err)
						}
					}()
				}
				wg.Wait()
			}
			checkRealRecovery(t, cfg, job, point != "ownership-after" && point != "part-after" && point != "payload-after")
		})
	}
}

func TestRealRemoteEmptyRecovery(t *testing.T) {
	if os.Getenv("FSSQL_REAL_PROTOCOL") != "s3" {
		t.Skip("real S3 empty_put only")
	}
	t.Setenv("FSSQL_REAL_EMPTY", "1")
	t.Run("faults", TestRealRemoteFaultRecovery)
	t.Run("kill", TestRealRemoteKillRecovery)
}
