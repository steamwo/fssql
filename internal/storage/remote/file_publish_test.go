package remote_test

import (
	"bytes"
	"context"
	"crypto/sha256"
	"crypto/tls"
	"crypto/x509"
	"encoding/hex"
	"errors"
	"fmt"
	"net"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"sync"
	"testing"

	"github.com/steamwo/fssql/internal/core/memory"
	exportpkg "github.com/steamwo/fssql/internal/export"
	"github.com/steamwo/fssql/internal/security"
	"github.com/steamwo/fssql/internal/storage/remote"
	"github.com/steamwo/fssql/internal/storage/remote/filetest"
	"github.com/steamwo/fssql/internal/storage/remote/ftp"
	"github.com/steamwo/fssql/internal/storage/remote/sftp"
	"github.com/steamwo/fssql/internal/storage/remote/webdav"
	"github.com/steamwo/fssql/internal/storage/vfs"
)

var fileProtocols = []string{"sftp/strict", "webdav/strict", "sftp/compatible", "webdav/compatible", "ftp/compatible", "ftps/compatible"}

func fileDriver(t *testing.T, kind string, store *filetest.Store) (vfs.Driver, vfs.StagedPublishDriver, vfs.URI, vfs.PublishMode) {
	t.Helper()
	protocol, modeText, _ := strings.Cut(kind, "/")
	mode := vfs.PublishMode(modeText)
	key := remote.ClientKey{Endpoint: "fixture", CredentialScope: "test", CredentialVersion: "1", SecurityProfile: "test"}
	var driver vfs.Driver
	switch protocol {
	case "sftp":
		listener, closeFn, err := filetest.Listen(store.SFTP)
		if err != nil {
			t.Fatal(err)
		}
		t.Cleanup(closeFn)
		key.Endpoint = listener.Addr().String()
		manager := sftp.NewManager(1, 2, sftp.SessionFactoryFunc(func(ctx context.Context, _ remote.ClientKey) (sftp.Session, error) {
			conn, e := (&net.Dialer{}).DialContext(ctx, "tcp", key.Endpoint)
			if e != nil {
				return nil, e
			}
			return sftp.NewWireSession(conn)
		}))
		t.Cleanup(func() { _ = manager.Close() })
		d, err := sftp.NewDriver(key, manager, 2)
		if err != nil {
			t.Fatal(err)
		}
		driver = d
	case "webdav":
		server := httptest.NewServer(store)
		t.Cleanup(server.Close)
		key.Endpoint = strings.TrimPrefix(server.URL, "http://")
		_, portText, _ := net.SplitHostPort(key.Endpoint)
		port, _ := strconv.Atoi(portText)
		policy := security.NewNetworkPolicy(port)
		policy.AllowLoopback = true
		clients, e := webdav.NewClientManager(1, "webdav", policy, nil)
		if e != nil {
			t.Fatal(e)
		}
		client, e := clients.Client(key)
		if e != nil {
			t.Fatal(e)
		}
		t.Cleanup(client.CloseIdleConnections)
		d, e := webdav.NewDriver("webdav", key, clients, webdav.CredentialProviderFunc(func(context.Context, remote.ClientKey, string) (webdav.Credentials, error) {
			return webdav.Credentials{Username: "user", Password: "pass"}, nil
		}), policy, 2)
		if e != nil {
			t.Fatal(e)
		}
		d.AtomicMoveVerified = true
		driver = d
	case "ftp", "ftps":
		var serverTLS, clientTLS *tls.Config
		if protocol == "ftps" {
			cert := httptest.NewTLSServer(http.NotFoundHandler())
			t.Cleanup(cert.Close)
			serverTLS = cert.TLS.Clone()
			roots := x509.NewCertPool()
			roots.AddCert(cert.Certificate())
			clientTLS = &tls.Config{RootCAs: roots}
		}
		listener, closeFn, e := filetest.Listen(func(c net.Conn) { store.FTP(c, serverTLS) })
		if e != nil {
			t.Fatal(e)
		}
		t.Cleanup(closeFn)
		key.Endpoint = listener.Addr().String()
		_, portText, _ := net.SplitHostPort(key.Endpoint)
		port, _ := strconv.Atoi(portText)
		policy := security.NewNetworkPolicy(port)
		policy.AllowLoopback = true
		policy.AllowPlainFTP = true
		factory := &ftp.WireFactory{Scheme: protocol, Policy: policy, TLSConfig: clientTLS, Credentials: ftp.CredentialProviderFunc(func(context.Context, remote.ClientKey, string) (ftp.Credentials, error) {
			return ftp.Credentials{Username: "user", Password: "pass"}, nil
		})}
		manager := ftp.NewManager(1, 2, factory)
		pool, e := manager.Pool(key)
		if e != nil {
			t.Fatal(e)
		}
		t.Cleanup(func() { _ = pool.Close() })
		d, e := ftp.NewDriver(protocol, key, manager, 2)
		if e != nil {
			t.Fatal(e)
		}
		driver = d
	}
	u, e := vfs.ParseURI(protocol + "://" + key.Endpoint + "/result.bin")
	if e != nil {
		t.Fatal(e)
	}
	return driver, driver.(vfs.StagedPublishDriver), u, mode
}

func fileMemory(t *testing.T) (*memory.Scope, *memory.BufferManager) {
	t.Helper()
	s := memory.NewManager(16 << 20).NewScope()
	t.Cleanup(func() {
		if s.Used() != 0 {
			t.Errorf("managed leak %d", s.Used())
		}
		_ = s.Close()
	})
	return s, memory.NewBufferManager(nil)
}
func fileEvidence(b []byte) vfs.WriteEvidence {
	sum := sha256.Sum256(b)
	return vfs.WriteEvidence{Size: int64(len(b)), Checksum: hex.EncodeToString(sum[:])}
}
func fileIntent(t *testing.T, p vfs.StagedPublishDriver, u vfs.URI, mode vfs.PublishMode) vfs.PublishIntent {
	t.Helper()
	job, e := vfs.NewPublishJobID()
	if e != nil {
		t.Fatal(e)
	}
	i, e := p.PlanPublish(context.Background(), u, job, mode)
	if e != nil {
		t.Fatal(e)
	}
	return i
}

func TestWebDAVETagRecoveryRetainsUnverifiedStaging(t *testing.T) {
	store := filetest.New()
	_, driver, dest, mode := fileDriver(t, "webdav/strict", store)
	scope, bm := fileMemory(t)
	ctx := context.Background()
	intent := fileIntent(t, driver, dest, mode)
	var ownership []byte
	handle, err := driver.BeginPublish(ctx, intent, func(_ context.Context, token []byte) error {
		ownership = append([]byte(nil), token...)
		return nil
	}, scope, bm)
	if handle != nil {
		_ = handle.Release()
	}
	if err != nil {
		t.Fatal(err)
	}

	stage := "/.fssql-" + intent.JobID + ".staging"
	store.Put(stage, []byte("different staging contents"))
	handle, err = driver.ResumePublish(ctx, intent, ownership, nil, scope, bm)
	if err != nil {
		t.Fatal(err)
	}
	defer handle.Release()
	if err := handle.Abort(ctx, nil); !errors.Is(err, vfs.ErrInvalidSealedObject) {
		t.Fatalf("cleanup without recorded ETag returned %v", err)
	}
	data, exists := store.Get(stage)
	if !exists || string(data) != "different staging contents" || store.Count("DELETE") != 0 {
		t.Fatal("cleanup changed staging without matching ownership")
	}
}

func TestWebDAVLockOwnershipSurvivesRestartAndRejectsLostLock(t *testing.T) {
	for _, loseLock := range []bool{false, true} {
		t.Run(fmt.Sprint(loseLock), func(t *testing.T) {
			store := filetest.New()
			store.DAVLocks = true
			store.DAVWeakETag = true
			_, p, u, mode := fileDriver(t, "webdav/strict", store)
			scope, bm := fileMemory(t)
			ctx := context.Background()
			i := fileIntent(t, p, u, mode)
			var ownership []byte
			h, err := p.BeginPublish(ctx, i, func(_ context.Context, b []byte) error { ownership = append([]byte(nil), b...); return nil }, scope, bm)
			if err != nil {
				if h != nil {
					_ = h.Release()
				}
				t.Fatal(err)
			}
			data := []byte("locked payload")
			if _, err := h.Write(ctx, data); err != nil {
				_ = h.Release()
				t.Fatal(err)
			}
			sealed, err := h.Seal(ctx, fileEvidence(data))
			_ = h.Release()
			store.DAVWeakETag = false
			if err != nil {
				t.Fatal(err)
			}
			if bytes.Contains(ownership, []byte("urn:uuid:")) || bytes.Contains(sealed.RecoveryToken, []byte("urn:uuid:")) {
				t.Fatal("persisted lock bearer token")
			}
			if loseLock {
				store.DropDAVLocks()
			}
			h, err = p.ResumePublish(ctx, i, ownership, &sealed, scope, bm)
			if err != nil {
				t.Fatal(err)
			}
			defer h.Release()
			_, err = h.Publish(ctx, sealed)
			if loseLock {
				if !errors.Is(err, vfs.ErrInvalidSealedObject) {
					t.Fatalf("lost lock publish: %v", err)
				}
				if err := h.Abort(ctx, &sealed); !errors.Is(err, vfs.ErrInvalidSealedObject) {
					t.Fatalf("lost lock cleanup: %v", err)
				}
				if store.Count("DELETE") != 0 || store.Count("MOVE") != 0 || store.StagingCount() != 1 {
					t.Fatal("modified staging after losing ownership")
				}
			} else {
				if err != nil {
					t.Fatal(err)
				}
				if _, err := h.Verify(ctx, sealed, true); err != nil {
					t.Fatal(err)
				}
				if err := h.Abort(ctx, &sealed); err != nil {
					t.Fatal(err)
				}
			}
		})
	}
}

func TestFilePublishSealResumeAndPhysicalReadback(t *testing.T) {
	for _, kind := range fileProtocols {
		for _, size := range []int{0, 200013} {
			t.Run(fmt.Sprintf("%s/%d", kind, size), func(t *testing.T) {
				store := filetest.New()
				_, p, u, mode := fileDriver(t, kind, store)
				scope, bm := fileMemory(t)
				ctx := context.Background()
				intent := fileIntent(t, p, u, mode)
				var ownership []byte
				callbacks := 0
				h, err := p.BeginPublish(ctx, intent, func(_ context.Context, b []byte) error {
					callbacks++
					ownership = append([]byte(nil), b...)
					return nil
				}, scope, bm)
				if err != nil {
					t.Fatal(err)
				}
				defer h.Release()
				if callbacks != 2 {
					t.Fatalf("ownership callbacks=%d", callbacks)
				}
				data := bytes.Repeat([]byte("x"), size)
				if _, err = h.Write(ctx, data); err != nil {
					t.Fatal(err)
				}
				sealed, err := h.Seal(ctx, fileEvidence(data))
				if err != nil {
					t.Fatal(err)
				}
				if _, ok := store.Get("/result.bin"); ok {
					t.Fatal("Seal published final")
				}
				if callbacks != 3 {
					t.Fatal("sealed identity not durably recorded")
				}
				if err = h.Release(); err != nil {
					t.Fatal(err)
				}
				if scope.Used() != 0 {
					t.Fatal("Release retained memory")
				}
				h, err = p.ResumePublish(ctx, intent, ownership, &sealed, scope, bm)
				if err != nil {
					t.Fatal(err)
				}
				defer h.Release()
				if _, err = h.Verify(ctx, sealed, false); err != nil {
					t.Fatal(err)
				}
				if _, err = h.Publish(ctx, sealed); err != nil {
					t.Fatal(err)
				}
				// Misleading Content-Encoding metadata must never decode physical bytes.
				store.SetFault("content_encoding")
				if _, err = h.Verify(ctx, sealed, true); err != nil {
					t.Fatal(err)
				}
				store.SetFault("")
				if err = h.Abort(ctx, &sealed); err != nil {
					t.Fatal(err)
				}
				if b, ok := store.Get("/result.bin"); !ok || !bytes.Equal(b, data) {
					t.Fatal("final removed or changed")
				}
				if store.StagingCount() != 0 {
					t.Fatal("owned staging leaked")
				}
				if strings.HasPrefix(kind, "sftp/strict") && store.Count("hardlink") != 1 {
					t.Fatal("strict did not hardlink")
				}
				if strings.HasPrefix(kind, "sftp/compatible") && store.Count("rename") != 1 {
					t.Fatal("compatible did not standard rename")
				}
			})
		}
	}
}

type fileSaveFault struct {
	exportpkg.ManifestStore
	state     exportpkg.ExportState
	after     bool
	used      bool
	ownership bool
}

func (s *fileSaveFault) Save(ctx context.Context, m *exportpkg.Manifest) error {
	match := !s.used && (m.ExportState == s.state || s.ownership && len(m.Publish.Ownership) > 0)
	if match {
		s.used = true
		if !s.after {
			return errors.New("save fault")
		}
	}
	if err := s.ManifestStore.Save(ctx, m); err != nil {
		return err
	}
	if match {
		return errors.New("save fault")
	}
	return nil
}
func fileCoordinator(t *testing.T, kind string, store *filetest.Store) (*exportpkg.PublishCoordinator, *exportpkg.Manifest, *fileSaveFault) {
	t.Helper()
	d, p, u, mode := fileDriver(t, kind, store)
	scope, bm := fileMemory(t)
	i := fileIntent(t, p, u, mode)
	m, e := exportpkg.NewPublishManifest(i.JobID, "query", "source", "csv", "identity", i)
	if e != nil {
		t.Fatal(e)
	}
	dir := t.TempDir()
	ms, e := exportpkg.NewLocalManifestStore(filepath.Join(dir, i.JobID+".json"), 0, scope, bm)
	if e != nil {
		t.Fatal(e)
	}
	fault := &fileSaveFault{ManifestStore: ms}
	registry, e := vfs.NewRegistry(d)
	if e != nil {
		t.Fatal(e)
	}
	return &exportpkg.PublishCoordinator{ManifestDir: dir, Scope: scope, Buffers: bm, Registry: registry, Store: func(string) (exportpkg.ManifestStore, error) { return fault, nil }}, m, fault
}
func writeFilePayload(ctx context.Context, s exportpkg.ByteSink) error {
	_, err := s.Write(ctx, []byte("raw physical bytes"))
	return err
}

func TestFilePublishCoordinatorLostResponsesAndStateGates(t *testing.T) {
	for _, kind := range fileProtocols {
		for _, fault := range []string{"publish_lost", "publish_before", "create_lost", "cleanup_error"} {
			if strings.HasPrefix(kind, "ftp") && fault == "create_lost" {
				continue
			}
			t.Run(kind+"/"+fault, func(t *testing.T) {
				store := filetest.New()
				c, m, ms := fileCoordinator(t, kind, store)
				store.SetFault(fault)
				result, err := c.Run(context.Background(), m, writeFilePayload)
				if fault == "cleanup_error" {
					if err != nil || result.JobID != m.JobID {
						t.Fatal(err)
					}
				} else if err == nil {
					t.Fatal("lost response succeeded")
				}
				unknown := fault == "publish_lost" || fault == "publish_before"
				if errors.Is(err, exportpkg.ErrCommitUncertain) != unknown {
					t.Fatalf("uncertainty=%t err=%v", unknown, err)
				}
				loaded, e := ms.Load(context.Background())
				if e != nil {
					t.Fatal(e)
				}
				if unknown && loaded.ExportState != exportpkg.ExportStateCommitting {
					t.Fatal(loaded.ExportState)
				}
				if fault == "create_lost" && (!loaded.Publish.CleanupPending || store.StagingCount() != 1) {
					t.Fatal("unknown ownership incorrectly cleaned")
				}
				store.SetFault("")
				_, err = c.Recover(context.Background(), m.JobID)
				if fault == "create_lost" {
					if !errors.Is(err, exportpkg.ErrExportNotRecoverable) {
						t.Fatal(err)
					}
					return
				}
				if err != nil {
					t.Fatal(err)
				}
				before := store.Count("MOVE") + store.Count("hardlink") + store.Count("rename") + store.Count("RNTO")
				if _, err = c.Recover(context.Background(), m.JobID); err != nil {
					t.Fatal(err)
				}
				if before != store.Count("MOVE")+store.Count("hardlink")+store.Count("rename")+store.Count("RNTO") {
					t.Fatal("committed recovery republished")
				}
				store.Remove("/result.bin")
				if _, err = c.Recover(context.Background(), m.JobID); !errors.Is(err, exportpkg.ErrExportNotRecoverable) {
					t.Fatalf("missing committed final=%v", err)
				}
			})
		}
	}
}

func TestFilePublishDurableSaveBoundaries(t *testing.T) {
	for _, kind := range fileProtocols {
		for _, state := range []exportpkg.ExportState{exportpkg.ExportStatePrepared, exportpkg.ExportStateCommitting, exportpkg.ExportStateCommitted} {
			for _, after := range []bool{false, true} {
				t.Run(fmt.Sprintf("%s/%s/%t", kind, state, after), func(t *testing.T) {
					store := filetest.New()
					c, m, ms := fileCoordinator(t, kind, store)
					ms.state = state
					ms.after = after
					if _, err := c.Run(context.Background(), m, writeFilePayload); err == nil {
						t.Fatal("save fault ignored")
					}
					if state != exportpkg.ExportStateCommitted {
						if _, exists := store.Get("/result.bin"); exists {
							t.Fatal("publication crossed failed save gate")
						}
					}
					_, err := c.Recover(context.Background(), m.JobID)
					if state == exportpkg.ExportStatePrepared && !after {
						if !errors.Is(err, exportpkg.ErrExportNotRecoverable) {
							t.Fatal(err)
						}
					} else if err != nil {
						t.Fatal(err)
					}
				})
			}
		}
	}
}

func TestFilePublishConcurrentRecoveryAndConflictingFinal(t *testing.T) {
	for _, kind := range fileProtocols {
		t.Run(kind, func(t *testing.T) {
			store := filetest.New()
			c, m, ms := fileCoordinator(t, kind, store)
			ms.state = exportpkg.ExportStatePrepared
			ms.after = true
			if _, err := c.Run(context.Background(), m, writeFilePayload); err == nil {
				t.Fatal("missing fault")
			}
			var wg sync.WaitGroup
			for n := 0; n < 6; n++ {
				wg.Add(1)
				go func() {
					defer wg.Done()
					if _, err := c.Recover(context.Background(), m.JobID); err != nil {
						t.Error(err)
					}
				}()
			}
			wg.Wait()
			store.Put("/result.bin", []byte("same length altered"))
			if _, err := c.Recover(context.Background(), m.JobID); !errors.Is(err, exportpkg.ErrCommitConflict) {
				t.Fatal(err)
			}
		})
	}
}

func TestFilePublishStrictRequirementsAndRaces(t *testing.T) {
	for _, kind := range fileProtocols {
		t.Run(kind, func(t *testing.T) {
			store := filetest.New()
			_, p, u, mode := fileDriver(t, kind, store)
			scope, bm := fileMemory(t)
			ctx := context.Background()
			i := fileIntent(t, p, u, mode)
			h, e := p.BeginPublish(ctx, i, func(context.Context, []byte) error { return nil }, scope, bm)
			if e != nil {
				t.Fatal(e)
			}
			defer h.Release()
			_, _ = h.Write(ctx, []byte("payload"))
			s, e := h.Seal(ctx, fileEvidence([]byte("payload")))
			if e != nil {
				t.Fatal(e)
			}
			store.Mu.Lock()
			store.BeforePublish = func() { store.Put("/result.bin", []byte("competitor")) }
			store.Mu.Unlock()
			_, e = h.Publish(ctx, s)
			if strings.HasPrefix(kind, "ftp") {
				// The intentionally documented check/rename race can overwrite.
				if e != nil {
					t.Fatal(e)
				}
				b, _ := store.Get("/result.bin")
				if string(b) != "payload" {
					t.Fatal("fixture failed to exercise TOCTOU")
				}
			} else {
				if !errors.Is(e, vfs.ErrPublishConflict) {
					t.Fatal(e)
				}
				b, _ := store.Get("/result.bin")
				if string(b) != "competitor" {
					t.Fatal("competitor overwritten")
				}
			}
		})
	}
	for _, kind := range []string{"sftp/strict", "webdav/strict", "ftp/compatible", "ftps/compatible"} {
		t.Run(kind+"/unsupported", func(t *testing.T) {
			store := filetest.New()
			store.Hardlink = "2"
			d, p, u, _ := fileDriver(t, kind, store)
			if dav, ok := d.(*webdav.Driver); ok {
				dav.AtomicMoveVerified = false
			}
			job, _ := vfs.NewPublishJobID()
			if _, err := p.PlanPublish(context.Background(), u, job, vfs.PublishStrict); !errors.Is(err, vfs.ErrWriteUnsupported) {
				t.Fatal(err)
			}
			if store.Count("create")+store.Count("PUT")+store.Count("STOR") != 0 {
				t.Fatal("unsupported strict created payload")
			}
		})
	}
}

func TestFilePublishTokensOwnershipAndCancellation(t *testing.T) {
	for _, kind := range fileProtocols {
		t.Run(kind, func(t *testing.T) {
			store := filetest.New()
			_, p, u, mode := fileDriver(t, kind, store)
			scope, bm := fileMemory(t)
			ctx := context.Background()
			i := fileIntent(t, p, u, mode)
			var ownership []byte
			h, e := p.BeginPublish(ctx, i, func(_ context.Context, b []byte) error { ownership = append([]byte(nil), b...); return nil }, scope, bm)
			if e != nil {
				t.Fatal(e)
			}
			_, _ = h.Write(ctx, []byte("hello"))
			s, e := h.Seal(ctx, fileEvidence([]byte("hello")))
			if e != nil {
				t.Fatal(e)
			}
			_ = h.Release()
			for _, change := range []string{"mode", "job", "destination", "scope", "intent", "ownership", "sealed"} {
				bad := i
				o := append([]byte(nil), ownership...)
				sealed := s
				sealed.RecoveryToken = append([]byte(nil), s.RecoveryToken...)
				switch change {
				case "mode":
					if mode == vfs.PublishStrict {
						bad.Mode = vfs.PublishCompatible
					} else {
						bad.Mode = vfs.PublishStrict
					}
				case "job":
					bad.JobID = strings.Repeat("0", 32)
				case "destination":
					bad.Destination += "changed"
				case "scope":
					bad.ScopeKey = "bad"
				case "intent":
					bad.Token = []byte("{}")
				case "ownership":
					o = []byte("{}")
				case "sealed":
					sealed.Checksum = strings.Repeat("0", 64)
				}
				r, err := p.ResumePublish(ctx, bad, o, &sealed, scope, bm)
				if r != nil {
					_ = r.Release()
				}
				if err == nil {
					t.Fatalf("accepted changed %s", change)
				}
			}
			store.Put("/result.bin", []byte("final"))
			h, e = p.ResumePublish(ctx, i, ownership, &s, scope, bm)
			if e != nil {
				t.Fatal(e)
			}
			defer h.Release()
			if _, e = h.Verify(ctx, s, true); !errors.Is(e, vfs.ErrPublishConflict) {
				t.Fatal(e)
			}
			store.Remove("/result.bin")
			store.Put("/.fssql-"+i.JobID+".staging", []byte("other"))
			if _, e = h.Verify(ctx, s, false); !errors.Is(e, vfs.ErrInvalidSealedObject) {
				t.Fatal(e)
			}
			cancelled, cancel := context.WithCancel(ctx)
			cancel()
			if _, e = h.Verify(cancelled, s, true); e == nil {
				t.Fatal("cancel ignored")
			}
			if _, ok := store.Get("/result.bin"); ok {
				t.Fatal("verification recreated final")
			}
		})
	}
}

func TestFilePublishOwnershipSaveStopsPayload(t *testing.T) {
	for _, kind := range fileProtocols {
		t.Run(kind, func(t *testing.T) {
			store := filetest.New()
			c, m, ms := fileCoordinator(t, kind, store)
			ms.ownership = true
			if _, e := c.Run(context.Background(), m, writeFilePayload); e == nil {
				t.Fatal("ownership save succeeded")
			}
			if store.Count("create")+store.Count("PUT")+store.Count("STOR") != 0 {
				t.Fatal("created payload after failed pending save")
			}
			if _, e := os.Stat(filepath.Join(c.ManifestDir, m.JobID+".json")); e != nil {
				t.Fatal(e)
			}
		})
	}
}

func TestFilePublishAcquiredOwnershipFailureAndQuota(t *testing.T) {
	for _, kind := range fileProtocols {
		t.Run(kind, func(t *testing.T) {
			store := filetest.New()
			_, p, u, mode := fileDriver(t, kind, store)
			scope, bm := fileMemory(t)
			ctx := context.Background()
			i := fileIntent(t, p, u, mode)
			calls := 0
			h, err := p.BeginPublish(ctx, i, func(context.Context, []byte) error {
				calls++
				if calls == 2 {
					return errors.New("ownership not saved")
				}
				return nil
			}, scope, bm)
			if err == nil || h == nil {
				t.Fatal("missing ownership failure")
			}
			defer h.Release()
			if n, e := h.Write(ctx, []byte("forbidden")); e == nil || n != 0 {
				t.Fatal("payload continued after ownership failure")
			}
			if e := h.Abort(ctx, nil); e != nil {
				t.Fatal(e)
			}
			_ = h.Release()
			if store.StagingCount() != 0 {
				t.Fatal("owned failed staging was not cleaned")
			}
			if _, ok := store.Get("/result.bin"); ok {
				t.Fatal("failure published final")
			}
			limited := memory.NewManager(512 << 10).NewScope()
			defer limited.Close()
			i = fileIntent(t, p, u, mode)
			h, err = p.BeginPublish(ctx, i, func(context.Context, []byte) error { return nil }, limited, bm)
			if err != nil {
				t.Fatal(err)
			}
			_, err = h.Write(ctx, []byte("data"))
			if err != nil {
				t.Fatal(err)
			}
			if _, err = h.Seal(ctx, fileEvidence([]byte("data"))); err == nil {
				t.Fatal("readback bypassed quota")
			}
			if e := h.Abort(ctx, nil); e != nil {
				t.Fatal(e)
			}
			_ = h.Release()
			if limited.Used() != 0 || store.StagingCount() != 0 {
				t.Fatal("quota failure leaked resources")
			}
		})
	}
}

func TestFilePublishNoReplaceCapabilityRequiresEvidence(t *testing.T) {
	for _, kind := range []string{"sftp/strict", "webdav/strict", "ftp/compatible"} {
		t.Run(kind, func(t *testing.T) {
			store := filetest.New()
			d, _, u, _ := fileDriver(t, kind, store)
			r, err := vfs.NewRegistry(d)
			if err != nil {
				t.Fatal(err)
			}
			caps, err := r.EffectiveCapabilities(context.Background(), u)
			if err != nil {
				t.Fatal(err)
			}
			if caps.Has(vfs.NoReplacePublish) == strings.HasPrefix(kind, "ftp") {
				t.Fatal("incorrect no-replace capability")
			}
			if dav, ok := d.(*webdav.Driver); ok {
				dav.AtomicMoveVerified = false
				caps, err = r.EffectiveCapabilities(context.Background(), u)
				if err != nil || caps.Has(vfs.NoReplacePublish) {
					t.Fatal("OPTIONS inferred atomicity", err)
				}
			}
		})
	}
}
