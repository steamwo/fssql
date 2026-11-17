package s3

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"path/filepath"
	"strings"
	"sync"
	"testing"

	"github.com/steamwo/fssql/internal/core/memory"
	"github.com/steamwo/fssql/internal/core/types"
	exportpkg "github.com/steamwo/fssql/internal/export"
	"github.com/steamwo/fssql/internal/storage/remote/s3/s3test"
	"github.com/steamwo/fssql/internal/storage/vfs"
)

func stagedS3Fixture(t *testing.T) (*Driver, *s3test.Server, *memory.Scope, *memory.BufferManager) {
	t.Helper()
	server := s3test.New()
	t.Cleanup(server.Close)
	d, err := NewDriver(Config{Endpoint: server.URL(), Region: "test-region", ForcePathStyle: true, AllowInsecureHTTP: true, CredentialScope: "scope", CredentialVersion: "v1", SecurityProfile: "test", MaxRetries: 1}, Credentials{AccessKey: "access", SecretKey: "secret"}, nil)
	if err != nil {
		t.Fatal(err)
	}
	scope := memory.NewManager(64 << 20).NewScope()
	bm := memory.NewBufferManager(nil)
	t.Cleanup(func() {
		if scope.Used() != 0 {
			t.Errorf("managed memory leak: %d", scope.Used())
		}
		_ = scope.Close()
	})
	return d, server, scope, bm
}
func evidence(data []byte) vfs.WriteEvidence {
	sum := sha256.Sum256(data)
	return vfs.WriteEvidence{Size: int64(len(data)), Checksum: hex.EncodeToString(sum[:])}
}
func stagedS3Intent(t *testing.T, d *Driver) vfs.PublishIntent {
	t.Helper()
	job, err := vfs.NewPublishJobID()
	if err != nil {
		t.Fatal(err)
	}
	uri, _ := vfs.ParseURI("s3://bucket/result.bin")
	intent, err := d.PlanPublish(context.Background(), uri, job, vfs.PublishStrict)
	if err != nil {
		t.Fatal(err)
	}
	return intent
}

func TestS3StagedSealAndMultipartRecoveryEvidence(t *testing.T) {
	for _, size := range []int{0, 19, defaultMultipartPartSize + 17} {
		t.Run(string(rune('a'+size%26)), func(t *testing.T) {
			d, server, scope, bm := stagedS3Fixture(t)
			server.SetPageSize(1)
			intent := stagedS3Intent(t, d)
			var ownership []byte
			callbacks := 0
			h, err := d.BeginPublish(context.Background(), intent, func(_ context.Context, p []byte) error {
				callbacks++
				ownership = append([]byte(nil), p...)
				return nil
			}, scope, bm)
			if err != nil {
				t.Fatal(err)
			}
			data := bytes.Repeat([]byte{'a'}, size)
			if _, err := h.Write(context.Background(), data); err != nil {
				t.Fatal(err)
			}
			sealed, err := h.Seal(context.Background(), evidence(data))
			if err != nil {
				t.Fatal(err)
			}
			if server.Counts("complete") != 0 || server.Counts("put") != 0 {
				t.Fatal("Seal published final")
			}
			if callbacks < 2 {
				t.Fatal("missing durable ownership callbacks")
			}
			if scope.Used() >= defaultMultipartPartSize {
				t.Fatal("sealed part buffer retained")
			}
			if err := h.Release(); err != nil {
				t.Fatal(err)
			}
			h, err = d.ResumePublish(context.Background(), intent, ownership, &sealed, scope, bm)
			if err != nil {
				t.Fatal(err)
			}
			if _, err := h.Verify(context.Background(), sealed, false); err != nil {
				t.Fatal(err)
			}
			if _, err := h.Publish(context.Background(), sealed); err != nil {
				t.Fatal(err)
			}
			if _, err := h.Verify(context.Background(), sealed, true); err != nil {
				t.Fatal(err)
			}
			if err := h.Release(); err != nil {
				t.Fatal(err)
			}
			if server.UploadCount() != 0 {
				t.Fatal("Complete did not consume upload")
			}
			// Complete invalidates UploadID; final verification must not ListParts.
			before := server.Counts("list")
			h, err = d.ResumePublish(context.Background(), intent, ownership, &sealed, scope, bm)
			if err != nil {
				t.Fatal(err)
			}
			if _, err := h.Verify(context.Background(), sealed, true); err != nil {
				t.Fatal(err)
			}
			_ = h.Release()
			if server.Counts("list") != before {
				t.Fatal("successful final recovery depended on UploadID")
			}
			if obj, ok := server.Object("/bucket/result.bin"); !ok || !bytes.Equal(obj.Data, data) {
				t.Fatal("final content mismatch")
			}
			for _, event := range server.Events() {
				if event.Operation == "complete" || event.Operation == "put" {
					if event.IfNoneMatch != "*" {
						t.Fatal("publication lacks no-replace condition")
					}
				}
				if event.Operation == "get" && (event.Version == "" || event.AcceptEncoding != "identity") {
					t.Fatal("verification did not pin raw object version")
				}
			}
		})
	}
}

func TestS3StagedListPartsRejectsChangedEvidence(t *testing.T) {
	for _, mutation := range []string{"missing", "etag", "size", "checksum", "upload", "foreign", "malformed"} {
		t.Run(mutation, func(t *testing.T) {
			d, server, scope, bm := stagedS3Fixture(t)
			intent := stagedS3Intent(t, d)
			var ownership []byte
			h, err := d.BeginPublish(context.Background(), intent, func(_ context.Context, p []byte) error { ownership = append([]byte(nil), p...); return nil }, scope, bm)
			if err != nil {
				t.Fatal(err)
			}
			data := []byte("physical bytes")
			_, _ = h.Write(context.Background(), data)
			sealed, err := h.Seal(context.Background(), evidence(data))
			if err != nil {
				t.Fatal(err)
			}
			_ = h.Release()
			if mutation == "foreign" {
				server.SetFault("list_foreign")
			} else if mutation == "malformed" {
				server.SetFault("list_xml")
			} else {
				server.MutatePart(1, mutation)
			}
			h, err = d.ResumePublish(context.Background(), intent, ownership, &sealed, scope, bm)
			if err != nil {
				t.Fatal(err)
			}
			if _, err := h.Verify(context.Background(), sealed, false); err == nil {
				t.Fatal("changed parts accepted")
			}
			_ = h.Release()
			if server.Counts("complete") != 0 || server.Counts("put") != 0 {
				t.Fatal("invalid upload published")
			}
		})
	}
}

type stagedS3Store struct {
	exportpkg.ManifestStore
	stop  string
	after bool
	panic bool
	err   error
}

func (s *stagedS3Store) Save(ctx context.Context, m *exportpkg.Manifest) error {
	match := string(m.ExportState) == s.stop
	if s.stop == "OWNED" {
		var state s3PublishState
		_ = json.Unmarshal(m.Publish.Ownership, &state)
		match = state.UploadID != "" && len(state.Parts) == 0
	}
	trigger := func() error {
		if s.panic {
			panic("crash")
		}
		return s.err
	}
	if match && !s.after {
		return trigger()
	}
	if err := s.ManifestStore.Save(ctx, m); err != nil {
		return err
	}
	if match && s.after {
		return trigger()
	}
	return nil
}
func stagedS3Coordinator(t *testing.T, d *Driver, scope *memory.Scope, bm *memory.BufferManager) (*exportpkg.PublishCoordinator, *exportpkg.Manifest, *stagedS3Store) {
	t.Helper()
	intent := stagedS3Intent(t, d)
	m, err := exportpkg.NewPublishManifest(intent.JobID, "query", "source", "csv", "identity", intent)
	if err != nil {
		t.Fatal(err)
	}
	dir := t.TempDir()
	store, err := exportpkg.NewLocalManifestStore(filepath.Join(dir, intent.JobID+".json"), 0, scope, bm)
	if err != nil {
		t.Fatal(err)
	}
	fault := &stagedS3Store{ManifestStore: store}
	registry, _ := vfs.NewRegistry(d)
	c := &exportpkg.PublishCoordinator{ManifestDir: dir, Store: func(string) (exportpkg.ManifestStore, error) { return fault, nil }, Registry: registry, Scope: scope, Buffers: bm}
	return c, m, fault
}

func TestS3StagedCoordinatorFailureAndLostResponses(t *testing.T) {
	for _, fault := range []string{"complete_lost", "complete_before", "complete_xml", "complete_trailing", "complete_error", "complete_http_error", "complete_race", "get_lost", "create_lost", "abort_error"} {
		t.Run(fault, func(t *testing.T) {
			d, server, scope, bm := stagedS3Fixture(t)
			c, m, store := stagedS3Coordinator(t, d, scope, bm)
			server.SetFault(fault)
			_, err := c.Run(context.Background(), m, func(ctx context.Context, s exportpkg.ByteSink) error {
				_, err := s.Write(ctx, []byte("physical bytes"))
				if fault == "abort_error" && err == nil {
					return errors.New("format failure")
				}
				return err
			})
			if err == nil {
				t.Fatal("fault did not fail")
			}
			unknown := fault == "complete_lost" || fault == "complete_before" || fault == "complete_xml" || fault == "complete_trailing" || fault == "get_lost"
			if errors.Is(err, exportpkg.ErrCommitUncertain) != unknown {
				t.Fatalf("uncertainty=%v: %v", unknown, err)
			}
			loaded, e := store.Load(context.Background())
			if e != nil {
				t.Fatal(e)
			}
			if unknown && loaded.ExportState != exportpkg.ExportStateCommitting {
				t.Fatal(loaded.ExportState)
			}
			if fault == "create_lost" && (!loaded.Publish.CleanupPending || server.UploadCount() != 1 || server.Counts("abort") != 0) {
				t.Fatal("unknown upload incorrectly claimed cleaned")
			}
			if server.Counts("complete") > 1 || server.Counts("create") > 1 {
				t.Fatal("unsafe implicit final/create retry")
			}
			server.SetFault("")
			if fault == "complete_lost" {
				obj, _ := server.Object("/bucket/result.bin")
				server.Put("/bucket/result.bin", obj.Data, "gzip")
			}
			_, err = c.Recover(context.Background(), m.JobID)
			if unknown {
				if err != nil {
					t.Fatal(err)
				}
			} else if err == nil {
				t.Fatal("failed job published during recovery")
			}
		})
	}
}

func TestS3StagedSaveFailuresRetainSealedUpload(t *testing.T) {
	for _, state := range []string{"PREPARED", "COMMITTING", "COMMITTED"} {
		for _, after := range []bool{false, true} {
			t.Run(state+map[bool]string{true: "/after", false: "/before"}[after], func(t *testing.T) {
				d, server, scope, bm := stagedS3Fixture(t)
				c, m, store := stagedS3Coordinator(t, d, scope, bm)
				store.stop = state
				store.after = after
				store.err = errors.New("save failure")
				_, err := c.Run(context.Background(), m, func(ctx context.Context, s exportpkg.ByteSink) error {
					_, err := s.Write(ctx, []byte("payload"))
					return err
				})
				if !errors.Is(err, store.err) {
					t.Fatal(err)
				}
				if errors.Is(err, exportpkg.ErrCommitUncertain) != (state == "COMMITTED") {
					t.Fatal(err)
				}
				if state != "COMMITTED" && (server.Counts("complete") != 0 || server.Counts("abort") != 0 || server.UploadCount() != 1) {
					t.Fatal("failed durable save published or discarded staging")
				}
				store.stop = ""
				if state == "PREPARED" && !after {
					if _, err := c.Recover(context.Background(), m.JobID); !errors.Is(err, exportpkg.ErrExportNotRecoverable) {
						t.Fatal(err)
					}
				} else if _, err := c.Recover(context.Background(), m.JobID); err != nil {
					t.Fatal(err)
				}
			})
		}
	}
}

func TestS3StagedConcurrentRecoveryPublishesOnce(t *testing.T) {
	d, server, scope, bm := stagedS3Fixture(t)
	c, m, store := stagedS3Coordinator(t, d, scope, bm)
	store.stop = "PREPARED"
	store.after = true
	store.err = errors.New("stop after prepared")
	_, _ = c.Run(context.Background(), m, func(ctx context.Context, s exportpkg.ByteSink) error {
		_, err := s.Write(ctx, []byte("payload"))
		return err
	})
	store.stop = ""
	var wg sync.WaitGroup
	results := make(chan error, 6)
	for i := 0; i < 6; i++ {
		wg.Add(1)
		go func() { defer wg.Done(); _, err := c.Recover(context.Background(), m.JobID); results <- err }()
	}
	wg.Wait()
	close(results)
	for err := range results {
		if err != nil {
			t.Fatal(err)
		}
	}
	if server.Counts("complete") != 1 {
		t.Fatal("concurrent recovery published more than once")
	}
}

func TestS3StagedMaximumPartDescriptorRemainsSupported(t *testing.T) {
	d, server, scope, bm := stagedS3Fixture(t)
	intent := stagedS3Intent(t, d)
	metadata, err := scope.Reserve(memory.TagMetadata, 0)
	if err != nil {
		t.Fatal(err)
	}
	defer metadata.Release()
	state := s3PublishState{Version: 1, IntentHash: publishIntentHash(intent), Kind: "multipart", UploadID: "upload", Parts: make([]multipartPart, maxMultipartParts), Size: int64(maxMultipartParts) * defaultMultipartPartSize, Checksum: emptyPayloadSHA256}
	for i := range state.Parts {
		state.Parts[i] = multipartPart{Number: i + 1, ETag: strings.Repeat("e", 32), Size: defaultMultipartPartSize, ChecksumSHA256: strings.Repeat("c", 44)}
	}
	h := &s3PublishHandle{metadata: metadata}
	token, err := h.encodeState(state)
	if err != nil {
		t.Fatal(err)
	}
	sealed := vfs.SealedObject{Size: state.Size, Checksum: state.Checksum, RecoveryToken: token}
	state.Size = 0
	state.Checksum = ""
	ownership, err := h.encodeState(state)
	if err != nil {
		t.Fatal(err)
	}
	resumed, err := d.ResumePublish(context.Background(), intent, ownership, &sealed, scope, bm)
	if err != nil {
		t.Fatal(err)
	}
	_ = resumed.Release()
	if len(token) <= 1<<20 {
		t.Fatal("fixture did not exceed the former token limit")
	}
	if len(server.Events()) != 0 {
		t.Fatal("descriptor validation issued network requests")
	}
}

func TestS3StagedRestartAtDurableBoundaries(t *testing.T) {
	for _, state := range []string{"OWNED", "PREPARED", "COMMITTING", "COMMITTED"} {
		for _, empty := range []bool{false, true} {
			if state == "OWNED" && empty {
				continue
			}
			t.Run(state+map[bool]string{true: "/empty", false: "/multipart"}[empty], func(t *testing.T) {
				d, server, scope, bm := stagedS3Fixture(t)
				c, m, store := stagedS3Coordinator(t, d, scope, bm)
				store.stop = state
				store.after = true
				store.panic = true
				crashed := false
				func() {
					defer func() {
						if r := recover(); r != nil {
							if r != "crash" {
								panic(r)
							}
							crashed = true
						}
					}()
					_, err := c.Run(context.Background(), m, func(ctx context.Context, s exportpkg.ByteSink) error {
						if empty {
							return nil
						}
						_, err := s.Write(ctx, []byte("payload"))
						return err
					})
					if err != nil {
						t.Fatal(err)
					}
				}()
				if !crashed {
					t.Fatal("crash not reached")
				}
				store.stop = ""
				store.panic = false
				_, err := c.Recover(context.Background(), m.JobID)
				if state == "OWNED" {
					if !errors.Is(err, exportpkg.ErrExportNotRecoverable) {
						t.Fatal(err)
					}
					if server.Counts("complete") != 0 || server.Counts("put") != 0 {
						t.Fatal("WRITING recovered as sealed")
					}
				} else {
					if err != nil {
						t.Fatal(err)
					}
					if _, err := c.Recover(context.Background(), m.JobID); err != nil {
						t.Fatal(err)
					}
					server.DeleteObject("/bucket/result.bin")
					if _, err := c.Recover(context.Background(), m.JobID); !errors.Is(err, exportpkg.ErrExportNotRecoverable) {
						t.Fatal(err)
					}
				}
			})
		}
	}
}

func TestS3StagedOwnershipPersistenceFailureStopsPayload(t *testing.T) {
	d, server, scope, bm := stagedS3Fixture(t)
	c, m, store := stagedS3Coordinator(t, d, scope, bm)
	store.stop = "OWNED"
	store.err = errors.New("ownership save failed")
	_, err := c.Run(context.Background(), m, func(ctx context.Context, s exportpkg.ByteSink) error {
		_, err := s.Write(ctx, bytes.Repeat([]byte{'x'}, defaultMultipartPartSize+1))
		return err
	})
	if !errors.Is(err, store.err) {
		t.Fatal(err)
	}
	if server.Counts("part") != 0 || server.Counts("complete") != 0 {
		t.Fatal("payload continued after ownership persistence failed")
	}
	if server.Counts("abort") != 1 || server.UploadCount() != 0 {
		t.Fatal("known upload not safely cleaned")
	}
}

func TestS3LegacyUncertainCloseKeepsOwnership(t *testing.T) {
	d, server, scope, bm := stagedS3Fixture(t)
	server.SetFault("complete_before")
	u, _ := vfs.ParseURI("s3://bucket/result.bin")
	h, err := d.OpenAtomicPublishWrite(context.Background(), u, vfs.WriteOptions{IfNoneMatch: true}, scope, bm)
	if err != nil {
		t.Fatal(err)
	}
	_, _ = h.Write(context.Background(), []byte("payload"))
	_, err = h.Close(context.Background())
	var uncertain *vfs.CommitUncertainError
	if !errors.Is(err, vfs.ErrCommitUncertain) || !errors.As(err, &uncertain) || len(uncertain.JobID) != 32 {
		t.Fatal(err)
	}
	if err := h.Abort(context.Background()); err != nil {
		t.Fatal(err)
	}
	if server.UploadCount() != 1 || server.Counts("abort") != 0 || scope.Used() != 0 {
		t.Fatal("unknown commit was aborted or leaked managed memory")
	}
}

func TestS3LegacyFormatWriterUncertaintyReleasesResources(t *testing.T) {
	for _, format := range []exportpkg.FileFormat{exportpkg.FormatCSV, exportpkg.FormatJSON, exportpkg.FormatNDJSON, exportpkg.FormatParquet} {
		t.Run(string(format), func(t *testing.T) {
			d, server, scope, bm := stagedS3Fixture(t)
			server.SetFault("complete_before")
			if format == exportpkg.FormatNDJSON {
				server.SetFault("put_before")
			}
			registry, _ := vfs.NewRegistry(d)
			pool, err := exportpkg.NewWriterPool(1, 1024, scope)
			if err != nil {
				t.Fatal(err)
			}
			config := exportpkg.FileWriterConfig{Format: format, Schema: []exportpkg.Column{{Name: "id", Type: types.LogicalType{ID: types.Integer}}}, CSV: exportpkg.CSVWriteOptions{Header: true}}
			if format == exportpkg.FormatCSV {
				config.Compression = "gzip"
			}
			factory, err := exportpkg.NewFileWriterFactory(config, registry, scope, bm, pool)
			if err != nil {
				t.Fatal(err)
			}
			manifest, err := exportpkg.NewManifest("legacy", "query", "source", "s3://bucket/", "csv", "identity", "atomic_publish", []string{"result.bin"})
			if err != nil {
				t.Fatal(err)
			}
			_ = manifest.TransitionExport(exportpkg.ExportStateWriting)
			router, err := exportpkg.NewFileRouter(exportpkg.RoutingConfig{Mode: exportpkg.LayoutSingleFile, SingleFilePath: "result.bin"}, config.Schema)
			if err != nil {
				t.Fatal(err)
			}
			writer, err := exportpkg.NewAtomicSingleFileDatasetWriter(manifest, router, factory)
			if err != nil {
				t.Fatal(err)
			}
			if err := writer.Close(context.Background()); !errors.Is(err, exportpkg.ErrCommitUncertain) {
				t.Fatal(err)
			}
			if pool.Active() != 0 || scope.Used() != 0 || server.Counts("abort") != 0 {
				t.Fatal("uncertain format close leaked resources or aborted remote ownership")
			}
			if manifest.FileStates[0].State == exportpkg.FileStateFailed {
				t.Fatal("unknown result marked FAILED")
			}
		})
	}
}

func TestS3StagedRejectsTokenScopeChanges(t *testing.T) {
	d, _, scope, bm := stagedS3Fixture(t)
	intent := stagedS3Intent(t, d)
	var own []byte
	h, err := d.BeginPublish(context.Background(), intent, func(_ context.Context, p []byte) error { own = append([]byte(nil), p...); return nil }, scope, bm)
	if err != nil {
		t.Fatal(err)
	}
	_, _ = h.Write(context.Background(), []byte("payload"))
	sealed, err := h.Seal(context.Background(), evidence([]byte("payload")))
	if err != nil {
		t.Fatal(err)
	}
	_ = h.Release()
	for _, change := range []string{"mode", "endpoint", "credential", "job", "target", "token", "parts", "version"} {
		t.Run(change, func(t *testing.T) {
			clone := *d
			cloneIntent := intent
			cloneSealed := sealed
			switch change {
			case "mode":
				cloneIntent.Mode = vfs.PublishCompatible
			case "endpoint":
				e := *d.endpoint
				e.Host = "other.invalid"
				clone.endpoint = &e
			case "credential":
				clone.creds.SecretKey = "different"
			case "job":
				cloneIntent.JobID = strings.Repeat("f", 32)
			case "target":
				cloneIntent.Destination = "s3://bucket/other"
			case "token":
				cloneSealed.RecoveryToken = []byte("{}")
			case "parts":
				state, _ := decodePublishState(sealed.RecoveryToken)
				state.Parts[0].ETag = "other"
				cloneSealed.RecoveryToken, _ = json.Marshal(state)
			case "version":
				state, _ := decodePublishState(sealed.RecoveryToken)
				state.Version = 2
				cloneSealed.RecoveryToken, _ = json.Marshal(state)
			}
			if h, err := clone.ResumePublish(context.Background(), cloneIntent, own, &cloneSealed, scope, bm); err == nil {
				_ = h.Release()
				t.Fatal("unauthorized token accepted")
			}
		})
	}
}
