package s3

import (
	"context"
	"io"
	"net/http"
	"net/http/httptest"
	"strconv"
	"sync"
	"testing"

	"github.com/steamwo/fssql/internal/core/memory"
	"github.com/steamwo/fssql/internal/storage/vfs"
)

type atomicPublishTestServer struct {
	mu             sync.Mutex
	final          []byte
	parts          [][]byte
	rejectComplete bool
	aborts         int
}

func (s *atomicPublishTestServer) handler(w http.ResponseWriter, r *http.Request) {
	s.mu.Lock()
	defer s.mu.Unlock()
	q := r.URL.Query()
	switch {
	case r.Method == http.MethodPost && q.Has("uploads"):
		w.Header().Set("Content-Type", "application/xml")
		_, _ = io.WriteString(w, `<InitiateMultipartUploadResult><UploadId>atomic-upload</UploadId></InitiateMultipartUploadResult>`)
	case r.Method == http.MethodPut && q.Get("uploadId") != "":
		body, _ := io.ReadAll(r.Body)
		s.parts = append(s.parts, append([]byte(nil), body...))
		w.Header().Set("ETag", `"part-`+strconv.Itoa(len(s.parts))+`"`)
		w.WriteHeader(http.StatusOK)
	case r.Method == http.MethodPost && q.Get("uploadId") != "":
		if s.rejectComplete {
			w.WriteHeader(http.StatusPreconditionFailed)
			return
		}
		s.final = s.final[:0]
		for _, part := range s.parts {
			s.final = append(s.final, part...)
		}
		w.WriteHeader(http.StatusOK)
	case r.Method == http.MethodDelete && q.Get("uploadId") != "":
		s.aborts++
		w.WriteHeader(http.StatusNoContent)
	case r.Method == http.MethodPut:
		body, _ := io.ReadAll(r.Body)
		s.final = append(s.final[:0], body...)
		w.WriteHeader(http.StatusOK)
	case r.Method == http.MethodHead:
		w.Header().Set("Content-Length", strconv.Itoa(len(s.final)))
		w.Header().Set("ETag", `"atomic-final"`)
		w.WriteHeader(http.StatusOK)
	default:
		http.Error(w, "unexpected request", http.StatusBadRequest)
	}
}

func newAtomicPublishTestDriver(t *testing.T, state *atomicPublishTestServer) (*Driver, *httptest.Server) {
	t.Helper()
	server := httptest.NewServer(http.HandlerFunc(state.handler))
	driver, err := NewDriver(Config{
		Endpoint:          server.URL,
		Region:            "test-region",
		ForcePathStyle:    true,
		AllowInsecureHTTP: true,
		CredentialScope:   "scope",
		CredentialVersion: "v1",
		SecurityProfile:   "test",
		MaxRetries:        1,
	}, Credentials{AccessKey: "access", SecretKey: "secret"}, nil)
	if err != nil {
		server.Close()
		t.Fatal(err)
	}
	return driver, server
}

func TestS3AtomicPublishCapabilityAndContract(t *testing.T) {
	state := &atomicPublishTestServer{final: []byte("old-complete-object")}
	driver, server := newAtomicPublishTestDriver(t, state)
	defer server.Close()
	if !driver.StaticCapabilities().Has(vfs.AtomicPublishWrite) {
		t.Fatal("S3 must advertise proven AtomicPublishWrite")
	}
	registry, err := vfs.NewRegistry(driver)
	if err != nil {
		t.Fatal(err)
	}
	uri, _ := vfs.ParseURI("s3://bucket/result.parquet")
	mm := memory.NewManager(32 << 20)
	scope := mm.NewScope()
	handle, err := registry.OpenAtomicPublishWrite(context.Background(), uri, vfs.WriteOptions{}, scope, memory.NewBufferManager(nil))
	if err != nil {
		t.Fatal(err)
	}

	// Force at least one uploaded multipart part before Close. Uploaded parts
	// must not replace or expose the final object.
	payload := make([]byte, defaultMultipartPartSize)
	for i := range payload {
		payload[i] = byte(i)
	}
	if n, err := handle.Write(context.Background(), payload); err != nil || n != len(payload) {
		t.Fatalf("Write n=%d err=%v", n, err)
	}
	state.mu.Lock()
	beforeClose := string(state.final)
	parts := len(state.parts)
	state.mu.Unlock()
	if beforeClose != "old-complete-object" || parts != 1 {
		t.Fatalf("final changed before publish: final=%q parts=%d", beforeClose, parts)
	}

	identity, err := handle.Close(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	state.mu.Lock()
	published := append([]byte(nil), state.final...)
	state.mu.Unlock()
	if len(published) != len(payload) || identity.Size != int64(len(payload)) {
		t.Fatalf("published=%d identity=%+v", len(published), identity)
	}
	if scope.Used() != 0 {
		t.Fatalf("managed memory leak=%d", scope.Used())
	}
}

func TestS3AtomicPublishFailureLeavesPreviousObjectAndAbortCleansOwnership(t *testing.T) {
	state := &atomicPublishTestServer{final: []byte("old-complete-object"), rejectComplete: true}
	driver, server := newAtomicPublishTestDriver(t, state)
	defer server.Close()
	registry, _ := vfs.NewRegistry(driver)
	uri, _ := vfs.ParseURI("s3://bucket/result.csv")
	mm := memory.NewManager(32 << 20)
	scope := mm.NewScope()
	handle, err := registry.OpenAtomicPublishWrite(context.Background(), uri, vfs.WriteOptions{}, scope, memory.NewBufferManager(nil))
	if err != nil {
		t.Fatal(err)
	}
	if _, err := handle.Write(context.Background(), []byte("new-data")); err != nil {
		t.Fatal(err)
	}
	if _, err := handle.Close(context.Background()); err == nil {
		t.Fatal("expected atomic completion failure")
	}
	state.mu.Lock()
	afterFailure := string(state.final)
	state.mu.Unlock()
	if afterFailure != "old-complete-object" {
		t.Fatalf("failed publish replaced final object: %q", afterFailure)
	}
	if err := handle.Abort(context.Background()); err != nil {
		t.Fatal(err)
	}
	state.mu.Lock()
	aborts := state.aborts
	state.mu.Unlock()
	if aborts != 1 {
		t.Fatalf("multipart aborts=%d", aborts)
	}
	if scope.Used() != 0 {
		t.Fatalf("managed memory leak=%d", scope.Used())
	}
}
