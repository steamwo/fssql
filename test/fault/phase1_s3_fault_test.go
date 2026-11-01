package fault_test

import (
	"context"
	"errors"
	"fmt"
	"net/http"
	"net/http/httptest"
	"sync/atomic"
	"testing"
	"time"

	"github.com/steamwo/fssql/internal/core/memory"
	remotes3 "github.com/steamwo/fssql/internal/storage/remote/s3"
	"github.com/steamwo/fssql/internal/storage/vfs"
)

func newPhase1S3Driver(t *testing.T, server *httptest.Server, timeout time.Duration, retries int) *remotes3.Driver {
	t.Helper()
	driver, err := remotes3.NewDriver(remotes3.Config{
		Endpoint:          server.URL,
		Region:            "us-east-1",
		ForcePathStyle:    true,
		AllowInsecureHTTP: true,
		CredentialScope:   "phase1-fault",
		CredentialVersion: "v1",
		SecurityProfile:   "test",
		RequestTimeout:    timeout,
		MaxRetries:        retries,
	}, remotes3.Credentials{AccessKey: "test", SecretKey: "secret"}, nil)
	if err != nil {
		t.Fatal(err)
	}
	return driver
}

func phase1RangeRequest(t *testing.T) vfs.RangeRequest {
	t.Helper()
	u, err := vfs.ParseURI("s3://bucket/key.parquet")
	if err != nil {
		t.Fatal(err)
	}
	return vfs.RangeRequest{URI: u, Offset: 0, Length: 4}
}

func runPhase1Range(t *testing.T, driver *remotes3.Driver, ctx context.Context, req vfs.RangeRequest) (vfs.RangeResult, error, *memory.Scope) {
	t.Helper()
	mm := memory.NewManager(1 << 20)
	scope := mm.NewScope()
	bm := memory.NewBufferManager(nil)
	result, err := driver.ReadRange(ctx, req, scope, bm)
	return result, err, scope
}

func TestPhase1S3Fault503RetriesThenSucceeds(t *testing.T) {
	var attempts atomic.Int32
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if attempts.Add(1) < 3 {
			w.WriteHeader(http.StatusServiceUnavailable)
			return
		}
		w.Header().Set("Content-Range", "bytes 0-3/4")
		w.Header().Set("Content-Length", "4")
		w.Header().Set("ETag", `"ok"`)
		w.WriteHeader(http.StatusPartialContent)
		_, _ = w.Write([]byte("PAR1"))
	}))
	defer server.Close()

	driver := newPhase1S3Driver(t, server, time.Second, 2)
	result, err, scope := runPhase1Range(t, driver, context.Background(), phase1RangeRequest(t))
	if err != nil {
		t.Fatal(err)
	}
	if attempts.Load() != 3 {
		t.Fatalf("attempts=%d", attempts.Load())
	}
	result.Buffer.Release()
	if scope.Used() != 0 {
		t.Fatalf("managed memory leaked: %d", scope.Used())
	}
}

func TestPhase1S3FaultTimeoutReleasesManagedBuffer(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		time.Sleep(100 * time.Millisecond)
	}))
	defer server.Close()

	driver := newPhase1S3Driver(t, server, 10*time.Millisecond, 1)
	_, err, scope := runPhase1Range(t, driver, context.Background(), phase1RangeRequest(t))
	if err == nil {
		t.Fatal("expected timeout error")
	}
	if scope.Used() != 0 {
		t.Fatalf("managed memory leaked after timeout: %d", scope.Used())
	}
}

func TestPhase1S3FaultCancelReleasesManagedBuffer(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		<-r.Context().Done()
	}))
	defer server.Close()

	driver := newPhase1S3Driver(t, server, time.Second, 1)
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	_, err, scope := runPhase1Range(t, driver, ctx, phase1RangeRequest(t))
	if !errors.Is(err, context.Canceled) {
		t.Fatalf("error=%v, want context canceled", err)
	}
	if scope.Used() != 0 {
		t.Fatalf("managed memory leaked after cancel: %d", scope.Used())
	}
}

func TestPhase1S3FaultShortReadReleasesManagedBuffer(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Range", "bytes 0-3/4")
		w.Header().Set("Content-Length", "2")
		w.WriteHeader(http.StatusPartialContent)
		_, _ = w.Write([]byte("PA"))
	}))
	defer server.Close()

	driver := newPhase1S3Driver(t, server, time.Second, 1)
	_, err, scope := runPhase1Range(t, driver, context.Background(), phase1RangeRequest(t))
	if !errors.Is(err, vfs.ErrShortRead) {
		t.Fatalf("error=%v, want short read", err)
	}
	if scope.Used() != 0 {
		t.Fatalf("managed memory leaked after short read: %d", scope.Used())
	}
}

func TestPhase1S3FaultObjectChangedReleasesManagedBuffer(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusPreconditionFailed)
	}))
	defer server.Close()

	driver := newPhase1S3Driver(t, server, time.Second, 1)
	req := phase1RangeRequest(t)
	req.Expected = &vfs.ObjectIdentity{ETag: "expected", Size: 4}
	_, err, scope := runPhase1Range(t, driver, context.Background(), req)
	if !errors.Is(err, vfs.ErrObjectChanged) {
		t.Fatalf("error=%v, want object changed", err)
	}
	if scope.Used() != 0 {
		t.Fatalf("managed memory leaked after object change: %d", scope.Used())
	}
}

func TestPhase1S3FaultCredentialFailureReleasesManagedBuffer(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusForbidden)
		_, _ = fmt.Fprint(w, "forbidden")
	}))
	defer server.Close()

	driver := newPhase1S3Driver(t, server, time.Second, 1)
	_, err, scope := runPhase1Range(t, driver, context.Background(), phase1RangeRequest(t))
	if !errors.Is(err, vfs.ErrCredential) {
		t.Fatalf("error=%v, want credential failure", err)
	}
	if scope.Used() != 0 {
		t.Fatalf("managed memory leaked after credential failure: %d", scope.Used())
	}
}
