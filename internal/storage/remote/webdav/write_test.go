package webdav

import (
	"context"
	"errors"
	"strings"
	"testing"
	"time"

	"github.com/steamwo/fssql/internal/core/memory"
	"github.com/steamwo/fssql/internal/security"
	"github.com/steamwo/fssql/internal/storage/vfs"
)

func TestOpenWriteStagesMovesAndReleasesSlot(t *testing.T) {
	fixture := newDAVFixture()
	driver, _ := newTestDriver(t, fixture, 1)
	uri, _ := vfs.ParseURI("webdav://" + driver.Key.Endpoint + "/out.bin")
	scope := memory.NewManager(1024).NewScope()
	handle, err := driver.OpenWrite(context.Background(), uri, vfs.WriteOptions{}, scope, memory.NewBufferManager(nil))
	if err != nil {
		t.Fatal(err)
	}
	if _, err := handle.Write(context.Background(), []byte("hello-webdav")); err != nil {
		t.Fatal(err)
	}
	identity, err := handle.Close(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	if identity.Size != int64(len("hello-webdav")) {
		t.Fatalf("identity=%+v", identity)
	}
	fixture.mu.Lock()
	data := append([]byte(nil), fixture.files["/out.bin"]...)
	for name := range fixture.files {
		if strings.Contains(name, ".fssql-write-") {
			fixture.mu.Unlock()
			t.Fatalf("staging object leaked: %s", name)
		}
	}
	fixture.mu.Unlock()
	if string(data) != "hello-webdav" {
		t.Fatalf("final=%q", data)
	}
	if len(driver.writeSlots) != 0 {
		t.Fatalf("write slot leaked: %d", len(driver.writeSlots))
	}
}

func TestAbortCancelsPUTDeletesStagingAndReleasesSlot(t *testing.T) {
	fixture := newDAVFixture()
	driver, _ := newTestDriver(t, fixture, 1)
	uri, _ := vfs.ParseURI("webdav://" + driver.Key.Endpoint + "/abort.bin")
	scope := memory.NewManager(1024).NewScope()
	handle, err := driver.OpenWrite(context.Background(), uri, vfs.WriteOptions{}, scope, memory.NewBufferManager(nil))
	if err != nil {
		t.Fatal(err)
	}
	if _, err := handle.Write(context.Background(), []byte("partial")); err != nil {
		t.Fatal(err)
	}
	if err := handle.Abort(context.Background()); err != nil {
		t.Fatal(err)
	}
	fixture.mu.Lock()
	_, finalExists := fixture.files["/abort.bin"]
	for name := range fixture.files {
		if strings.Contains(name, ".fssql-write-") {
			fixture.mu.Unlock()
			t.Fatalf("staging object leaked: %s", name)
		}
	}
	fixture.mu.Unlock()
	if finalExists {
		t.Fatal("aborted final object exists")
	}
	if len(driver.writeSlots) != 0 {
		t.Fatalf("write slot leaked: %d", len(driver.writeSlots))
	}
}

func TestOpenWriteRespectsBoundedSlotsAndCancellation(t *testing.T) {
	fixture := newDAVFixture()
	driver, _ := newTestDriver(t, fixture, 1)
	uri1, _ := vfs.ParseURI("webdav://" + driver.Key.Endpoint + "/one.bin")
	uri2, _ := vfs.ParseURI("webdav://" + driver.Key.Endpoint + "/two.bin")
	scope := memory.NewManager(1024).NewScope()
	bm := memory.NewBufferManager(nil)
	first, err := driver.OpenWrite(context.Background(), uri1, vfs.WriteOptions{}, scope, bm)
	if err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	if _, err := driver.OpenWrite(ctx, uri2, vfs.WriteOptions{}, scope, bm); !errors.Is(err, context.Canceled) {
		t.Fatalf("second OpenWrite err=%v, want context.Canceled", err)
	}
	if err := first.Abort(context.Background()); err != nil {
		t.Fatal(err)
	}
	if len(driver.writeSlots) != 0 {
		t.Fatalf("write slot leaked: %d", len(driver.writeSlots))
	}
}

func TestCloseCancellationStopsPUTAndCleansStaging(t *testing.T) {
	fixture := newDAVFixture()
	fixture.stallPUTResponse = true
	driver, _ := newTestDriver(t, fixture, 1)
	uri, _ := vfs.ParseURI("webdav://" + driver.Key.Endpoint + "/slow.bin")
	scope := memory.NewManager(1024).NewScope()
	handle, err := driver.OpenWrite(context.Background(), uri, vfs.WriteOptions{}, scope, memory.NewBufferManager(nil))
	if err != nil {
		t.Fatal(err)
	}
	if _, err := handle.Write(context.Background(), []byte("slow-body")); err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithTimeout(context.Background(), 25*time.Millisecond)
	defer cancel()
	_, err = handle.Close(ctx)
	if !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("Close err=%v, want DeadlineExceeded", err)
	}
	if len(driver.writeSlots) != 0 {
		t.Fatalf("write slot leaked after cancellation: %d", len(driver.writeSlots))
	}
	fixture.mu.Lock()
	defer fixture.mu.Unlock()
	for name := range fixture.files {
		if strings.Contains(name, ".fssql-write-") {
			t.Fatalf("staging object leaked after cancellation: %s", name)
		}
	}
}

func TestRedirectTargetIsRevalidatedBeforeFollow(t *testing.T) {
	fixture := newDAVFixture()
	fixture.redirectRange = true
	driver, _ := newTestDriver(t, fixture, 2)
	uri, _ := vfs.ParseURI("webdav://" + driver.Key.Endpoint + "/data.bin")
	scope := memory.NewManager(1024).NewScope()
	_, err := driver.ReadRange(context.Background(), vfs.RangeRequest{URI: uri, Offset: 0, Length: 1}, scope, memory.NewBufferManager(nil))
	if !errors.Is(err, security.ErrEndpointDenied) {
		t.Fatalf("err=%v, want ErrEndpointDenied", err)
	}
	if scope.Used() != 0 {
		t.Fatalf("managed bytes leaked: %d", scope.Used())
	}
}

func TestListLimitFailsExplicitly(t *testing.T) {
	fixture := newDAVFixture()
	driver, _ := newTestDriver(t, fixture, 2)
	driver.MaxListBytes = 32
	uri, _ := vfs.ParseURI("webdav://" + driver.Key.Endpoint + "/")
	_, err := driver.List(context.Background(), uri, vfs.ListOptions{})
	if !errors.Is(err, ErrListLimit) {
		t.Fatalf("err=%v, want ErrListLimit", err)
	}
}

func TestConditionalWriteIsNotAdvertisedOrAccepted(t *testing.T) {
	fixture := newDAVFixture()
	driver, _ := newTestDriver(t, fixture, 2)
	if driver.StaticCapabilities().Has(vfs.ConditionalWrite) || driver.StaticCapabilities().Has(vfs.AtomicRename) {
		t.Fatalf("static capabilities overclaim: %#x", uint64(driver.StaticCapabilities()))
	}
	uri, _ := vfs.ParseURI("webdav://" + driver.Key.Endpoint + "/out.bin")
	scope := memory.NewManager(1024).NewScope()
	_, err := driver.OpenWrite(context.Background(), uri, vfs.WriteOptions{IfNoneMatch: true}, scope, memory.NewBufferManager(nil))
	if !errors.Is(err, vfs.ErrWriteUnsupported) {
		t.Fatalf("err=%v, want ErrWriteUnsupported", err)
	}
}
