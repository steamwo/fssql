package webdav

import (
	"context"
	"errors"
	"testing"

	"github.com/steamwo/fssql/internal/core/memory"
	"github.com/steamwo/fssql/internal/storage/vfs"
)

func TestProbeRequiresPUTAndMOVEForStreamingWrite(t *testing.T) {
	fixture := newDAVFixture()
	driver, _ := newTestDriver(t, fixture, 2)
	probe, err := driver.Probe(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	if !probe.Supported.Has(vfs.StreamingWrite) || !probe.Supported.Has(vfs.ServerSideList) {
		t.Fatalf("supported=%#x", uint64(probe.Supported))
	}
	fixture.mu.Lock()
	fixture.allow = "OPTIONS, HEAD, GET, PUT, PROPFIND"
	fixture.mu.Unlock()
	probe, err = driver.Probe(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	if probe.Supported.Has(vfs.StreamingWrite) {
		t.Fatalf("streaming write advertised without MOVE: %#x", uint64(probe.Supported))
	}
}

func TestReadRangeUsesManagedBufferAndExact206(t *testing.T) {
	fixture := newDAVFixture()
	driver, _ := newTestDriver(t, fixture, 2)
	uri, _ := vfs.ParseURI("webdav://" + driver.Key.Endpoint + "/data.bin")
	manager := memory.NewManager(1024)
	scope := manager.NewScope()
	result, err := driver.ReadRange(context.Background(), vfs.RangeRequest{URI: uri, Offset: 2, Length: 4}, scope, memory.NewBufferManager(nil))
	if err != nil {
		t.Fatal(err)
	}
	if got := string(result.Buffer.Bytes()); got != "cdef" {
		t.Fatalf("range=%q", got)
	}
	result.Buffer.Release()
	if scope.Used() != 0 {
		t.Fatalf("managed bytes leaked: %d", scope.Used())
	}
}

func TestReadRangeRejectsWholeObjectFallbackWithoutReadingIt(t *testing.T) {
	fixture := newDAVFixture()
	fixture.ignoreRange = true
	driver, _ := newTestDriver(t, fixture, 2)
	uri, _ := vfs.ParseURI("webdav://" + driver.Key.Endpoint + "/data.bin")
	scope := memory.NewManager(1024).NewScope()
	_, err := driver.ReadRange(context.Background(), vfs.RangeRequest{URI: uri, Offset: 1, Length: 3}, scope, memory.NewBufferManager(nil))
	if !errors.Is(err, vfs.ErrRangeUnsupported) {
		t.Fatalf("err=%v, want ErrRangeUnsupported", err)
	}
	if scope.Used() != 0 {
		t.Fatalf("managed bytes leaked: %d", scope.Used())
	}
}

func TestListParsesBoundedPropfind(t *testing.T) {
	fixture := newDAVFixture()
	fixture.files["/alpha.csv"] = []byte("abc")
	driver, _ := newTestDriver(t, fixture, 2)
	uri, _ := vfs.ParseURI("webdav://" + driver.Key.Endpoint + "/")
	page, err := driver.List(context.Background(), uri, vfs.ListOptions{Prefix: "a", MaxKeys: 10})
	if err != nil {
		t.Fatal(err)
	}
	if len(page.Entries) != 1 || page.Entries[0].URI.Path != "/alpha.csv" {
		t.Fatalf("page=%+v", page)
	}
}
